using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Hosted consumer of one queue: long-polls, dispatches each message to the scoped
/// <see cref="IMessageHandler{TMessage}"/> and batch-deletes the handled ones. A message whose handler
/// throws is left in the queue: it reappears after the visibility timeout and dead-letters once the
/// redrive policy says so.
/// <para>
/// Messages carrying a <see cref="ReceivedMessage{TMessage}.GroupId"/> (FIFO queues) are processed one
/// group at a time in receive order, and the rest of a group is left untouched after a failure — the
/// in-group ordering a FIFO queue exists to provide survives both concurrency and errors. Groups are
/// independent of each other and run concurrently up to <see cref="SqsConsumerOptions.MaxConcurrency"/>.
/// </para>
/// </summary>
internal sealed class SqsConsumerService<TMessage>(
    IMessageQueue<TMessage> queue,
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<SqsConsumerOptions> optionsMonitor,
    ILogger<SqsConsumerService<TMessage>> logger) : BackgroundService
    where TMessage : class
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Get runs the options validators, so a misconfigured consumer fails host startup here
        // instead of spinning in a receive-error loop. Consumer options are always declared in code,
        // so a bad value is a programming error and failing the host is the right answer.
        var options = optionsMonitor.Get(SqsConsumerOptions.NameFor(typeof(TMessage)));

        // queue.Name resolves the connection options, which may be deliberately unvalidated
        // (validateOnStart: false); reading it before the first await would fail host startup and
        // defeat that choice, so the queue name only appears in the logs below.
        logger.LogInformation("Consumer of {MessageType} started.", typeof(TMessage).Name);

        while (!stoppingToken.IsCancellationRequested)
        {
            IReadOnlyList<ReceivedMessage<TMessage>> messages;
            try
            {
                messages = await queue.ReceiveAsync(options.BatchSize, options.WaitTime, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (OptionsValidationException ex)
            {
                // The connection is not configured and validation was deferred to first use. Retrying
                // cannot fix it, so the consumer stops instead of logging the same error forever; the
                // host keeps running, which is what validateOnStart: false asked for.
                logger.LogCritical(ex,
                    "Consumer of {MessageType} stopped: the SQS connection is not configured. {Reason}",
                    typeof(TMessage).Name, string.Join(" ", ex.Failures));
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Receive from queue {Queue} failed; retrying in {Delay}.", QueueNameForLog(), options.PollDelayOnError);
                if (!await DelayAsync(options.PollDelayOnError, stoppingToken))
                    break;
                continue;
            }

            if (messages.Count == 0)
            {
                if (!await DelayAsync(options.EmptyReadDelay, stoppingToken))
                    break;
                continue;
            }

            var handled = await ProcessBatchAsync(messages, options, stoppingToken);
            if (handled.Count == 0)
                continue;

            try
            {
                // One batch call instead of a delete per message. Deletion must not be cancelled by
                // shutdown: the handlers have already run, redelivering them is the worse outcome.
                await queue.DeleteAsync(handled, CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Deleting {Count} handled messages from queue {Queue} failed; they will redeliver.",
                    handled.Count, QueueNameForLog());
            }
        }

        logger.LogInformation("Consumer of {MessageType} stopped.", typeof(TMessage).Name);
    }

    static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
            return !cancellationToken.IsCancellationRequested;

        try
        {
            await Task.Delay(delay, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>Queue name for diagnostics; never throws, so logging cannot mask the error being logged.</summary>
    string QueueNameForLog()
    {
        try
        {
            return queue.Name;
        }
        catch
        {
            return typeof(TMessage).Name;
        }
    }

    async Task<List<ReceivedMessage<TMessage>>> ProcessBatchAsync(
        IReadOnlyList<ReceivedMessage<TMessage>> messages, SqsConsumerOptions options, CancellationToken cancellationToken)
    {
        var handled = new List<ReceivedMessage<TMessage>>(messages.Count);

        try
        {
            await Parallel.ForEachAsync(
                OrderedChains(messages),
                new ParallelOptions { MaxDegreeOfParallelism = options.MaxConcurrency, CancellationToken = cancellationToken },
                async (chain, ct) =>
                {
                    foreach (var message in chain)
                    {
                        if (!await ProcessAsync(message, ct))
                        {
                            // Ordered chain: the rest of this group must not overtake the failed
                            // message. It stays in flight and the whole group redelivers in order.
                            if (chain.Count > 1)
                                logger.LogWarning(
                                    "Message {MessageId} of group {GroupId} failed; {Remaining} later messages of the group are left for redelivery to keep their order.",
                                    message.MessageId, message.GroupId, chain.Count - chain.IndexOf(message) - 1);
                            break;
                        }

                        lock (handled)
                            handled.Add(message);
                    }
                });
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down mid-batch: unprocessed messages stay in flight and reappear after the
            // visibility timeout - the at-least-once contract. Handled ones are still deleted.
        }

        return handled;
    }

    /// <summary>
    /// Work items for one batch: every FIFO group is one chain processed in order, and each message
    /// without a group is a chain of its own — so standard queues keep full batch concurrency.
    /// </summary>
    static List<List<ReceivedMessage<TMessage>>> OrderedChains(IReadOnlyList<ReceivedMessage<TMessage>> messages)
    {
        List<List<ReceivedMessage<TMessage>>> chains = [];
        Dictionary<string, List<ReceivedMessage<TMessage>>>? groups = null;

        foreach (var message in messages)
        {
            if (message.GroupId is not string groupId)
            {
                chains.Add([message]);
                continue;
            }

            groups ??= new(StringComparer.Ordinal);
            if (!groups.TryGetValue(groupId, out var chain))
            {
                groups[groupId] = chain = [];
                chains.Add(chain);
            }

            chain.Add(message);
        }

        return chains;
    }

    async Task<bool> ProcessAsync(ReceivedMessage<TMessage> message, CancellationToken cancellationToken)
    {
        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();

            await handler.HandleAsync(new MessageContext<TMessage>(message, queue.Name), cancellationToken);

            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down mid-processing: the message stays in flight and reappears after the
            // visibility timeout, which is exactly the at-least-once contract.
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Handling of message {MessageId} (delivery {DeliveryCount}) from queue {Queue} failed; the message will be redelivered.",
                message.MessageId, message.DeliveryCount, queue.Name);
            return false;
        }
    }
}
