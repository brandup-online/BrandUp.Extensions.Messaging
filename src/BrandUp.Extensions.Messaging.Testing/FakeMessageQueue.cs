using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// In-memory <see cref="IMessageQueue{TMessage}"/> over <see cref="InMemoryMessageBus"/>: publish adds to
/// the pending list, receive takes from it, abandon puts back. Payloads go through the registered
/// <see cref="IMessageSerializer"/> and argument limits match the real SQS queue, so what passes here
/// does not crash in production. Time is not emulated: <see cref="PublishOptions.Delay"/> is accepted
/// but not awaited, and there is no visibility timeout — a received message is simply out of the queue
/// until deleted or abandoned.
/// </summary>
public class FakeMessageQueue<TMessage>(InMemoryMessageBus bus, string name, IMessageSerializer serializer, QueueSettings settings)
    : IMessageQueue<TMessage>
    where TMessage : class
{
    readonly Dictionary<string, FakePublishedMessage> inFlight = [];
    readonly Lock sync = new();

    public string Name { get; } = name;

    public Task<PublishResult> PublishAsync(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        SqsLimits.ValidatePublish(options, settings.Fifo, nameof(options));

        // A FIFO queue always delivers grouped; a standard queue has no groups at all.
        var groupId = settings.Fifo ? options?.GroupId ?? "default" : null;
        var added = FakePublications.Publish(bus, Name, message, options, groupId, serializer);

        return Task.FromResult(new PublishResult { MessageId = added.MessageId });
    }

    public Task<IReadOnlyList<ReceivedMessage<TMessage>>> ReceiveAsync(int maxMessages = 1, TimeSpan? waitTime = null, CancellationToken cancellationToken = default)
    {
        SqsLimits.ValidateReceive(maxMessages, waitTime);

        var taken = bus.TakePending(typeof(TMessage), maxMessages);
        var result = new List<ReceivedMessage<TMessage>>(taken.Count);

        lock (sync)
        {
            foreach (var message in taken)
            {
                var receiptHandle = Guid.NewGuid().ToString("N");
                inFlight[receiptHandle] = message;
                result.Add(message.ToReceived<TMessage>(receiptHandle));
            }
        }

        return Task.FromResult<IReadOnlyList<ReceivedMessage<TMessage>>>(result);
    }

    public Task DeleteAsync(ReceivedMessage<TMessage> message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        lock (sync)
        {
            if (!inFlight.Remove(message.ReceiptHandle))
                throw new MessagingException($"Receipt handle of message {message.MessageId} is unknown or already completed.");
        }

        return Task.CompletedTask;
    }

    public async Task DeleteAsync(IReadOnlyCollection<ReceivedMessage<TMessage>> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        foreach (var message in messages)
            await DeleteAsync(message, cancellationToken);
    }

    public Task AbandonAsync(ReceivedMessage<TMessage> message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (delay is TimeSpan value)
            SqsLimits.EnsureInRange(value, SqsLimits.MaxVisibilityTimeout, nameof(delay));

        FakePublishedMessage pending;
        lock (sync)
        {
            if (!inFlight.Remove(message.ReceiptHandle, out pending!))
                throw new MessagingException($"Receipt handle of message {message.MessageId} is unknown or already completed.");
        }

        bus.Return(pending with { DeliveryCount = pending.DeliveryCount + 1 });

        return Task.CompletedTask;
    }

    public Task<bool> ExistsAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);

    public Task<int> GetApproximateCountAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(bus.PendingCount(typeof(TMessage)));

    public Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        bus.PurgePending(typeof(TMessage));
        return Task.CompletedTask;
    }
}
