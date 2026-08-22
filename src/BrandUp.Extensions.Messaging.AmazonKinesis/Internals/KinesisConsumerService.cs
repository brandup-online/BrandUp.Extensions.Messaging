using System.Collections.Concurrent;
using System.Text;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Hosted reader of one stream: discovers shards, reads each of them in order and stores its position
/// in the <see cref="ICheckpointStore"/> after the records it covers have been handled — so a restart
/// resumes where the handler got to, at-least-once.
/// <para>
/// One shard is read by one loop, records go to the handler one at a time, and a failing record is
/// retried in place: that is what keeps a shard's order intact. Shards are read concurrently, since
/// order is only defined within a shard (i.e. within a partition key).
/// </para>
/// <para>
/// Splits and merges are followed: a closed shard is checkpointed as completed and its children start
/// only once every parent is done. One reader per shard is assumed — running several instances in one
/// consumer group would have them read the same shards and re-deliver records; use different groups,
/// or a single instance, until shard leases are implemented.
/// </para>
/// </summary>
internal sealed class KinesisConsumerService<TMessage>(
    IMessageStream<TMessage> stream,
    IKinesisClientFactory clientFactory,
    ICheckpointStore checkpoints,
    IMessageSerializer serializer,
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<KinesisConsumerOptions> optionsMonitor,
    ILogger<KinesisConsumerService<TMessage>> logger) : BackgroundService
    where TMessage : class
{
    // Only the discovery loop touches workers; completed is also written by the shard workers, which
    // finish concurrently, so it has to be a concurrent collection.
    readonly Dictionary<string, Task> workers = [];
    readonly ConcurrentDictionary<string, bool> completed = new(StringComparer.Ordinal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Get runs the options validators, so a misconfigured reader fails host startup here.
        var options = optionsMonitor.Get(KinesisConsumerOptions.NameFor(typeof(TMessage)));
        logger.LogInformation(
            "Stream reader of {MessageType} started in group {ConsumerGroup}.", typeof(TMessage).Name, options.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await StartNewShardsAsync(options, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        "Listing shards of stream {Stream} failed; retrying in {Delay}.", StreamNameForLog(), options.PollDelayOnError);
                    if (!await DelayAsync(options.PollDelayOnError, stoppingToken))
                        break;
                    continue;
                }

                if (!await DelayAsync(options.ShardDiscoveryInterval, stoppingToken))
                    break;
            }
        }
        finally
        {
            // Workers observe the same token; wait so the reader does not outlive the host.
            await Task.WhenAll(workers.Values);
            logger.LogInformation("Stream reader of {MessageType} stopped.", typeof(TMessage).Name);
        }
    }

    async Task StartNewShardsAsync(KinesisConsumerOptions options, CancellationToken stoppingToken)
    {
        // Drop finished workers, so a shard closed by a split can be re-evaluated and its children start.
        foreach (var (shardId, worker) in workers.Where(w => w.Value.IsCompleted).ToList())
        {
            workers.Remove(shardId);
            await worker;   // surfaces a worker crash into the discovery loop's error handling
        }

        var shards = await ListShardsAsync(stoppingToken);
        var present = shards.Select(s => s.ShardId).ToHashSet(StringComparer.Ordinal);

        foreach (var shard in shards)
        {
            if (workers.ContainsKey(shard.ShardId) || completed.ContainsKey(shard.ShardId))
                continue;

            var checkpoint = await checkpoints.GetAsync(KeyOf(shard.ShardId, options), stoppingToken);
            if (checkpoint is { Completed: true })
            {
                completed[shard.ShardId] = true;
                continue;
            }

            // A child shard may only start once every parent is drained, or records of the same
            // partition key would be handled out of order across the reshard. A merge has two parents.
            if (!await AreParentsDrainedAsync(shard, present, options, stoppingToken))
                continue;

            workers[shard.ShardId] = ReadShardAsync(shard.ShardId, checkpoint, options, stoppingToken);
        }
    }

    async Task<bool> AreParentsDrainedAsync(
        Shard shard, HashSet<string> present, KinesisConsumerOptions options, CancellationToken cancellationToken)
    {
        foreach (var parentId in new[] { shard.ParentShardId, shard.AdjacentParentShardId })
        {
            if (!string.IsNullOrEmpty(parentId) && !await IsDrainedAsync(parentId, present, options, cancellationToken))
                return false;
        }

        return true;
    }

    async Task<bool> IsDrainedAsync(
        string shardId, HashSet<string> present, KinesisConsumerOptions options, CancellationToken cancellationToken)
    {
        if (completed.ContainsKey(shardId))
            return true;

        // A parent trimmed away by retention is no longer in the shard list: whatever it still held is
        // gone, so waiting for it would stall its children forever - with or without a partial checkpoint.
        if (!present.Contains(shardId) && !workers.ContainsKey(shardId))
        {
            completed[shardId] = true;
            return true;
        }

        var checkpoint = await checkpoints.GetAsync(KeyOf(shardId, options), cancellationToken);
        if (checkpoint is { Completed: true })
        {
            completed[shardId] = true;
            return true;
        }

        return false;
    }

    async Task ReadShardAsync(string shardId, Checkpoint? checkpoint, KinesisConsumerOptions options, CancellationToken stoppingToken)
    {
        var key = KeyOf(shardId, options);
        var position = checkpoint?.Position;

        logger.LogInformation(
            "Reading shard {ShardId} of stream {Stream} from {Position}.",
            shardId, StreamNameForLog(), position is null ? options.StartPosition.ToString() : $"after {position}");

        // When the reader starts at the end of the stream and has no position yet, re-acquiring a
        // LATEST iterator after an error would skip whatever arrived meanwhile. Reading from the moment
        // this shard was opened gives the same starting point without the gap.
        var startedAt = DateTime.UtcNow;
        string? iterator = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                iterator ??= await GetIteratorAsync(shardId, position, startedAt, options, stoppingToken);

                var response = await clientFactory.Get().GetRecordsAsync(
                    new GetRecordsRequest { ShardIterator = iterator, Limit = options.BatchSize }, stoppingToken);

                if (response.Records is { Count: > 0 } records)
                {
                    foreach (var record in records)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            return;

                        // Abandoned by shutdown rather than handled: the checkpoint must not move past
                        // a record nobody processed, or it is lost instead of redelivered.
                        if (!await HandleAsync(record, shardId, options, stoppingToken))
                            return;

                        // Checkpoint per record: a crash then re-delivers one record, not a whole batch.
                        // The store is a single upsert, and a stream read is far more expensive.
                        position = record.SequenceNumber;
                        await checkpoints.SetAsync(key, new Checkpoint(position), stoppingToken);
                    }
                }

                iterator = response.NextShardIterator;

                if (iterator is null)
                {
                    // The shard was closed by a split or merge and is now fully read.
                    await checkpoints.SetAsync(key, new Checkpoint(position ?? "", Completed: true), stoppingToken);
                    completed[shardId] = true;
                    logger.LogInformation("Shard {ShardId} of stream {Stream} is closed and fully read.", shardId, StreamNameForLog());
                    return;
                }

                if (response.Records is not { Count: > 0 })
                {
                    // Nothing new: back off, or the 5-reads-per-second shard quota is spent on emptiness.
                    if (!await DelayAsync(options.EmptyReadDelay, stoppingToken))
                        return;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Reading shard {ShardId} of stream {Stream} failed; retrying in {Delay}.",
                    shardId, StreamNameForLog(), options.PollDelayOnError);

                // An expired iterator is the common case; re-acquire it from the last checkpoint.
                iterator = null;

                if (!await DelayAsync(options.PollDelayOnError, stoppingToken))
                    return;
            }
        }
    }

    /// <summary>
    /// Hands the record to the handler. <see langword="true"/> when it was handled — or deliberately
    /// given up on, so the checkpoint may move past it; <see langword="false"/> when shutdown
    /// interrupted the work, and the record must be read again next time.
    /// </summary>
    async Task<bool> HandleAsync(Record record, string shardId, KinesisConsumerOptions options, CancellationToken cancellationToken)
    {
        TMessage message;
        try
        {
            message = (TMessage)serializer.Deserialize(ReadPayload(record), typeof(TMessage))
                ?? throw new MessagingException($"The serializer returned no {typeof(TMessage).Name} for the payload.");
        }
        catch (Exception ex)
        {
            // A stream carries no per-record metadata to route on, so an unreadable record can only be
            // logged and passed over; the checkpoint then moves beyond it.
            logger.LogCritical(ex,
                "Record {SequenceNumber} of shard {ShardId} cannot be read as {MessageType} and was skipped.",
                record.SequenceNumber, shardId, typeof(TMessage).FullName);
            return true;
        }

        for (var attempt = 1; !cancellationToken.IsCancellationRequested; attempt++)
        {
            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<TMessage>>();

                await handler.HandleAsync(new MessageContext<TMessage>
                {
                    Message = message,
                    MessageId = record.SequenceNumber,
                    QueueName = StreamNameForLog(),
                    DeliveryCount = attempt,
                    GroupId = record.PartitionKey,
                    EnqueuedAt = record.ApproximateArrivalTimestamp,
                }, cancellationToken);

                return true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                var lastAttempt = options.MaxDeliveryAttempts > 0 && attempt >= options.MaxDeliveryAttempts;

                logger.Log(lastAttempt ? LogLevel.Critical : LogLevel.Error, ex,
                    lastAttempt
                        ? "Handling of record {SequenceNumber} (shard {ShardId}) failed {Attempts} times and was skipped; the checkpoint moves past it."
                        : "Handling of record {SequenceNumber} (shard {ShardId}) failed on attempt {Attempts}; retrying, the shard waits.",
                    record.SequenceNumber, shardId, attempt);

                // Out of attempts: the record is given up on deliberately, so the shard moves on.
                if (lastAttempt)
                    return true;

                // Shutdown cut the backoff short — the record was neither handled nor given up on.
                if (!await DelayAsync(options.RetryDelay, cancellationToken))
                    return false;
            }
        }

        return false;
    }

    async Task<string> GetIteratorAsync(
        string shardId, string? position, DateTime startedAt, KinesisConsumerOptions options, CancellationToken cancellationToken)
    {
        var request = new GetShardIteratorRequest
        {
            StreamName = stream.Name,
            ShardId = shardId,
        };

        if (position is not null)
        {
            request.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
            request.StartingSequenceNumber = position;
        }
        else if (options.StartPosition == StreamStartPosition.Newest)
        {
            // Anchored to when this shard was opened rather than to "now", so a retry after an error
            // does not skip the records that arrived during the backoff.
            request.ShardIteratorType = ShardIteratorType.AT_TIMESTAMP;
            request.Timestamp = startedAt;
        }
        else
        {
            request.ShardIteratorType = ShardIteratorType.TRIM_HORIZON;
        }

        try
        {
            return (await clientFactory.Get().GetShardIteratorAsync(request, cancellationToken)).ShardIterator;
        }
        catch (AmazonKinesisException ex)
        {
            throw new MessagingException($"Failed to open shard {shardId} of stream '{stream.Name}'.", ex.ErrorCode, ex);
        }
    }

    async Task<IReadOnlyList<Shard>> ListShardsAsync(CancellationToken cancellationToken)
    {
        var client = clientFactory.Get();
        var shards = new List<Shard>();
        string? nextToken = null;

        try
        {
            do
            {
                // A continuation token replaces the stream name rather than accompanying it.
                var request = nextToken is null
                    ? new ListShardsRequest { StreamName = stream.Name }
                    : new ListShardsRequest { NextToken = nextToken };

                var response = await client.ListShardsAsync(request, cancellationToken);
                if (response.Shards is { Count: > 0 })
                    shards.AddRange(response.Shards);

                nextToken = response.NextToken;
            }
            while (!string.IsNullOrEmpty(nextToken));
        }
        catch (AmazonKinesisException ex)
        {
            throw new MessagingException($"Failed to list shards of stream '{stream.Name}'.", ex.ErrorCode, ex);
        }

        return shards;
    }

    CheckpointKey KeyOf(string shardId, KinesisConsumerOptions options)
        => new(stream.Name, options.ConsumerGroup, shardId);

    static string ReadPayload(Record record)
    {
        if (record.Data is null)
            return "";

        using var reader = new StreamReader(record.Data, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    /// <summary>Stream name for diagnostics; never throws, so logging cannot mask the error being logged.</summary>
    string StreamNameForLog()
    {
        try
        {
            return stream.Name;
        }
        catch
        {
            return typeof(TMessage).Name;
        }
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
}
