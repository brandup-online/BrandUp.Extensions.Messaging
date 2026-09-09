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
/// only once every parent is done.
/// </para>
/// <para>
/// A shard is read by one instance. With an <see cref="IShardLeaseStore"/> registered that is enforced
/// across instances: every shard is leased before it is read, the lease is renewed while it is being
/// read, and the readers of a group divide the stream between them — a reader below its share takes
/// free shards and then asks an overloaded reader to hand one over. Without a lease store nothing
/// stops a second instance of the same group from reading the same shards, so run one instance per
/// group, or give each its own <see cref="KinesisConsumerOptions.ConsumerGroup"/>.
/// </para>
/// </summary>
internal sealed class KinesisConsumerService<TMessage>(
    IMessageStream<TMessage> stream,
    IKinesisClientFactory clientFactory,
    ICheckpointStore checkpoints,
    IMessageSerializer serializer,
    IServiceScopeFactory scopeFactory,
    IOptionsMonitor<KinesisConsumerOptions> optionsMonitor,
    ILogger<KinesisConsumerService<TMessage>> logger,
    IShardLeaseStore? leases = null) : BackgroundService
    where TMessage : class
{
    // Only the discovery loop touches workers; completed is also written by the shard workers, which
    // finish concurrently, so it has to be a concurrent collection.
    readonly Dictionary<string, Task> workers = [];
    readonly ConcurrentDictionary<string, bool> completed = new(StringComparer.Ordinal);

    // Identity of this reader in the lease store; set once the options are read.
    string owner = "";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Get runs the options validators, so a misconfigured reader fails host startup here.
        var options = optionsMonitor.Get(KinesisConsumerOptions.NameFor(typeof(TMessage)));

        // Unique per instance, since that is what a lease is held by: two readers sharing an identity
        // would renew each other's leases and read the same shard.
        owner = options.ReaderId ?? $"{Environment.MachineName}/{Environment.ProcessId}/{Guid.NewGuid().ToString("N")[..6]}";

        if (leases is null)
            logger.LogInformation(
                "Stream reader of {MessageType} started in group {ConsumerGroup}; no shard lease store is registered, so this must be the only reader of the group.",
                typeof(TMessage).Name, options.ConsumerGroup);
        else
            logger.LogInformation(
                "Stream reader of {MessageType} started in group {ConsumerGroup} as {Reader}, sharing the stream by shard leases.",
                typeof(TMessage).Name, options.ConsumerGroup, owner);

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
        var plan = await PlanAsync(shards, options, stoppingToken);

        foreach (var shard in shards)
        {
            if (workers.ContainsKey(shard.ShardId) || completed.ContainsKey(shard.ShardId))
                continue;

            // Another reader of the group is on it: nothing here would succeed, and the checkpoint read
            // it would take is one per foreign shard on every pass.
            if (plan.LeasedByOthers.Contains(shard.ShardId))
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

            ShardLease? lease = null;
            if (leases is not null)
            {
                // Shards beyond this reader's share are left to the others, even when they are free.
                if (workers.Count >= plan.MaxShards)
                    continue;

                lease = await leases.TryAcquireAsync(KeyOf(shard.ShardId, options), owner, options.LeaseDuration, stoppingToken);
                if (lease is null)
                    continue;   // another reader of the group holds the shard
            }

            workers[shard.ShardId] = ReadShardAsync(shard.ShardId, checkpoint, lease, options, stoppingToken);
        }

        // Only after the free shards: a handover makes another reader stop, and costs a re-read of the
        // record it was on, so it is worth asking for only when nothing free brings this reader up to
        // its share.
        if (plan.ShardToAskFor is { } handover && leases is not null && workers.Count < plan.MaxShards)
        {
            if (await leases.TryRequestHandoverAsync(handover, owner, stoppingToken))
                logger.LogInformation(
                    "Asked {Reader} to hand shard {ShardId} of stream {Stream} over.",
                    handover.Owner, handover.Key.ShardId, StreamNameForLog());
        }
    }

    /// <summary>
    /// How many shards this reader may hold, and which lease to ask for when it holds fewer. Without a
    /// lease store there is nothing to share and nobody to ask.
    /// </summary>
    async Task<BalancingPlan> PlanAsync(
        IReadOnlyList<Shard> shards, KinesisConsumerOptions options, CancellationToken cancellationToken)
    {
        if (leases is null)
            return new BalancingPlan(int.MaxValue, null, []);

        var stored = await leases.ListAsync(stream.Name, options.ConsumerGroup, cancellationToken);
        var inPlay = shards.Count(s => !completed.ContainsKey(s.ShardId));

        return ShardBalancer.Plan(stored, owner, inPlay, workers.Count, DateTime.UtcNow);
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

    async Task ReadShardAsync(
        string shardId, Checkpoint? checkpoint, ShardLease? lease, KinesisConsumerOptions options, CancellationToken stoppingToken)
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

        // The lease as this worker last saw it, or null when it holds none - either because no lease
        // store is registered, or because the shard has moved on to another reader.
        var held = lease;
        var renewAt = DateTime.UtcNow + options.LeaseRenewInterval;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (!await KeepShardAsync())
                        return;

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

                            // Between records is where a handover is answered: the record just committed
                            // is the last one this reader owes the shard.
                            if (!await KeepShardAsync())
                                return;
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
        finally
        {
            if (held is not null)
                await ReleaseLeaseAsync(held, shardId);
        }

        // Keeps the lease alive and answers a handover. False when the shard is no longer this reader's:
        // it stops there, and whatever it has not committed is re-read by whoever takes the shard.
        async Task<bool> KeepShardAsync()
        {
            // No lease store: the shard is this reader's for as long as it reads it.
            if (held is null || DateTime.UtcNow < renewAt)
                return true;

            ShardLease? renewed;
            try
            {
                renewed = await leases!.TryRenewAsync(held, options.LeaseDuration, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A failed write is not a lost shard: the lease stands until it actually runs out, and
                // the next iteration tries again. Past that point another reader may take over, so
                // reading on would double-handle records.
                if (DateTime.UtcNow < held.ExpiresAt)
                {
                    logger.LogWarning(ex,
                        "Renewing the lease on shard {ShardId} of stream {Stream} failed; retrying.", shardId, StreamNameForLog());
                    return true;
                }

                logger.LogError(ex,
                    "The lease on shard {ShardId} of stream {Stream} expired and could not be renewed; the shard is given up.",
                    shardId, StreamNameForLog());
                return false;
            }

            if (renewed is null)
            {
                // Taken over after the lease expired - releasing it now would delete another reader's lease.
                logger.LogWarning(
                    "The lease on shard {ShardId} of stream {Stream} is no longer held by {Reader}; the shard is given up.",
                    shardId, StreamNameForLog(), owner);
                held = null;
                return false;
            }

            held = renewed;
            renewAt = DateTime.UtcNow + options.LeaseRenewInterval;

            if (renewed.HandoverTo is { } requester && !string.Equals(requester, owner, StringComparison.Ordinal))
            {
                logger.LogInformation(
                    "Handing shard {ShardId} of stream {Stream} over to {Reader}.", shardId, StreamNameForLog(), requester);
                return false;   // the lease is released in the finally, so the shard is free at once
            }

            return true;
        }
    }

    async Task ReleaseLeaseAsync(ShardLease lease, string shardId)
    {
        try
        {
            // Shutdown has already cancelled this worker's token, and the release still has to reach the
            // store - otherwise the shard sits out the whole lease before another instance may take it.
            await leases!.ReleaseAsync(lease, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Releasing the lease on shard {ShardId} of stream {Stream} failed; it expires on its own.",
                shardId, StreamNameForLog());
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
