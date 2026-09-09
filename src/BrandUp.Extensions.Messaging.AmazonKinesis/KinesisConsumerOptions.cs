using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Options of one hosted stream reader, named by <see cref="NameFor"/> (the message type's full name)
/// in the options monitor. Validated when the reader starts — an out-of-range value fails host startup
/// instead of looping.
/// </summary>
public class KinesisConsumerOptions
{
    /// <summary>
    /// Reader group this consumer belongs to. Every group reads the whole stream independently and
    /// keeps its own checkpoints; two deployments sharing a group share progress. Defaults to
    /// <c>default</c>.
    /// </summary>
    public string ConsumerGroup { get; set; } = "default";

    /// <summary>Where to start when the group has no checkpoint for a shard yet.</summary>
    public StreamStartPosition StartPosition { get; set; } = StreamStartPosition.Oldest;

    /// <summary>Records requested per read, 1–10000 (the Kinesis cap). Defaults to 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Pause after an empty read. A stream read returns immediately even when there is nothing new, and
    /// Kinesis allows 5 reads per second per shard, so this is what keeps the reader from burning the
    /// quota. Defaults to 1 second.
    /// </summary>
    public TimeSpan EmptyReadDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Pause after a failed read before trying again. Defaults to 5 seconds.</summary>
    public TimeSpan PollDelayOnError { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the reader re-lists the stream's shards to pick up splits and merges. Defaults to
    /// 1 minute.
    /// </summary>
    public TimeSpan ShardDiscoveryInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Attempts to hand one record to the handler before giving up on it. While a record is being
    /// retried its shard makes no progress — that is what keeps the stream in order. After the last
    /// attempt the record is logged as critical and skipped, so one poison record cannot stall a shard
    /// forever. Set to 0 to retry indefinitely and never skip. Defaults to 5.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Pause between delivery attempts of the same record. Defaults to 2 seconds.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long a shard lease is valid without renewal — how long the shards of a reader that died or
    /// lost the network stay unread before another instance picks them up. Only used when an
    /// <see cref="IShardLeaseStore"/> is registered. Defaults to 30 seconds.
    /// <para>
    /// A lease is renewed between records, so keep it longer than <see cref="MaxDeliveryAttempts"/> ×
    /// <see cref="RetryDelay"/>: a record being retried holds its shard for that long, and a shorter
    /// lease would have the shard taken over while the record is still in the handler.
    /// </para>
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How often the reader extends the leases it holds. Must be shorter than
    /// <see cref="LeaseDuration"/> — with room for a retry, or a single slow write costs the shard.
    /// Defaults to 10 seconds.
    /// </summary>
    public TimeSpan LeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Identity of this reader — the <see cref="ShardLease.Owner"/> of every shard it leases. Defaults
    /// to machine, process and a random suffix: unique per instance, which is what leases rely on. Set
    /// it only to something equally unique (a pod name, say) that reads better in diagnostics.
    /// </summary>
    public string? ReaderId { get; set; }

    /// <summary>
    /// The options-monitor name of the reader bound to <paramref name="messageType"/> — the contract
    /// between registration, the hosted service and configuration binding.
    /// </summary>
    public static string NameFor(Type messageType) => messageType.FullName ?? messageType.Name;
}

public enum StreamStartPosition
{
    /// <summary>Start from the oldest record the stream still holds (Kinesis <c>TRIM_HORIZON</c>).</summary>
    Oldest,

    /// <summary>Start from records published after the reader starts (Kinesis <c>LATEST</c>).</summary>
    Newest,
}

internal class KinesisConsumerOptionsValidator : IValidateOptions<KinesisConsumerOptions>
{
    // The Kinesis GetRecords cap.
    const int MaxBatchSize = 10_000;

    public ValidateOptionsResult Validate(string? name, KinesisConsumerOptions options)
    {
        var consumer = string.IsNullOrEmpty(name) ? "" : $" of reader '{name}'";

        if (string.IsNullOrWhiteSpace(options.ConsumerGroup))
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.ConsumerGroup)}{consumer} is required.");
        if (options.BatchSize is < 1 or > MaxBatchSize)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.BatchSize)}{consumer} must be between 1 and {MaxBatchSize}.");
        if (options.EmptyReadDelay < TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.EmptyReadDelay)}{consumer} must not be negative.");
        if (options.PollDelayOnError < TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.PollDelayOnError)}{consumer} must not be negative.");
        if (options.ShardDiscoveryInterval <= TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.ShardDiscoveryInterval)}{consumer} must be positive.");
        if (options.MaxDeliveryAttempts < 0)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.MaxDeliveryAttempts)}{consumer} must not be negative (0 retries indefinitely).");
        if (options.RetryDelay < TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.RetryDelay)}{consumer} must not be negative.");
        if (options.LeaseDuration <= TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.LeaseDuration)}{consumer} must be positive.");
        if (options.LeaseRenewInterval <= TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.LeaseRenewInterval)}{consumer} must be positive.");
        if (options.LeaseRenewInterval >= options.LeaseDuration)
            return ValidateOptionsResult.Fail(
                $"{nameof(KinesisConsumerOptions.LeaseRenewInterval)}{consumer} must be shorter than {nameof(KinesisConsumerOptions.LeaseDuration)}, or a lease expires while it is being renewed.");
        if (options.ReaderId is not null && string.IsNullOrWhiteSpace(options.ReaderId))
            return ValidateOptionsResult.Fail($"{nameof(KinesisConsumerOptions.ReaderId)}{consumer} must not be empty.");

        return ValidateOptionsResult.Success;
    }
}
