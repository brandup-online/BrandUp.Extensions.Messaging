namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Where a stream reader keeps its position. Streams — unlike queues — do not track what a consumer
/// has read: the reader stores the position itself and resumes from it after a restart.
/// <para>
/// A checkpoint is written after the records it covers have been handled, so delivery is at-least-once
/// and handlers must be idempotent — the same guarantee the queue side gives.
/// </para>
/// </summary>
public interface ICheckpointStore
{
    /// <summary>The stored checkpoint, or <see langword="null"/> when this reader has never committed one.</summary>
    Task<Checkpoint?> GetAsync(CheckpointKey key, CancellationToken cancellationToken = default);

    Task SetAsync(CheckpointKey key, Checkpoint checkpoint, CancellationToken cancellationToken = default);
}

/// <summary>
/// What a checkpoint identifies: one shard of one stream, as read by one consumer group. Different
/// groups read the same stream independently, each with its own position.
/// </summary>
public readonly record struct CheckpointKey(string StreamName, string ConsumerGroup, string ShardId)
{
    /// <summary>Stable string form, usable as a storage key.</summary>
    public override string ToString() => $"{StreamName}|{ConsumerGroup}|{ShardId}";
}

/// <param name="Position">
/// Position of the last handled record — the provider's sequence number. Reading resumes strictly
/// after it.
/// </param>
/// <param name="Completed">
/// The shard is closed and fully read. Set after a split or merge, so the reader knows not to reopen
/// the shard and may start its children — a parent must be drained before its children, or ordering
/// across the reshard is lost.
/// </param>
public sealed record Checkpoint(string Position, bool Completed = false);
