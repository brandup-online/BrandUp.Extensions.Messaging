namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Who may read a shard right now. Without a lease store one instance per consumer group reads the
/// whole stream; with one, several instances of the same group share it: a shard is held by exactly
/// one reader, a lease expires unless it is renewed — so the shards of a dead instance are picked up —
/// and an under-loaded reader asks an over-loaded one to hand a shard over.
/// <para>
/// A lease bounds duplication, it does not remove it: a reader stalled past its lease loses the shard
/// while still working on a record. Delivery stays at-least-once, exactly as with checkpoints alone,
/// and handlers must be idempotent.
/// </para>
/// </summary>
public interface IShardLeaseStore
{
    /// <summary>
    /// Every stored lease of one stream and consumer group, expired ones included — the caller decides
    /// what counts as active, since only it knows the current time and its own identity.
    /// </summary>
    Task<IReadOnlyCollection<ShardLease>> ListAsync(
        string streamName, string consumerGroup, CancellationToken cancellationToken = default);

    /// <summary>
    /// Takes the shard for <paramref name="owner"/> when it is free, expired or already theirs, and
    /// returns the new lease; <see langword="null"/> when another reader holds it. Acquiring issues a
    /// fresh <see cref="ShardLease.Token"/> and clears a pending handover.
    /// </summary>
    Task<ShardLease?> TryAcquireAsync(
        CheckpointKey key, string owner, TimeSpan duration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extends a lease still held under the same token and returns it as stored — including a handover
    /// requested meanwhile. <see langword="null"/> when the lease is gone or was taken over, and the
    /// caller must stop reading the shard.
    /// </summary>
    Task<ShardLease?> TryRenewAsync(ShardLease lease, TimeSpan duration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asks the holder of <paramref name="lease"/> to give the shard up, so the stream spreads over the
    /// readers of a group without anyone's work being cut short. <see langword="false"/> when the lease
    /// has moved on or a handover is already pending.
    /// </summary>
    Task<bool> TryRequestHandoverAsync(ShardLease lease, string requester, CancellationToken cancellationToken = default);

    /// <summary>Gives the shard up, so another reader can take it without waiting for the lease to expire.</summary>
    Task ReleaseAsync(ShardLease lease, CancellationToken cancellationToken = default);
}

/// <param name="Key">Shard the lease is on — one stream, one consumer group, one shard.</param>
/// <param name="Owner">Reader holding it; see <c>KinesisConsumerOptions.ReaderId</c>.</param>
/// <param name="Token">
/// Issued anew on every acquisition. Renewing and releasing require it, so a reader that lost the shard
/// while it was stalled cannot revive its lease over the one that replaced it.
/// </param>
/// <param name="ExpiresAt">UTC instant after which another reader may take the shard.</param>
/// <param name="HandoverTo">
/// Reader that asked for this shard. The holder sees it on the next renewal and gives the shard up
/// after the record in flight — a handover never interrupts work in progress.
/// </param>
public sealed record ShardLease(
    CheckpointKey Key, string Owner, string Token, DateTime ExpiresAt, string? HandoverTo = null)
{
    public bool IsExpired(DateTime utcNow) => ExpiresAt <= utcNow;
}
