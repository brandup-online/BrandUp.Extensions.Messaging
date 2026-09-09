using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// <see cref="IShardLeaseStore"/> in a dictionary: the same rules as the MongoDB store — a live lease
/// is not handed to a second reader, renewing and releasing need the token the lease was issued with —
/// and inspectable, so a test can assert on who holds which shard.
/// </summary>
public class InMemoryShardLeaseStore : IShardLeaseStore
{
    readonly Lock sync = new();
    readonly Dictionary<CheckpointKey, ShardLease> leases = [];

    /// <summary>Every lease held right now, expired ones included.</summary>
    public IReadOnlyCollection<ShardLease> Leases
    {
        get { lock (sync) return [.. leases.Values]; }
    }

    public Task<IReadOnlyCollection<ShardLease>> ListAsync(
        string streamName, string consumerGroup, CancellationToken cancellationToken = default)
    {
        lock (sync)
        {
            IReadOnlyCollection<ShardLease> matching =
            [
                .. leases.Values.Where(l =>
                    string.Equals(l.Key.StreamName, streamName, StringComparison.Ordinal)
                    && string.Equals(l.Key.ConsumerGroup, consumerGroup, StringComparison.Ordinal))
            ];

            return Task.FromResult(matching);
        }
    }

    public Task<ShardLease?> TryAcquireAsync(
        CheckpointKey key, string owner, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(owner);

        var now = DateTime.UtcNow;

        lock (sync)
        {
            if (leases.TryGetValue(key, out var current)
                && !current.IsExpired(now)
                && !string.Equals(current.Owner, owner, StringComparison.Ordinal))
                return Task.FromResult<ShardLease?>(null);

            // A fresh token invalidates whatever the previous holder still believes about the shard.
            var lease = new ShardLease(key, owner, Guid.NewGuid().ToString("N"), now + duration);
            leases[key] = lease;

            return Task.FromResult<ShardLease?>(lease);
        }
    }

    public Task<ShardLease?> TryRenewAsync(
        ShardLease lease, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        lock (sync)
        {
            if (!IsHeld(lease))
                return Task.FromResult<ShardLease?>(null);

            // The handover asked for meanwhile rides back with the renewal — that is how the holder
            // learns it should give the shard up.
            var renewed = leases[lease.Key] with { ExpiresAt = DateTime.UtcNow + duration };
            leases[lease.Key] = renewed;

            return Task.FromResult<ShardLease?>(renewed);
        }
    }

    public Task<bool> TryRequestHandoverAsync(
        ShardLease lease, string requester, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentException.ThrowIfNullOrEmpty(requester);

        lock (sync)
        {
            if (!IsHeld(lease) || leases[lease.Key].HandoverTo is not null)
                return Task.FromResult(false);

            leases[lease.Key] = leases[lease.Key] with { HandoverTo = requester };

            return Task.FromResult(true);
        }
    }

    public Task ReleaseAsync(ShardLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        lock (sync)
        {
            if (IsHeld(lease))
                leases.Remove(lease.Key);
        }

        return Task.CompletedTask;
    }

    public void Clear()
    {
        lock (sync)
            leases.Clear();
    }

    /// <summary>The stored lease is the one the caller holds — same shard, same owner, same token.</summary>
    bool IsHeld(ShardLease lease)
        => leases.TryGetValue(lease.Key, out var current)
            && string.Equals(current.Owner, lease.Owner, StringComparison.Ordinal)
            && string.Equals(current.Token, lease.Token, StringComparison.Ordinal);
}

public static class InMemoryShardLeaseServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="InMemoryShardLeaseStore"/> as the shard lease store — what a test needs to
    /// exercise several readers of one consumer group in one process. Repeated calls reuse the same
    /// instance, so what a test inspects is what the readers lease from.
    /// </summary>
    public static IServiceCollection AddInMemoryMessagingShardLeases(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (RegistrationGuards.FindInstance<InMemoryShardLeaseStore>(services) is not null)
            return services;

        var store = new InMemoryShardLeaseStore();
        services.AddSingleton(store);
        services.AddSingleton<IShardLeaseStore>(store);

        return services;
    }
}
