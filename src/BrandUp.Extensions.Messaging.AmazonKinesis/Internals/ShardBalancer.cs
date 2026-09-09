namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// How one reader decides its share of a stream, from the leases of its consumer group alone — there is
/// no coordinator, every reader sees the same lease list and reaches the same arithmetic.
/// <para>
/// The share is the shards divided by the readers, rounded up: with 5 shards and 2 readers each takes 3
/// and 2. A reader below its share takes free shards first, and only then asks the most loaded reader
/// for one — a handover costs a re-read of the record in flight, a free shard costs nothing.
/// </para>
/// </summary>
internal static class ShardBalancer
{
    /// <param name="leases">Leases of the group, as stored — expired ones are ignored here.</param>
    /// <param name="self">Identity of the deciding reader; it counts as a reader even while holding nothing.</param>
    /// <param name="shardCount">Shards still to be read — completed ones are not shared out.</param>
    /// <param name="heldByMe">Shards this reader is reading now.</param>
    /// <param name="utcNow">Now, against which a lease counts as expired.</param>
    public static BalancingPlan Plan(
        IReadOnlyCollection<ShardLease> leases, string self, int shardCount, int heldByMe, DateTime utcNow)
    {
        var active = leases.Where(l => !l.IsExpired(utcNow)).ToList();
        var byOthers = active
            .Where(l => !string.Equals(l.Owner, self, StringComparison.Ordinal))
            .Select(l => l.Key.ShardId)
            .ToHashSet(StringComparer.Ordinal);

        // A reader with no lease yet is invisible to the others, and is exactly the reader that has to
        // count itself in: otherwise a newly started instance would compute the old share and never
        // ask for anything.
        var owners = active.Select(l => l.Owner).Append(self).Distinct(StringComparer.Ordinal).Count();
        var maxShards = shardCount <= 0 ? 0 : Math.Max(1, (shardCount + owners - 1) / owners);

        if (heldByMe >= maxShards)
            return new BalancingPlan(maxShards, null, byOthers);

        // One handover at a time: until the shard already asked for arrives, asking for another would
        // strip a working reader of more than this one needs.
        if (active.Any(l => string.Equals(l.HandoverTo, self, StringComparison.Ordinal)))
            return new BalancingPlan(maxShards, null, byOthers);

        var overloaded = active
            .Where(l => !string.Equals(l.Owner, self, StringComparison.Ordinal))
            .GroupBy(l => l.Owner, StringComparer.Ordinal)
            .Where(g => g.Count() > maxShards)
            .OrderByDescending(g => g.Count())
            .ThenBy(g => g.Key, StringComparer.Ordinal)
            .FirstOrDefault();

        var handover = overloaded
            ?.Where(l => l.HandoverTo is null)
            .OrderBy(l => l.Key.ShardId, StringComparer.Ordinal)
            .FirstOrDefault();

        return new BalancingPlan(maxShards, handover, byOthers);
    }
}

/// <param name="MaxShards">Shards this reader may hold; it starts no new one beyond that.</param>
/// <param name="ShardToAskFor">
/// Lease to ask for, when free shards alone cannot bring the reader up to its share;
/// <see langword="null"/> when nothing is to be asked for.
/// </param>
/// <param name="LeasedByOthers">
/// Shards another reader is holding a live lease on. Trying to take one is pointless until its lease
/// runs out, and an expired lease is not in here — so a reader that stopped still gets taken over.
/// </param>
internal readonly record struct BalancingPlan(
    int MaxShards, ShardLease? ShardToAskFor, HashSet<string> LeasedByOthers);
