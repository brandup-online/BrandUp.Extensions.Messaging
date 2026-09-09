using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The arithmetic every reader of a group runs on the same lease list: how many shards are mine, and
/// whom to ask when free shards are not enough.
/// </summary>
public class ShardBalancerTests
{
    static readonly DateTime Now = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    static ShardLease Lease(string shardId, string owner, TimeSpan expiresIn, string? handoverTo = null)
        => new(new CheckpointKey("stream", "group", shardId), owner, "token-" + shardId, Now + expiresIn, handoverTo);

    [Fact]
    public void SingleReader_TakesEveryShard()
    {
        var plan = ShardBalancer.Plan([], "me", shardCount: 4, heldByMe: 0, Now);

        Assert.Equal(4, plan.MaxShards);
        Assert.Null(plan.ShardToAskFor);
    }

    [Fact]
    public void TwoReaders_SplitTheStream_RoundingUp()
    {
        // Five shards, two readers: 3 and 2 — rounding down would leave a shard unread.
        ShardLease[] leases = [Lease("0", "other", TimeSpan.FromSeconds(30))];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 5, heldByMe: 0, Now);

        Assert.Equal(3, plan.MaxShards);
    }

    [Fact]
    public void ExpiredLeases_DoNotCountAsReaders()
    {
        // The holder is gone: its shards are free, and it must not shrink anyone's share.
        ShardLease[] leases =
        [
            Lease("0", "gone", TimeSpan.FromSeconds(-1)),
            Lease("1", "gone", TimeSpan.FromSeconds(-1)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 2, heldByMe: 0, Now);

        Assert.Equal(2, plan.MaxShards);
        Assert.Null(plan.ShardToAskFor);
    }

    [Fact]
    public void ReaderWithoutLeases_CountsItself()
    {
        // A reader holding nothing is invisible in the lease list; counting itself is what lets it
        // arrive at a share below what the others hold, and ask for a shard.
        ShardLease[] leases =
        [
            Lease("0", "other", TimeSpan.FromSeconds(30)),
            Lease("1", "other", TimeSpan.FromSeconds(30)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 2, heldByMe: 0, Now);

        Assert.Equal(1, plan.MaxShards);
        Assert.NotNull(plan.ShardToAskFor);
        Assert.Equal("other", plan.ShardToAskFor.Owner);
    }

    [Fact]
    public void HandoverIsAskedOfTheMostLoadedReader()
    {
        ShardLease[] leases =
        [
            Lease("0", "loaded", TimeSpan.FromSeconds(30)),
            Lease("1", "loaded", TimeSpan.FromSeconds(30)),
            Lease("2", "loaded", TimeSpan.FromSeconds(30)),
            Lease("3", "light", TimeSpan.FromSeconds(30)),
        ];

        // Four shards over three readers: two each at most, and only "loaded" is above that.
        var plan = ShardBalancer.Plan(leases, "me", shardCount: 4, heldByMe: 0, Now);

        Assert.Equal(2, plan.MaxShards);
        Assert.Equal("loaded", plan.ShardToAskFor?.Owner);
        Assert.Equal("0", plan.ShardToAskFor?.Key.ShardId);
    }

    [Fact]
    public void ReaderAtItsShare_AsksForNothing()
    {
        ShardLease[] leases =
        [
            Lease("0", "me", TimeSpan.FromSeconds(30)),
            Lease("1", "other", TimeSpan.FromSeconds(30)),
            Lease("2", "other", TimeSpan.FromSeconds(30)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 3, heldByMe: 2, Now);

        Assert.Equal(2, plan.MaxShards);
        Assert.Null(plan.ShardToAskFor);
    }

    [Fact]
    public void PendingRequest_StopsTheReaderFromAskingForMore()
    {
        // The shard asked for is on its way; asking again would strip a working reader of two.
        ShardLease[] leases =
        [
            Lease("0", "other", TimeSpan.FromSeconds(30), handoverTo: "me"),
            Lease("1", "other", TimeSpan.FromSeconds(30)),
            Lease("2", "other", TimeSpan.FromSeconds(30)),
            Lease("3", "other", TimeSpan.FromSeconds(30)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 4, heldByMe: 0, Now);

        Assert.Equal(2, plan.MaxShards);
        Assert.Null(plan.ShardToAskFor);
    }

    [Fact]
    public void ShardAlreadyPromisedToAnother_IsNotAskedFor()
    {
        ShardLease[] leases =
        [
            Lease("0", "other", TimeSpan.FromSeconds(30), handoverTo: "third"),
            Lease("1", "other", TimeSpan.FromSeconds(30)),
            Lease("2", "other", TimeSpan.FromSeconds(30)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 3, heldByMe: 0, Now);

        Assert.Equal("1", plan.ShardToAskFor?.Key.ShardId);
    }

    [Fact]
    public void ShardsHeldByOthers_AreNamed_ExpiredAndOwnOnesAreNot()
    {
        // What the reader skips without going to the checkpoint store: the shards someone else is on.
        ShardLease[] leases =
        [
            Lease("0", "me", TimeSpan.FromSeconds(30)),
            Lease("1", "other", TimeSpan.FromSeconds(30)),
            Lease("2", "gone", TimeSpan.FromSeconds(-1)),
        ];

        var plan = ShardBalancer.Plan(leases, "me", shardCount: 3, heldByMe: 1, Now);

        Assert.Equal(["1"], plan.LeasedByOthers);
    }

    [Fact]
    public void NothingLeftToRead_LeavesNoShardsToTake()
    {
        var plan = ShardBalancer.Plan([], "me", shardCount: 0, heldByMe: 0, Now);

        Assert.Equal(0, plan.MaxShards);
        Assert.Null(plan.ShardToAskFor);
    }
}
