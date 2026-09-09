using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The rules a lease store has to keep, whichever storage is behind it: one holder at a time, a token
/// that only the current holder has, and a handover the holder learns about when it renews.
/// </summary>
public class InMemoryShardLeaseStoreTests
{
    static readonly CheckpointKey Shard = new("stream", "group", "shard-0");
    static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    [Fact]
    public async Task Acquire_FreeShard_ReturnsLease()
    {
        var store = new InMemoryShardLeaseStore();

        var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);

        Assert.NotNull(lease);
        Assert.Equal("reader-a", lease.Owner);
        Assert.Equal(Shard, lease.Key);
        Assert.NotEmpty(lease.Token);
        Assert.False(lease.IsExpired(DateTime.UtcNow));
        Assert.Null(lease.HandoverTo);
    }

    [Fact]
    public async Task Acquire_ShardHeldByAnother_ReturnsNull()
    {
        var store = new InMemoryShardLeaseStore();
        await store.TryAcquireAsync(Shard, "reader-a", Minute);

        Assert.Null(await store.TryAcquireAsync(Shard, "reader-b", Minute));
    }

    [Fact]
    public async Task Acquire_ExpiredShard_TakesItOver_AndTheOldLeaseIsDead()
    {
        var store = new InMemoryShardLeaseStore();
        var expiring = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromMilliseconds(1));
        Assert.NotNull(expiring);

        await Task.Delay(20);

        var taken = await store.TryAcquireAsync(Shard, "reader-b", Minute);

        Assert.Equal("reader-b", taken?.Owner);

        // The token moved on with the shard: the reader that lost it cannot renew its way back in.
        Assert.Null(await store.TryRenewAsync(expiring, Minute));
    }

    [Fact]
    public async Task Acquire_ByTheSameReader_IsAllowed_AndReissuesTheToken()
    {
        var store = new InMemoryShardLeaseStore();
        var first = await store.TryAcquireAsync(Shard, "reader-a", Minute);
        var second = await store.TryAcquireAsync(Shard, "reader-a", Minute);

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.NotEqual(first.Token, second.Token);
    }

    [Fact]
    public async Task Renew_ExtendsTheLease()
    {
        var store = new InMemoryShardLeaseStore();
        var lease = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromSeconds(1));
        Assert.NotNull(lease);

        var renewed = await store.TryRenewAsync(lease, TimeSpan.FromMinutes(5));

        Assert.NotNull(renewed);
        Assert.True(renewed.ExpiresAt > lease.ExpiresAt);
        Assert.Equal(lease.Token, renewed.Token);
    }

    [Fact]
    public async Task Renew_CarriesTheRequestedHandoverBack()
    {
        var store = new InMemoryShardLeaseStore();
        var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);
        Assert.NotNull(lease);

        Assert.True(await store.TryRequestHandoverAsync(lease, "reader-b"));

        // Renewing is how the holder finds out that someone is waiting for the shard.
        var renewed = await store.TryRenewAsync(lease, Minute);
        Assert.Equal("reader-b", renewed?.HandoverTo);
    }

    [Fact]
    public async Task RequestHandover_OnlyOnce_AndOnlyOnTheCurrentLease()
    {
        var store = new InMemoryShardLeaseStore();
        var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);
        Assert.NotNull(lease);

        Assert.True(await store.TryRequestHandoverAsync(lease, "reader-b"));
        Assert.False(await store.TryRequestHandoverAsync(lease, "reader-c"));

        await store.ReleaseAsync(lease);
        Assert.False(await store.TryRequestHandoverAsync(lease, "reader-b"));
    }

    [Fact]
    public async Task Release_FreesTheShardAtOnce()
    {
        var store = new InMemoryShardLeaseStore();
        var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);
        Assert.NotNull(lease);

        await store.ReleaseAsync(lease);

        Assert.Empty(store.Leases);
        Assert.NotNull(await store.TryAcquireAsync(Shard, "reader-b", Minute));
    }

    [Fact]
    public async Task Release_WithAStaleLease_LeavesTheCurrentHolderAlone()
    {
        var store = new InMemoryShardLeaseStore();
        var stale = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromMilliseconds(1));
        Assert.NotNull(stale);

        await Task.Delay(20);
        var current = await store.TryAcquireAsync(Shard, "reader-b", Minute);

        // A reader that comes back from a stall must not delete the lease that replaced its own.
        await store.ReleaseAsync(stale);

        var remaining = Assert.Single(store.Leases);
        Assert.Equal(current?.Token, remaining.Token);
    }

    [Fact]
    public async Task List_ReturnsOneStreamAndGroup()
    {
        var store = new InMemoryShardLeaseStore();
        await store.TryAcquireAsync(new CheckpointKey("orders", "billing", "shard-0"), "reader-a", Minute);
        await store.TryAcquireAsync(new CheckpointKey("orders", "billing", "shard-1"), "reader-b", Minute);
        await store.TryAcquireAsync(new CheckpointKey("orders", "analytics", "shard-0"), "reader-c", Minute);
        await store.TryAcquireAsync(new CheckpointKey("events", "billing", "shard-0"), "reader-d", Minute);

        var billing = await store.ListAsync("orders", "billing");

        Assert.Equal(2, billing.Count);
        Assert.All(billing, l => Assert.Equal("billing", l.Key.ConsumerGroup));
    }

    [Fact]
    public void AddInMemoryMessagingShardLeases_RegistersOneInspectableStore()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessagingShardLeases();
        services.AddInMemoryMessagingShardLeases();

        using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<InMemoryShardLeaseStore>();
        Assert.Same(store, provider.GetRequiredService<IShardLeaseStore>());
    }
}
