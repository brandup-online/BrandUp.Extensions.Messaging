using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The lease rules against a real server, where two readers really do race for the same shard: the
/// conditional updates the store is built on are what decides who wins.
/// </summary>
[Trait("Category", "Integration")]
public class MongoShardLeaseStoreTests
{
    // The default name, so a rename cannot make the tests and the library drift apart.
    static readonly string CollectionName = new MongoShardLeaseOptions().CollectionName;
    static readonly CheckpointKey Shard = new("stream", "group", "shard-0");
    static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    static (MongoShardLeaseStore Store, IMongoDatabase Database) CreateStore()
    {
        var client = new MongoClient(MongoEnvironment.ConnectionString);
        var database = client.GetDatabase("lease-tests-" + Guid.NewGuid().ToString("N")[..8]);

        return (new MongoShardLeaseStore(database.GetCollection<ShardLeaseDocument>(CollectionName)), database);
    }

    static Task DropAsync(IMongoDatabase database)
        => database.Client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName);

    [MongoFact]
    public async Task Acquire_FreeShard_ReturnsLease()
    {
        var (store, database) = CreateStore();
        try
        {
            var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);

            Assert.NotNull(lease);
            Assert.Equal("reader-a", lease.Owner);
            Assert.Equal(Shard, lease.Key);
            Assert.NotEmpty(lease.Token);
            Assert.False(lease.IsExpired(DateTime.UtcNow));
            Assert.Null(lease.HandoverTo);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Acquire_ShardHeldByAnother_ReturnsNull()
    {
        var (store, database) = CreateStore();
        try
        {
            await store.TryAcquireAsync(Shard, "reader-a", Minute);

            // The upsert collides with the existing document instead of stealing it — a duplicate key
            // is the server's way of saying the shard is taken.
            Assert.Null(await store.TryAcquireAsync(Shard, "reader-b", Minute));

            var stored = Assert.Single(await store.ListAsync(Shard.StreamName, Shard.ConsumerGroup));
            Assert.Equal("reader-a", stored.Owner);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Acquire_ExpiredShard_TakesItOver_AndTheOldLeaseIsDead()
    {
        var (store, database) = CreateStore();
        try
        {
            var expiring = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromMilliseconds(1));
            Assert.NotNull(expiring);

            await Task.Delay(50);

            Assert.Equal("reader-b", (await store.TryAcquireAsync(Shard, "reader-b", Minute))?.Owner);
            Assert.Null(await store.TryRenewAsync(expiring, Minute));
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Acquire_ConcurrentReaders_OnlyOneWins()
    {
        var (store, database) = CreateStore();
        try
        {
            var attempts = Enumerable.Range(0, 8)
                .Select(i => store.TryAcquireAsync(Shard, $"reader-{i}", Minute))
                .ToArray();

            var leases = await Task.WhenAll(attempts);

            Assert.Single(leases, l => l is not null);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Renew_ExtendsTheLease_AndCarriesTheHandoverBack()
    {
        var (store, database) = CreateStore();
        try
        {
            var lease = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromSeconds(1));
            Assert.NotNull(lease);

            Assert.True(await store.TryRequestHandoverAsync(lease, "reader-b"));

            var renewed = await store.TryRenewAsync(lease, TimeSpan.FromMinutes(5));

            Assert.NotNull(renewed);
            Assert.True(renewed.ExpiresAt > lease.ExpiresAt);
            Assert.Equal(lease.Token, renewed.Token);
            Assert.Equal("reader-b", renewed.HandoverTo);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task RequestHandover_OnlyOnce_AndOnlyOnTheCurrentLease()
    {
        var (store, database) = CreateStore();
        try
        {
            var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);
            Assert.NotNull(lease);

            Assert.True(await store.TryRequestHandoverAsync(lease, "reader-b"));
            Assert.False(await store.TryRequestHandoverAsync(lease, "reader-c"));

            await store.ReleaseAsync(lease);
            Assert.False(await store.TryRequestHandoverAsync(lease, "reader-b"));
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Acquire_AfterHandover_ClearsTheRequest()
    {
        var (store, database) = CreateStore();
        try
        {
            var lease = await store.TryAcquireAsync(Shard, "reader-a", Minute);
            Assert.NotNull(lease);
            await store.TryRequestHandoverAsync(lease, "reader-b");
            await store.ReleaseAsync(lease);

            // The reader that asked for the shard now holds it: the request must not still be pending,
            // or it would hand the shard straight on.
            var taken = await store.TryAcquireAsync(Shard, "reader-b", Minute);

            Assert.Equal("reader-b", taken?.Owner);
            Assert.Null(taken?.HandoverTo);
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task Release_FreesTheShard_AndAStaleLeaseLeavesTheHolderAlone()
    {
        var (store, database) = CreateStore();
        try
        {
            var stale = await store.TryAcquireAsync(Shard, "reader-a", TimeSpan.FromMilliseconds(1));
            Assert.NotNull(stale);

            await Task.Delay(50);
            var current = await store.TryAcquireAsync(Shard, "reader-b", Minute);
            Assert.NotNull(current);

            await store.ReleaseAsync(stale);
            Assert.Equal(current.Token, Assert.Single(await store.ListAsync(Shard.StreamName, Shard.ConsumerGroup)).Token);

            await store.ReleaseAsync(current);
            Assert.Empty(await store.ListAsync(Shard.StreamName, Shard.ConsumerGroup));
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public async Task List_ReturnsOneStreamAndGroup()
    {
        var (store, database) = CreateStore();
        try
        {
            await store.TryAcquireAsync(new CheckpointKey("orders", "billing", "shard-0"), "reader-a", Minute);
            await store.TryAcquireAsync(new CheckpointKey("orders", "billing", "shard-1"), "reader-b", Minute);
            await store.TryAcquireAsync(new CheckpointKey("orders", "analytics", "shard-0"), "reader-c", Minute);
            await store.TryAcquireAsync(new CheckpointKey("events", "billing", "shard-0"), "reader-d", Minute);

            var billing = await store.ListAsync("orders", "billing");

            Assert.Equal(2, billing.Count);
            Assert.All(billing, l => Assert.Equal("orders", l.Key.StreamName));
            Assert.All(billing, l => Assert.Equal("billing", l.Key.ConsumerGroup));
        }
        finally
        {
            await DropAsync(database);
        }
    }

    [MongoFact]
    public void AddMongoMessagingShardLeases_ResolvesStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMongoDatabase>(_ => new MongoClient(MongoEnvironment.ConnectionString).GetDatabase("registration-test"));
        services.AddMongoMessagingShardLeases();

        using var provider = services.BuildServiceProvider();

        Assert.IsType<MongoShardLeaseStore>(provider.GetRequiredService<IShardLeaseStore>());
    }

    [Fact]
    public void AddMongoMessagingShardLeases_WithoutDatabase_ThrowsWithHint()
    {
        var services = new ServiceCollection();
        services.AddMongoMessagingShardLeases();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(provider.GetRequiredService<IShardLeaseStore>);
        Assert.Contains(nameof(MongoShardLeaseOptions.DatabaseAccessor), exception.Message);
    }
}
