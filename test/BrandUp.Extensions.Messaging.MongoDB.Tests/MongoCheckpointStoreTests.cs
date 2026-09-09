using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

[Trait("Category", "Integration")]
public class MongoCheckpointStoreTests
{
    // The default name, so a rename cannot make the tests and the library drift apart.
    static readonly string CollectionName = new MongoCheckpointOptions().CollectionName;

    static (MongoCheckpointStore Store, IMongoDatabase Database) CreateStore()
    {
        var client = new MongoClient(MongoEnvironment.ConnectionString);
        var database = client.GetDatabase("checkpoint-tests-" + Guid.NewGuid().ToString("N")[..8]);

        return (new MongoCheckpointStore(database.GetCollection<CheckpointDocument>(CollectionName)), database);
    }

    [MongoFact]
    public async Task Get_UnknownKey_ReturnsNull()
    {
        var (store, database) = CreateStore();
        try
        {
            Assert.Null(await store.GetAsync(new CheckpointKey("stream", "group", "shard-0")));
        }
        finally
        {
            await database.Client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName);
        }
    }

    [MongoFact]
    public async Task Set_ThenGet_Roundtrips_AndUpserts()
    {
        var (store, database) = CreateStore();
        var key = new CheckpointKey("stream", "group", "shard-0");
        try
        {
            await store.SetAsync(key, new Checkpoint("100"));
            Assert.Equal(new Checkpoint("100"), await store.GetAsync(key));

            // A second write updates in place rather than inserting a duplicate.
            await store.SetAsync(key, new Checkpoint("200"));
            Assert.Equal(new Checkpoint("200"), await store.GetAsync(key));

            var count = await database.GetCollection<CheckpointDocument>(CollectionName)
                .CountDocumentsAsync(FilterDefinition<CheckpointDocument>.Empty);
            Assert.Equal(1, count);
        }
        finally
        {
            await database.Client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName);
        }
    }

    [MongoFact]
    public async Task Completed_IsPersisted()
    {
        var (store, database) = CreateStore();
        var key = new CheckpointKey("stream", "group", "shard-0");
        try
        {
            await store.SetAsync(key, new Checkpoint("100", Completed: true));

            var checkpoint = await store.GetAsync(key);
            Assert.NotNull(checkpoint);
            Assert.True(checkpoint.Completed);
        }
        finally
        {
            await database.Client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName);
        }
    }

    [MongoFact]
    public async Task ConsumerGroupsAndShards_AreIndependent()
    {
        var (store, database) = CreateStore();
        try
        {
            await store.SetAsync(new CheckpointKey("s", "billing", "shard-0"), new Checkpoint("10"));
            await store.SetAsync(new CheckpointKey("s", "analytics", "shard-0"), new Checkpoint("20"));
            await store.SetAsync(new CheckpointKey("s", "billing", "shard-1"), new Checkpoint("30"));

            Assert.Equal("10", (await store.GetAsync(new CheckpointKey("s", "billing", "shard-0")))?.Position);
            Assert.Equal("20", (await store.GetAsync(new CheckpointKey("s", "analytics", "shard-0")))?.Position);
            Assert.Equal("30", (await store.GetAsync(new CheckpointKey("s", "billing", "shard-1")))?.Position);
        }
        finally
        {
            await database.Client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName);
        }
    }

    [MongoFact]
    public void AddMongoMessagingCheckpoints_ResolvesStore()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMongoDatabase>(_ => new MongoClient(MongoEnvironment.ConnectionString).GetDatabase("registration-test"));
        services.AddMongoMessagingCheckpoints();

        using var provider = services.BuildServiceProvider();

        Assert.IsType<MongoCheckpointStore>(provider.GetRequiredService<ICheckpointStore>());
    }

    [Fact]
    public void AddMongoMessagingCheckpoints_WithoutDatabase_ThrowsWithHint()
    {
        var services = new ServiceCollection();
        services.AddMongoMessagingCheckpoints();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(provider.GetRequiredService<ICheckpointStore>);
        Assert.Contains(nameof(MongoCheckpointOptions.DatabaseAccessor), exception.Message);
    }
}

/// <summary>
/// Reads the MongoDB connection from the environment. Without <c>MONGO_CONNECTION_STRING</c> the
/// integration tests are skipped, so the suite stays green locally without a running MongoDB.
/// </summary>
static class MongoEnvironment
{
    public const string SkipReason = "MONGO_CONNECTION_STRING is not configured; skipping MongoDB integration test.";

    public static string? ConnectionString => Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING");

    public static bool IsConfigured => !string.IsNullOrEmpty(ConnectionString);
}

public sealed class MongoFactAttribute : FactAttribute
{
    public MongoFactAttribute(
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1)
        : base(sourceFilePath, sourceLineNumber)
    {
        if (!MongoEnvironment.IsConfigured)
            Skip = MongoEnvironment.SkipReason;
    }
}
