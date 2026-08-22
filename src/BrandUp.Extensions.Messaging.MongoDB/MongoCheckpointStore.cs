using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Checkpoint store on MongoDB: one upsert per write, keyed by the checkpoint key, so no index beyond
/// <c>_id</c> is needed and concurrent shard readers never contend.
/// </summary>
public class MongoCheckpointStore(IMongoCollection<CheckpointDocument> collection) : ICheckpointStore
{
    public async Task<Checkpoint?> GetAsync(CheckpointKey key, CancellationToken cancellationToken = default)
    {
        var document = await collection
            .Find(Filter(key))
            .FirstOrDefaultAsync(cancellationToken);

        return document is null ? null : new Checkpoint(document.Position, document.Completed);
    }

    public async Task SetAsync(CheckpointKey key, Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        var update = Builders<CheckpointDocument>.Update
            .Set(d => d.Position, checkpoint.Position)
            .Set(d => d.Completed, checkpoint.Completed)
            .Set(d => d.UpdatedAt, DateTime.UtcNow)
            .SetOnInsert(d => d.StreamName, key.StreamName)
            .SetOnInsert(d => d.ConsumerGroup, key.ConsumerGroup);

        await collection.UpdateOneAsync(
            Filter(key), update, new UpdateOptions { IsUpsert = true }, cancellationToken);
    }

    static FilterDefinition<CheckpointDocument> Filter(CheckpointKey key)
        => Builders<CheckpointDocument>.Filter.Eq(d => d.Id, key.ToString());
}
