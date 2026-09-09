using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Shard leases on MongoDB: one small document per shard, keyed by the checkpoint key. Every state
/// change is a single conditional update on <c>_id</c> — taking a shard succeeds only while it is free
/// or expired, renewing and releasing only for the reader that still holds the token — so the outcome
/// of two readers racing for the same shard is decided by the server, not by a read-then-write.
/// </summary>
public class MongoShardLeaseStore(IMongoCollection<ShardLeaseDocument> collection) : IShardLeaseStore
{
    public async Task<IReadOnlyCollection<ShardLease>> ListAsync(
        string streamName, string consumerGroup, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(streamName);
        ArgumentException.ThrowIfNullOrEmpty(consumerGroup);

        // A group holds one document per shard — tens of them at most, so this reads the whole set
        // rather than paging it, and needs no index beyond _id.
        var filter = Builders<ShardLeaseDocument>.Filter.And(
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.StreamName, streamName),
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.ConsumerGroup, consumerGroup));

        var documents = await collection.Find(filter).ToListAsync(cancellationToken);

        return documents.ConvertAll(ToLease);
    }

    public async Task<ShardLease?> TryAcquireAsync(
        CheckpointKey key, string owner, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);

        var now = DateTime.UtcNow;
        var filter = Builders<ShardLeaseDocument>.Filter.And(
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.Id, key.ToString()),
            Builders<ShardLeaseDocument>.Filter.Or(
                Builders<ShardLeaseDocument>.Filter.Lte(d => d.ExpiresAt, now),
                Builders<ShardLeaseDocument>.Filter.Eq(d => d.Owner, owner)));

        var update = Builders<ShardLeaseDocument>.Update
            .Set(d => d.Owner, owner)
            .Set(d => d.Token, Guid.NewGuid().ToString("N"))
            .Set(d => d.ExpiresAt, now + duration)
            .Set(d => d.HandoverTo, null)
            .SetOnInsert(d => d.StreamName, key.StreamName)
            .SetOnInsert(d => d.ConsumerGroup, key.ConsumerGroup)
            .SetOnInsert(d => d.ShardId, key.ShardId);

        try
        {
            var document = await collection.FindOneAndUpdateAsync(
                filter,
                update,
                new FindOneAndUpdateOptions<ShardLeaseDocument> { IsUpsert = true, ReturnDocument = ReturnDocument.After },
                cancellationToken);

            return document is null ? null : ToLease(document);
        }
        catch (MongoException ex) when (IsDuplicateKey(ex))
        {
            // The shard exists and the filter did not match it, so the upsert tried to insert a second
            // document with the same _id: someone else holds a live lease. That is an answer, not an error.
            return null;
        }
    }

    public async Task<ShardLease?> TryRenewAsync(
        ShardLease lease, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);

        var update = Builders<ShardLeaseDocument>.Update.Set(d => d.ExpiresAt, DateTime.UtcNow + duration);

        var document = await collection.FindOneAndUpdateAsync(
            Held(lease),
            update,
            new FindOneAndUpdateOptions<ShardLeaseDocument> { ReturnDocument = ReturnDocument.After },
            cancellationToken);

        // No document: the lease was taken over after it expired, or released. Either way the shard is
        // not this reader's any more — and the returned one carries a handover asked for meanwhile.
        return document is null ? null : ToLease(document);
    }

    public async Task<bool> TryRequestHandoverAsync(
        ShardLease lease, string requester, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentException.ThrowIfNullOrEmpty(requester);

        var filter = Builders<ShardLeaseDocument>.Filter.And(
            Held(lease),
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.HandoverTo, null));

        var result = await collection.UpdateOneAsync(
            filter, Builders<ShardLeaseDocument>.Update.Set(d => d.HandoverTo, requester), cancellationToken: cancellationToken);

        return result.ModifiedCount > 0;
    }

    public async Task ReleaseAsync(ShardLease lease, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lease);

        // Deleting rather than expiring: the shard is free at once, and the token in the filter keeps a
        // late release from removing the lease that replaced this one.
        await collection.DeleteOneAsync(Held(lease), cancellationToken);
    }

    static FilterDefinition<ShardLeaseDocument> Held(ShardLease lease)
        => Builders<ShardLeaseDocument>.Filter.And(
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.Id, lease.Key.ToString()),
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.Owner, lease.Owner),
            Builders<ShardLeaseDocument>.Filter.Eq(d => d.Token, lease.Token));

    static ShardLease ToLease(ShardLeaseDocument document)
        => new(
            new CheckpointKey(document.StreamName, document.ConsumerGroup, document.ShardId),
            document.Owner,
            document.Token,
            DateTime.SpecifyKind(document.ExpiresAt, DateTimeKind.Utc),
            document.HandoverTo);

    static bool IsDuplicateKey(MongoException exception) => exception switch
    {
        MongoCommandException command => command.Code == 11000,
        MongoWriteException write => write.WriteError?.Category == ServerErrorCategory.DuplicateKey,
        _ => false,
    };
}
