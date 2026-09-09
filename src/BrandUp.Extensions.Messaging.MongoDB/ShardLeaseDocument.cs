using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Who is reading one shard of one stream in one consumer group. The identifier is the checkpoint key
/// (<c>stream|group|shard</c>), so taking a shard is a single conditional upsert and two readers can
/// never end up with a lease each.
/// </summary>
public sealed class ShardLeaseDocument
{
    [BsonId]
    public required string Id { get; set; }

    /// <summary>Stream the shard belongs to; together with the group this is what a reader lists on.</summary>
    [BsonElement("s")]
    public required string StreamName { get; set; }

    [BsonElement("g")]
    public required string ConsumerGroup { get; set; }

    [BsonElement("sh")]
    public required string ShardId { get; set; }

    /// <summary>Reader holding the shard.</summary>
    [BsonElement("o")]
    public required string Owner { get; set; }

    /// <summary>Issued anew on every acquisition; renewing and releasing require it.</summary>
    [BsonElement("tok")]
    public required string Token { get; set; }

    [BsonElement("exp"), BsonDateTimeOptions(Kind = DateTimeKind.Utc, Representation = BsonType.DateTime)]
    public DateTime ExpiresAt { get; set; }

    /// <summary>Reader that asked for the shard; the holder gives it up after the record in flight.</summary>
    [BsonElement("ho")]
    public string? HandoverTo { get; set; }
}
