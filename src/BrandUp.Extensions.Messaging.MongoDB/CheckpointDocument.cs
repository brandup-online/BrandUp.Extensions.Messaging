using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The read position of one shard, as seen by one consumer group. The identifier is the checkpoint key
/// (<c>stream|group|shard</c>), so a write is a single upsert with no index beyond <c>_id</c>.
/// </summary>
public sealed class CheckpointDocument
{
    [BsonId]
    public required string Id { get; set; }

    /// <summary>Sequence number of the last handled record; reading resumes strictly after it.</summary>
    [BsonElement("p")]
    public required string Position { get; set; }

    /// <summary>The shard is closed and fully read — its children may start.</summary>
    [BsonElement("done")]
    public bool Completed { get; set; }

    [BsonElement("ts"), BsonDateTimeOptions(Kind = DateTimeKind.Utc, Representation = BsonType.DateTime)]
    public DateTime UpdatedAt { get; set; }

    /// <summary>Stream the shard belongs to; stored for diagnostics, not used for lookup.</summary>
    [BsonElement("s")]
    public string? StreamName { get; set; }

    /// <summary>Consumer group; stored for diagnostics, not used for lookup.</summary>
    [BsonElement("g")]
    public string? ConsumerGroup { get; set; }
}
