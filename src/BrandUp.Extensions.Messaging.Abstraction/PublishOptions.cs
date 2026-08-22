namespace BrandUp.Extensions.Messaging;

public class PublishOptions
{
    /// <summary>
    /// Delivery delay: the message becomes visible to consumers after this time. Capped at 15 minutes.
    /// Not supported on FIFO queues (use <see cref="QueueSettings.DeliveryDelay"/> there) or on
    /// streams — both throw rather than deliver immediately.
    /// </summary>
    public TimeSpan? Delay { get; set; }

    /// <summary>
    /// Ordering group. On a FIFO queue this is the <c>MessageGroupId</c> (messages of one group are
    /// delivered in order, one at a time); when omitted the whole queue acts as a single group. On a
    /// stream this is the partition key: records sharing a group land in one shard and keep their order.
    /// Standard queues ignore it.
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// FIFO deduplication id: messages with the same id published within the 5-minute deduplication
    /// window are accepted once. Not needed when the queue has content-based deduplication enabled
    /// (<see cref="QueueSettings.ContentBasedDeduplication"/>).
    /// </summary>
    public string? DeduplicationId { get; set; }

    /// <summary>
    /// Custom message attributes, delivered next to the payload (SQS message attributes). Streams carry
    /// no attributes — there the values are ignored. The library's own <c>BrandUp-</c> attributes are
    /// reserved and rejected here.
    /// </summary>
    public Dictionary<string, string> Attributes { get; } = new(StringComparer.Ordinal);
}

public class PublishResult
{
    /// <summary>Provider-assigned id: SQS message id, or the record sequence number for a stream.</summary>
    public required string MessageId { get; init; }

    /// <summary>Sequence number — set by FIFO queues and streams, <see langword="null"/> otherwise.</summary>
    public string? SequenceNumber { get; init; }
}
