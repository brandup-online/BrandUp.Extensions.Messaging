// Linked as a shared source file into the stream transport (AmazonKinesis) and the testing fake, so a
// publish that a real stream refuses is refused in tests too - and for the same reason.
namespace BrandUp.Extensions.Messaging.Internals;

internal static class StreamLimits
{
    /// <summary>The Kinesis partition-key limit; Yandex Data Streams follows it.</summary>
    public const int MaxPartitionKeyLength = 256;

    /// <summary>Records one PutRecords call may carry.</summary>
    public const int MaxBatchRecords = 500;

    /// <summary>Payload one PutRecords call may carry — 5 MiB across all its records.</summary>
    public const int MaxBatchBytes = 5 * 1024 * 1024;

    /// <summary>
    /// Checks the options against what a stream can do and returns the partition key to publish with.
    /// Options a stream cannot honour are refused rather than dropped: silently ignoring a delay would
    /// make a message type behave differently after moving from a queue to a stream.
    /// </summary>
    public static string ResolvePartitionKey(PublishOptions? options, string parameterName)
    {
        if (options?.Delay is not null)
            throw new ArgumentException(
                "Streams do not support delayed delivery; publish later or use a queue.", parameterName);

        if (options?.DeduplicationId is not null)
            throw new ArgumentException(
                "Streams do not deduplicate; remove DeduplicationId or use a FIFO queue.", parameterName);

        // Records sharing a partition key land in one shard and keep their order; without a group the
        // records spread across shards evenly. An empty GroupId (e.g. derived from missing data) would
        // be rejected server-side, so it falls back like null does.
        var partitionKey = options?.GroupId;
        if (string.IsNullOrEmpty(partitionKey))
            return Guid.NewGuid().ToString("N");

        if (partitionKey.Length > MaxPartitionKeyLength)
            throw new ArgumentException(
                $"GroupId is {partitionKey.Length} characters; the stream partition-key limit is {MaxPartitionKeyLength}.",
                parameterName);

        return partitionKey;
    }
}
