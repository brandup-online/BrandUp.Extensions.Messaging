using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal sealed class KinesisMessageStream<TMessage> : IMessageStream<TMessage>
    where TMessage : class
{
    // The Kinesis partition-key limit.
    const int MaxPartitionKeyLength = 256;

    readonly IKinesisClientFactory clientFactory;
    readonly IMessageSerializer serializer;
    readonly Lazy<string> name;

    public KinesisMessageStream(
        IKinesisClientFactory clientFactory,
        IMessageSerializer serializer,
        IOptions<KinesisMessagingOptions> options,
        string logicalName)
    {
        this.clientFactory = clientFactory;
        this.serializer = serializer;
        name = new(() =>
        {
            var resolved = options.Value.Streams.TryGetValue(logicalName, out var physicalName)
                ? physicalName
                : logicalName;

            // Fail with a clear error at first use instead of an opaque PutRecord rejection.
            if (string.IsNullOrWhiteSpace(resolved))
                throw new MessagingException($"Physical stream name mapped for '{logicalName}' is empty.");

            return resolved;
        }, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public string Name => name.Value;

    public async Task<PublishResult> PublishAsync(TMessage message, PublishOptions? publishOptions = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        // Options a stream cannot honour are refused rather than dropped: silently ignoring a delay
        // would make a message type behave differently after moving from a queue to a stream.
        if (publishOptions?.Delay is not null)
            throw new ArgumentException(
                "Streams do not support delayed delivery; publish later or use a queue.", nameof(publishOptions));

        if (publishOptions?.DeduplicationId is not null)
            throw new ArgumentException(
                "Streams do not deduplicate; remove DeduplicationId or use a FIFO queue.", nameof(publishOptions));

        // Records sharing a partition key land in one shard and keep their order; without a group the
        // records spread across shards evenly. An empty GroupId (e.g. derived from missing data) would
        // be rejected server-side, so it falls back like null does.
        var partitionKey = publishOptions?.GroupId;
        if (string.IsNullOrEmpty(partitionKey))
            partitionKey = Guid.NewGuid().ToString("N");
        else if (partitionKey.Length > MaxPartitionKeyLength)
            throw new ArgumentException(
                $"GroupId is {partitionKey.Length} characters; the Kinesis partition-key limit is {MaxPartitionKeyLength}.",
                nameof(publishOptions));

        var request = new PutRecordRequest
        {
            StreamName = Name,
            Data = new MemoryStream(serializer.SerializeToUtf8Bytes(message), writable: false),
            PartitionKey = partitionKey,
        };

        try
        {
            var response = await clientFactory.Get().PutRecordAsync(request, cancellationToken);
            return new PublishResult { MessageId = response.SequenceNumber, SequenceNumber = response.SequenceNumber };
        }
        catch (AmazonKinesisException ex)
        {
            throw new MessagingException($"Failed to publish to stream '{Name}'.", ex.ErrorCode, ex);
        }
    }
}
