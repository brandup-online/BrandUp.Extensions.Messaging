using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal sealed class KinesisMessageStream<TMessage> : IMessageStream<TMessage>
    where TMessage : class
{
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

        var partitionKey = StreamLimits.ResolvePartitionKey(publishOptions, nameof(publishOptions));

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
