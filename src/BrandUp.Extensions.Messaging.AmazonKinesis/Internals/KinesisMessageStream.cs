using System.Text;
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

    public async Task<IReadOnlyList<PublishResult>> PublishAsync(
        IReadOnlyCollection<PublishMessage<TMessage>> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return [];

        // Everything is prepared - and validated - before the first call, so a bad record in the middle
        // of a batch is an error instead of a half-published batch.
        var records = new PutRecordsRequestEntry[messages.Count];
        var sizes = new int[messages.Count];
        var position = 0;

        foreach (var item in messages)
        {
            ArgumentNullException.ThrowIfNull(item, nameof(messages));
            ArgumentNullException.ThrowIfNull(item.Message, nameof(messages));

            var partitionKey = StreamLimits.ResolvePartitionKey(item.Options, $"{nameof(messages)}[{position}]");
            var data = serializer.SerializeToUtf8Bytes(item.Message);

            records[position] = new PutRecordsRequestEntry
            {
                Data = new MemoryStream(data, writable: false),
                PartitionKey = partitionKey,
            };
            // A record costs its payload plus its partition key against the 5 MiB of a request.
            sizes[position] = data.Length + Encoding.UTF8.GetByteCount(partitionKey);
            position++;
        }

        var results = new PublishResult?[records.Length];
        string? errorCode = null;

        foreach (var (offset, count) in PublishBatches.Split(sizes, StreamLimits.MaxBatchRecords, StreamLimits.MaxBatchBytes))
        {
            var request = new PutRecordsRequest
            {
                StreamName = Name,
                Records = [.. records.Skip(offset).Take(count)],
            };

            PutRecordsResponse response;
            try
            {
                response = await clientFactory.Get().PutRecordsAsync(request, cancellationToken);
            }
            catch (AmazonKinesisException ex)
            {
                throw new MessagingException($"Failed to publish to stream '{Name}'.", ex.ErrorCode, ex);
            }

            // Results come back in the order of the records that were sent - that is the only way a
            // record is identified, PutRecords has no per-record id. A record without a sequence number
            // was not written, and its empty slot is what reports it as failed.
            for (var i = 0; i < count; i++)
            {
                var record = response.Records is { Count: > 0 } written && i < written.Count ? written[i] : null;

                if (record?.SequenceNumber is { Length: > 0 } sequenceNumber)
                    results[offset + i] = new PublishResult { MessageId = sequenceNumber, SequenceNumber = sequenceNumber };
                else
                    errorCode ??= record?.ErrorCode;
            }
        }

        return PublishBatches.Complete(results, $"stream '{Name}'", errorCode);
    }
}
