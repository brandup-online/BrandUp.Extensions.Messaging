using System.Collections.ObjectModel;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace BrandUp.Extensions.Messaging.Internals;

internal sealed class SqsMessageQueue<TMessage> : IMessageQueue<TMessage>
    where TMessage : class
{
    static readonly List<string> ReceiveSystemAttributes =
    [
        MessageSystemAttributeName.ApproximateReceiveCount,
        MessageSystemAttributeName.SentTimestamp,
        // FIFO ordering group: the consumer needs it to keep a group's messages in order.
        MessageSystemAttributeName.MessageGroupId,
    ];
    static readonly List<string> AllMessageAttributes = ["All"];

    readonly ISqsQueueProvider provider;
    readonly IMessageSerializer serializer;
    readonly ILogger<SqsMessageQueue<TMessage>> logger;
    readonly Lazy<string> name;
    readonly Lazy<string> typeName;

    public SqsMessageQueue(ISqsQueueProvider provider, IMessageSerializer serializer, ILogger<SqsMessageQueue<TMessage>> logger)
    {
        this.provider = provider;
        this.serializer = serializer;
        this.logger = logger;

        // Both are immutable after startup and read per message - resolve once.
        name = new(() => provider.ResolveName(typeof(TMessage)), LazyThreadSafetyMode.ExecutionAndPublication);
        typeName = new(() => serializer.GetTypeName(typeof(TMessage)), LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public string Name => name.Value;

    // The factory owns client lifetime and caches per connection; caching again here would pin the
    // first-resolved client past an options reload.
    IAmazonSQS Client => provider.ClientFor(typeof(TMessage));

    public async Task<PublishResult> PublishAsync(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var settings = provider.GetSettings(typeof(TMessage));
        var prepared = Prepare(message, options, settings, nameof(options));

        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = prepared.Body,
            MessageAttributes = prepared.Attributes,
            MessageGroupId = prepared.GroupId,
            MessageDeduplicationId = prepared.DeduplicationId,
        };

        if (prepared.DelaySeconds is int delaySeconds)
            request.DelaySeconds = delaySeconds;

        var response = await SendAsync(request, cancellationToken);
        return new PublishResult { MessageId = response.MessageId, SequenceNumber = response.SequenceNumber };
    }

    public async Task<IReadOnlyList<PublishResult>> PublishAsync(
        IReadOnlyCollection<PublishMessage<TMessage>> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return [];

        var settings = provider.GetSettings(typeof(TMessage));

        // Everything is prepared - and validated - before the first call, so a bad message in the middle
        // of a batch is an error instead of a half-published batch.
        var prepared = new PreparedMessage[messages.Count];
        var position = 0;
        foreach (var item in messages)
        {
            ArgumentNullException.ThrowIfNull(item, nameof(messages));
            ArgumentNullException.ThrowIfNull(item.Message, nameof(messages));

            prepared[position] = Prepare(item.Message, item.Options, settings, $"{nameof(messages)}[{position}]");
            position++;
        }

        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        var results = new PublishResult?[prepared.Length];
        var sizes = Array.ConvertAll(prepared, p => p.Bytes);
        string? errorCode = null;

        foreach (var (offset, count) in PublishBatches.Split(sizes, SqsLimits.MaxBatch, SqsLimits.MaxBatchBytes))
        {
            var request = new SendMessageBatchRequest
            {
                QueueUrl = queueUrl,
                // The entry id is the position inside this call, which is how a result finds its message.
                Entries = [.. Enumerable.Range(0, count).Select(i => EntryOf(prepared[offset + i], i))],
            };

            var response = await SendBatchAsync(request, settings, cancellationToken);

            if (response.Successful is { Count: > 0 } successful)
            {
                foreach (var entry in successful)
                    results[offset + int.Parse(entry.Id)] =
                        new PublishResult { MessageId = entry.MessageId, SequenceNumber = entry.SequenceNumber };
            }

            // Which messages failed follows from the results that stayed empty; only the reason has to
            // be picked up here, while the response still has it.
            if (response.Failed is { Count: > 0 } failures)
                errorCode ??= failures[0].Code;
        }

        return PublishBatches.Complete(results, $"queue '{Name}'", errorCode);
    }

    /// <summary>
    /// A message as SQS needs it: serialized, with the type attribute, and with the FIFO fields a
    /// group requires. One place for it, so a batch cannot drift from a single publish.
    /// </summary>
    PreparedMessage Prepare(TMessage message, PublishOptions? options, QueueSettings settings, string parameterName)
    {
        SqsLimits.ValidatePublish(options, settings.Fifo, parameterName);

        var body = Serialize(message);
        var attributes = new Dictionary<string, MessageAttributeValue>
        {
            [MessagingConstants.TypeAttributeName] = new() { DataType = "String", StringValue = typeName.Value },
        };

        int? delaySeconds = null;
        if (options is not null)
        {
            if (options.Delay is TimeSpan delay)
                delaySeconds = SqsLimits.WholeSeconds(delay);

            // The reserved type attribute is rejected by ValidatePublish, so nothing here can overwrite it.
            foreach (var (key, value) in options.Attributes)
                attributes[key] = new MessageAttributeValue { DataType = "String", StringValue = value };
        }

        string? groupId = null;
        string? deduplicationId = null;
        if (settings.Fifo)
        {
            // FIFO requires a group id on every message; without an explicit one the queue acts as a single group.
            groupId = options?.GroupId ?? "default";

            // Without content-based deduplication SQS rejects a message that carries no deduplication id,
            // so a parameterless publish gets a unique one - no deduplication, standard-queue expectations.
            deduplicationId = options?.DeduplicationId
                ?? (settings.ContentBasedDeduplication ? null : NewDeduplicationId());
        }

        return new PreparedMessage(body, attributes, delaySeconds, groupId, deduplicationId, SizeOf(body, attributes));
    }

    static SendMessageBatchRequestEntry EntryOf(PreparedMessage prepared, int id)
    {
        var entry = new SendMessageBatchRequestEntry
        {
            Id = id.ToString(),
            MessageBody = prepared.Body,
            MessageAttributes = prepared.Attributes,
            MessageGroupId = prepared.GroupId,
            MessageDeduplicationId = prepared.DeduplicationId,
        };

        if (prepared.DelaySeconds is int delaySeconds)
            entry.DelaySeconds = delaySeconds;

        return entry;
    }

    /// <summary>
    /// What one message costs against the 256 KiB a request may carry. SQS counts an attribute's name,
    /// its type and its value, so all three are counted here — an underestimate would build a batch the
    /// server then rejects whole.
    /// </summary>
    static int SizeOf(string body, Dictionary<string, MessageAttributeValue> attributes)
    {
        var bytes = Encoding.UTF8.GetByteCount(body);
        foreach (var (key, value) in attributes)
        {
            bytes += Encoding.UTF8.GetByteCount(key)
                + Encoding.UTF8.GetByteCount(value.DataType ?? "")
                + Encoding.UTF8.GetByteCount(value.StringValue ?? "");
        }

        return bytes;
    }

    readonly record struct PreparedMessage(
        string Body,
        Dictionary<string, MessageAttributeValue> Attributes,
        int? DelaySeconds,
        string? GroupId,
        string? DeduplicationId,
        int Bytes);

    public async Task<IReadOnlyList<ReceivedMessage<TMessage>>> ReceiveAsync(int maxMessages = 1, TimeSpan? waitTime = null, CancellationToken cancellationToken = default)
    {
        SqsLimits.ValidateReceive(maxMessages, waitTime);

        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        var request = new ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = maxMessages,
            // Long polling by default: a plain receive loop must not burn billed empty receives. Rounded
            // up, so a sub-second wait stays a (short) long poll instead of silently becoming one.
            WaitTimeSeconds = SqsLimits.WholeSeconds(waitTime ?? SqsLimits.MaxWaitTime),
            MessageSystemAttributeNames = ReceiveSystemAttributes,
            MessageAttributeNames = AllMessageAttributes,
        };

        var response = await InvokeAsync(() => Client.ReceiveMessageAsync(request, cancellationToken), "receive from");

        if (response.Messages is not { Count: > 0 } messages)
            return [];

        var result = new List<ReceivedMessage<TMessage>>(messages.Count);
        List<(Message Message, string Reason)>? poison = null;

        foreach (var message in messages)
        {
            if (Parse(message, out var poisonReason) is not { } body)
            {
                (poison ??= []).Add((message, poisonReason!));
                continue;
            }

            result.Add(new ReceivedMessage<TMessage>
            {
                MessageId = message.MessageId,
                Body = body,
                ReceiptHandle = message.ReceiptHandle,
                DeliveryCount = ReceiveCountOf(message),
                GroupId = SystemAttributeOf(message, MessageSystemAttributeName.MessageGroupId),
                EnqueuedAt = EnqueuedAtOf(message),
                Attributes = CustomAttributesOf(message),
            });
        }

        if (poison is not null)
            await HandlePoisonAsync(poison, queueUrl, cancellationToken);

        return result;
    }

    public Task DeleteAsync(ReceivedMessage<TMessage> message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        return InvokeAsync(async () =>
        {
            var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
            return await Client.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken);
        }, "delete from");
    }

    public async Task DeleteAsync(IReadOnlyCollection<ReceivedMessage<TMessage>> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return;

        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        var failed = await DeleteBatchesAsync(
            [.. messages.Select(m => (m.MessageId, m.ReceiptHandle))], queueUrl, cancellationToken);

        // The failed messages keep their receipt handles and redeliver after the visibility timeout.
        if (failed.Count > 0)
            throw new MessagingException(
                $"Failed to delete {failed.Count} of {messages.Count} messages from queue '{Name}': {string.Join(", ", failed)}.");
    }

    public Task AbandonAsync(ReceivedMessage<TMessage> message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var delaySeconds = 0;
        if (delay is TimeSpan value)
        {
            SqsLimits.EnsureInRange(value, SqsLimits.MaxVisibilityTimeout, nameof(delay));
            delaySeconds = SqsLimits.WholeSeconds(value);
        }

        return InvokeAsync(async () =>
        {
            var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
            return await Client.ChangeMessageVisibilityAsync(queueUrl, message.ReceiptHandle, delaySeconds, cancellationToken);
        }, "abandon in");
    }

    public Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => provider.QueueExistsAsync(typeof(TMessage), cancellationToken);

    public async Task<int> GetApproximateCountAsync(CancellationToken cancellationToken = default)
    {
        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        var response = await InvokeAsync(
            () => Client.GetQueueAttributesAsync(queueUrl, [QueueAttributeName.ApproximateNumberOfMessages], cancellationToken),
            "read attributes of");
        return response.ApproximateNumberOfMessages;
    }

    public async Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        var queueUrl = await provider.GetQueueUrlAsync(typeof(TMessage), cancellationToken);
        await InvokeAsync(() => Client.PurgeQueueAsync(queueUrl, cancellationToken), "purge");
    }

    string Serialize(TMessage message)
    {
        try
        {
            return serializer.Serialize(message);
        }
        catch (Exception ex) when (ex is not MessagingException)
        {
            throw new MessagingException(
                $"Failed to serialize a message of type {typeof(TMessage).FullName}.", MessagingErrorKind.Serialization, null, ex);
        }
    }

    /// <summary>The message body as <typeparamref name="TMessage"/>, or <see langword="null"/> with a poison reason.</summary>
    TMessage? Parse(Message message, out string? poisonReason)
    {
        poisonReason = null;

        // A type attribute naming another type means the payload is not ours even if it happens to
        // parse: JSON web defaults would fill missing properties with defaults and the handler would
        // run on garbage. An absent attribute is accepted for interop with foreign producers.
        if (TypeNameOf(message) is string actualTypeName && actualTypeName != typeName.Value)
        {
            poisonReason = $"its type attribute is '{actualTypeName}', expected '{typeName.Value}'";
            return null;
        }

        try
        {
            // A serializer is an extension point: it may throw anything (JsonException, a custom
            // converter's own type) or, against its contract, return null - all of it is poison, and
            // none of it may escape and take the rest of the batch down with it.
            var body = serializer.Deserialize(message.Body, typeof(TMessage)) as TMessage;
            if (body is null)
                poisonReason = $"the serializer returned no {typeof(TMessage).Name} for the payload";

            return body;
        }
        catch (Exception ex)
        {
            poisonReason = ex.Message;
            return null;
        }
    }

    async Task HandlePoisonAsync(List<(Message Message, string Reason)> poison, string queueUrl, CancellationToken cancellationToken)
    {
        var delete = provider.GetSettings(typeof(TMessage)).PoisonMessageHandling == PoisonMessageHandling.Delete;

        foreach (var (message, reason) in poison)
        {
            if (delete)
            {
                logger.LogError(
                    "Poison message {MessageId} of queue {Queue} deleted: {Reason}. Body kept out of the log; enable Debug to capture it.",
                    message.MessageId, Name, reason);
                logger.LogDebug("Poison message {MessageId} body: {Body}", message.MessageId, message.Body);
            }
            else
            {
                // Redeliver: the message stays invisible until its visibility timeout, then comes back; a
                // redrive policy (QueueSettings.MaxReceiveCount) eventually dead-letters it. Without one it
                // loops forever - and on FIFO blocks its message group - hence the log points at the fix.
                logger.LogError(
                    "Poison message {MessageId} of queue {Queue} skipped (delivery {DeliveryCount}): {Reason}. " +
                    "It will redeliver until dead-lettered; configure QueueSettings.MaxReceiveCount or PoisonMessageHandling.Delete.",
                    message.MessageId, Name, ReceiveCountOf(message), reason);
            }
        }

        if (!delete)
            return;

        // A broken producer floods the queue, so poison arrives in bursts - one batch call, not one per message.
        var failed = await DeleteBatchesAsync(
            [.. poison.Select(p => (p.Message.MessageId, p.Message.ReceiptHandle))], queueUrl, cancellationToken);

        if (failed.Count > 0)
            logger.LogError(
                "Failed to delete {Count} poison messages from queue {Queue}: {MessageIds}. They will redeliver.",
                failed.Count, Name, string.Join(", ", failed));
    }

    async Task<List<string>> DeleteBatchesAsync(
        IReadOnlyList<(string MessageId, string ReceiptHandle)> messages, string queueUrl, CancellationToken cancellationToken)
    {
        var failed = new List<string>();

        foreach (var chunk in messages.Chunk(SqsLimits.MaxBatch))
        {
            var request = new DeleteMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = [.. chunk.Select((message, index) => new DeleteMessageBatchRequestEntry
                {
                    Id = index.ToString(),
                    ReceiptHandle = message.ReceiptHandle,
                })],
            };

            var response = await InvokeAsync(() => Client.DeleteMessageBatchAsync(request, cancellationToken), "delete from");

            if (response.Failed is { Count: > 0 } failures)
                failed.AddRange(failures.Select(f => chunk[int.Parse(f.Id)].MessageId));
        }

        return failed;
    }

    async Task<SendMessageResponse> SendAsync(SendMessageRequest request, CancellationToken cancellationToken)
    {
        try
        {
            return await InvokeAsync(() => Client.SendMessageAsync(request, cancellationToken), "publish to");
        }
        catch (MessagingException ex) when (RequiresDeduplicationId(ex) && request.MessageDeduplicationId is null)
        {
            // QueueSettings describe how the queue is created, and an existing queue is never
            // reconciled - so a queue predating this deployment may lack content-based deduplication
            // the registration declares. Publishing our own id keeps the send working; the log says
            // what to fix.
            logger.LogWarning(
                "Queue {Queue} has no content-based deduplication, unlike its registration; publishing with a generated deduplication id. " +
                "Enable it on the queue or set an explicit PublishOptions.DeduplicationId.", Name);

            request.MessageDeduplicationId = NewDeduplicationId();

            return await InvokeAsync(() => Client.SendMessageAsync(request, cancellationToken), "publish to");
        }
    }

    /// <summary>
    /// Sends one batch, with the same fallback a single publish has: a FIFO queue that lacks the
    /// content-based deduplication its registration declares rejects entries without a deduplication
    /// id, and those entries are re-sent once with a generated one.
    /// </summary>
    async Task<SendMessageBatchResponse> SendBatchAsync(
        SendMessageBatchRequest request, QueueSettings settings, CancellationToken cancellationToken)
    {
        var response = await InvokeAsync(() => Client.SendMessageBatchAsync(request, cancellationToken), "publish to");

        if (response.Failed is not { Count: > 0 } failures)
            return response;

        var retry = failures
            .Where(RequiresDeduplicationId)
            .Select(failure => request.Entries.First(entry => entry.Id == failure.Id))
            .Where(entry => entry.MessageDeduplicationId is null)
            .ToList();

        if (retry.Count == 0)
            return response;

        logger.LogWarning(
            "Queue {Queue} has no content-based deduplication, unlike its registration; re-publishing {Count} batch entries " +
            "with a generated deduplication id. Enable it on the queue or set an explicit PublishOptions.DeduplicationId.",
            Name, retry.Count);

        foreach (var entry in retry)
            entry.MessageDeduplicationId = NewDeduplicationId();

        var second = await InvokeAsync(
            () => Client.SendMessageBatchAsync(
                new SendMessageBatchRequest { QueueUrl = request.QueueUrl, Entries = retry }, cancellationToken),
            "publish to");

        // Merge: what the retry published is no longer a failure of this batch.
        var retried = retry.Select(entry => entry.Id).ToHashSet(StringComparer.Ordinal);

        return new SendMessageBatchResponse
        {
            Successful = [.. response.Successful ?? [], .. second.Successful ?? []],
            Failed =
            [
                .. failures.Where(failure => !retried.Contains(failure.Id)),
                .. second.Failed ?? [],
            ],
        };
    }

    static bool RequiresDeduplicationId(BatchResultErrorEntry failure)
        => failure.Code == "InvalidParameterValue"
            && failure.Message?.Contains("MessageDeduplicationId", StringComparison.Ordinal) == true;

    static bool RequiresDeduplicationId(MessagingException ex)
        => ex.InnerException is AmazonSQSException { ErrorCode: "InvalidParameterValue" } inner
            && inner.Message.Contains("MessageDeduplicationId", StringComparison.Ordinal);

    static string NewDeduplicationId() => Guid.NewGuid().ToString("N");

    async Task<T> InvokeAsync<T>(Func<Task<T>> call, string action)
    {
        try
        {
            return await call();
        }
        catch (AmazonSQSException ex)
        {
            throw SqsErrors.Wrap($"Failed to {action} queue '{Name}'.", ex);
        }
    }

    static string? TypeNameOf(Message message)
        => message.MessageAttributes is not null
            && message.MessageAttributes.TryGetValue(MessagingConstants.TypeAttributeName, out var value)
                ? value.StringValue
                : null;

    static string? SystemAttributeOf(Message message, string name)
        => message.Attributes is not null && message.Attributes.TryGetValue(name, out var value) ? value : null;

    static int ReceiveCountOf(Message message)
        => int.TryParse(SystemAttributeOf(message, MessageSystemAttributeName.ApproximateReceiveCount), out var count)
            ? count
            : 1;

    static DateTimeOffset? EnqueuedAtOf(Message message)
        => long.TryParse(SystemAttributeOf(message, MessageSystemAttributeName.SentTimestamp), out var unixMilliseconds)
            ? DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds)
            : null;

    static IReadOnlyDictionary<string, string> CustomAttributesOf(Message message)
    {
        if (message.MessageAttributes is not { Count: > 0 } attributes)
            return ReadOnlyDictionary<string, string>.Empty;

        var result = new Dictionary<string, string>(attributes.Count, StringComparer.Ordinal);
        foreach (var (key, value) in attributes)
        {
            if (key == MessagingConstants.TypeAttributeName || value.StringValue is null)
                continue;
            result[key] = value.StringValue;
        }

        return result;
    }
}
