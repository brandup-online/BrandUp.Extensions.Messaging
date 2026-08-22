using System.Collections.ObjectModel;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// In-memory transport behind the fake queues: keeps everything that was published and the per-queue
/// pending lists. Inject it into a test to assert on published messages or to pump them into handlers
/// with <see cref="DispatchPendingAsync"/>.
/// </summary>
public class InMemoryMessageBus
{
    readonly Lock sync = new();
    readonly List<FakePublishedMessage> published = [];
    readonly Dictionary<Type, List<FakePublishedMessage>> pending = [];
    long nextId;

    /// <summary>Everything published since the start (or the last <see cref="Clear"/>), in publish order.</summary>
    public IReadOnlyList<FakePublishedMessage> Published
    {
        get { lock (sync) return [.. published]; }
    }

    /// <summary>
    /// Published messages of one type, in publish order — deserialized from the captured payload, so
    /// what a test asserts on is what a consumer would receive, not the publisher's instance.
    /// </summary>
    public IReadOnlyList<TMessage> PublishedOf<TMessage>() where TMessage : class
    {
        lock (sync)
            return [.. published.Where(m => m.MessageType == typeof(TMessage)).Select(m => (TMessage)m.Message)];
    }

    /// <summary>
    /// Delivers every pending message to its <see cref="IMessageHandler{TMessage}"/> resolved from
    /// <paramref name="serviceProvider"/> (each in its own scope, like the hosted consumer does) and
    /// returns how many were delivered. A throwing handler stops the dispatch: the failed message stays
    /// pending with its delivery count incremented, and the exception surfaces to the test.
    /// </summary>
    public async Task<int> DispatchPendingAsync(IServiceProvider serviceProvider, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        var count = 0;
        while (TryDequeuePending(out var message))
        {
            try
            {
                await message.DispatchAsync(serviceProvider, cancellationToken);
            }
            catch
            {
                Return(message with { DeliveryCount = message.DeliveryCount + 1 });
                throw;
            }
            count++;
        }

        return count;
    }

    public void Clear()
    {
        lock (sync)
        {
            published.Clear();
            pending.Clear();
        }
    }

    internal FakePublishedMessage Add(FakePublishedMessage message)
    {
        lock (sync)
        {
            message = message with { MessageId = Interlocked.Increment(ref nextId).ToString() };
            published.Add(message);
            PendingOf(message.MessageType).Add(message);
            return message;
        }
    }

    internal List<FakePublishedMessage> TakePending(Type messageType, int maxMessages)
    {
        lock (sync)
        {
            var queue = PendingOf(messageType);
            var taken = queue.Take(maxMessages).ToList();
            queue.RemoveRange(0, taken.Count);
            return taken;
        }
    }

    internal void Return(FakePublishedMessage message)
    {
        lock (sync)
            PendingOf(message.MessageType).Insert(0, message);
    }

    internal int PendingCount(Type messageType)
    {
        lock (sync)
            return PendingOf(messageType).Count;
    }

    internal void PurgePending(Type messageType)
    {
        lock (sync)
            PendingOf(messageType).Clear();
    }

    bool TryDequeuePending(out FakePublishedMessage message)
    {
        lock (sync)
        {
            foreach (var queue in pending.Values)
            {
                if (queue.Count > 0)
                {
                    message = queue[0];
                    queue.RemoveAt(0);
                    return true;
                }
            }
        }

        message = null!;
        return false;
    }

    List<FakePublishedMessage> PendingOf(Type messageType)
    {
        if (!pending.TryGetValue(messageType, out var queue))
            pending[messageType] = queue = [];
        return queue;
    }
}

/// <summary>One captured publish: the serialized payload plus everything the transport would carry.</summary>
public sealed record FakePublishedMessage
{
    object? deserialized;

    public required string MessageId { get; init; }
    public required Type MessageType { get; init; }

    /// <summary>The serialized payload, exactly as a real transport would carry it.</summary>
    public required string Payload { get; init; }

    /// <summary>
    /// The payload deserialized back into <see cref="MessageType"/> — a different instance than the
    /// one published, just as a consumer would get.
    /// </summary>
    public object Message => deserialized ??= Deserialize(Payload);

    public required string QueueName { get; init; }
    public PublishOptions? Options { get; init; }

    /// <summary>Ordering group the message was published to; set for FIFO queues only.</summary>
    public string? GroupId { get; init; }

    /// <summary>Snapshot of <see cref="PublishOptions.Attributes"/> at publish time — what the transport would deliver.</summary>
    public IReadOnlyDictionary<string, string> Attributes { get; init; } = ReadOnlyDictionary<string, string>.Empty;

    public DateTimeOffset PublishedAt { get; init; }
    public int DeliveryCount { get; init; } = 1;

    /// <summary>Typed deserialization captured at publish time, so the bus stays free of reflection.</summary>
    internal Func<string, object> Deserialize { get; init; } = static _ =>
        throw new MessagingException("The captured message was not published through a fake queue and cannot be read.");

    /// <summary>Typed dispatch captured at publish time, so the bus can deliver without reflection.</summary>
    internal Func<IServiceProvider, FakePublishedMessage, CancellationToken, Task>? DispatchCore { get; init; }

    internal Task DispatchAsync(IServiceProvider serviceProvider, CancellationToken cancellationToken)
        => DispatchCore is not null
            ? DispatchCore(serviceProvider, this, cancellationToken)
            : throw new MessagingException($"Message {MessageId} was not published through a fake queue and cannot be dispatched.");

    /// <summary>
    /// The one mapping to the delivery shape, shared by receive and dispatch — same fields a real
    /// transport delivers, so handlers see identical context in tests and production.
    /// </summary>
    internal ReceivedMessage<TMessage> ToReceived<TMessage>(string? receiptHandle = null)
        where TMessage : class
        => new()
        {
            MessageId = MessageId,
            // Deserialized per delivery, like a real transport: a handler cannot see another
            // consumer's mutations, and a test cannot mutate what was already published.
            Body = (TMessage)Deserialize(Payload),
            ReceiptHandle = receiptHandle ?? "",
            DeliveryCount = DeliveryCount,
            GroupId = GroupId,
            EnqueuedAt = PublishedAt,
            Attributes = Attributes,
        };
}
