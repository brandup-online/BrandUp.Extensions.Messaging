using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Application handler of one message type, invoked by a hosted consumer (<c>AddConsumer</c>) in its own
/// DI scope — the same contract for queues and streams.
/// <para>
/// On a queue: completing deletes the message; throwing leaves it for redelivery after the visibility
/// timeout — configure a dead-letter queue (<see cref="QueueSettings.MaxReceiveCount"/>) to keep poison
/// messages from looping forever.
/// </para>
/// <para>
/// On a stream: completing lets the reader advance its checkpoint past the record; throwing makes the
/// reader retry the record, holding the shard so ordering survives.
/// </para>
/// </summary>
public interface IMessageHandler<TMessage> where TMessage : class
{
    Task HandleAsync(MessageContext<TMessage> context, CancellationToken cancellationToken);
}

/// <summary>Received message as seen by an <see cref="IMessageHandler{TMessage}"/>.</summary>
public class MessageContext<TMessage> where TMessage : class
{
    public MessageContext()
    {
    }

    /// <summary>
    /// The one mapping from a received message to the handler context, shared by every transport —
    /// real consumers and test fakes must not copy fields by hand and drift apart.
    /// </summary>
    [SetsRequiredMembers]
    public MessageContext(ReceivedMessage<TMessage> message, string queueName)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrEmpty(queueName);

        Message = message.Body;
        MessageId = message.MessageId;
        QueueName = queueName;
        DeliveryCount = message.DeliveryCount;
        GroupId = message.GroupId;
        EnqueuedAt = message.EnqueuedAt;
        Attributes = message.Attributes;
    }

    public required TMessage Message { get; init; }
    public required string MessageId { get; init; }

    /// <summary>Physical name of the queue — or stream — the message came from.</summary>
    public required string QueueName { get; init; }

    /// <summary>How many times the message has been received, this delivery included. 1 on first delivery.</summary>
    public int DeliveryCount { get; init; } = 1;

    /// <summary>
    /// Ordering group: the FIFO message group on a queue, the partition key on a stream;
    /// <see langword="null"/> on a standard queue.
    /// </summary>
    public string? GroupId { get; init; }

    public DateTimeOffset? EnqueuedAt { get; init; }

    public IReadOnlyDictionary<string, string> Attributes { get; init; } = ReadOnlyDictionary<string, string>.Empty;
}
