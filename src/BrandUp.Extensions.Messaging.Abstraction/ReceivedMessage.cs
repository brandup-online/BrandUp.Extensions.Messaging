using System.Collections.ObjectModel;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Message obtained from <see cref="IMessageQueue{TMessage}.ReceiveAsync"/>. The receipt handle pins this
/// particular delivery: it is what <c>DeleteAsync</c> and <c>AbandonAsync</c> operate on, and it expires
/// together with the visibility timeout.
/// </summary>
public class ReceivedMessage<TMessage> where TMessage : class
{
    public required string MessageId { get; init; }
    public required TMessage Body { get; init; }
    public required string ReceiptHandle { get; init; }

    /// <summary>How many times the message has been received, this delivery included. 1 on first delivery.</summary>
    public int DeliveryCount { get; init; } = 1;

    /// <summary>
    /// Ordering group the message was published to (<see cref="PublishOptions.GroupId"/>), set by FIFO
    /// queues only. Messages of one group must be processed one at a time, in order; a consumer that
    /// ignores this loses the ordering the FIFO queue exists to provide.
    /// </summary>
    public string? GroupId { get; init; }

    public DateTimeOffset? EnqueuedAt { get; init; }

    public IReadOnlyDictionary<string, string> Attributes { get; init; } = ReadOnlyDictionary<string, string>.Empty;
}
