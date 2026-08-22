namespace BrandUp.Extensions.Messaging;

public interface IMessageQueue
{
    /// <summary>Physical queue name, with the environment prefix/suffix and the <c>.fifo</c> suffix applied.</summary>
    string Name { get; }

    Task<bool> ExistsAsync(CancellationToken cancellationToken = default);

    /// <summary>Approximate number of messages available for receive. Providers report it with a lag.</summary>
    Task<int> GetApproximateCountAsync(CancellationToken cancellationToken = default);

    /// <summary>Deletes every message in the queue. Irreversible; SQS allows one purge per minute.</summary>
    Task PurgeAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Typed queue with point-to-point semantics: a received message is invisible to other consumers until it
/// is deleted, abandoned or its visibility timeout expires. For most applications the hosted consumer
/// (<c>AddConsumer</c>) is enough and this interface is only needed for manual receive loops.
/// </summary>
public interface IMessageQueue<TMessage> : IMessageQueue, IMessageSender<TMessage>
    where TMessage : class
{
    /// <summary>
    /// Receives up to <paramref name="maxMessages"/> messages (SQS caps a single receive at 10).
    /// By default the call long-polls for up to 20 seconds: it returns as soon as a message arrives
    /// and after the wait when the queue stays empty — a plain receive loop is then cheap. Pass
    /// <see cref="TimeSpan.Zero"/> as <paramref name="waitTime"/> to return immediately.
    /// Poison messages — payload not deserializable as <typeparamref name="TMessage"/>, or a type
    /// attribute naming another type — are not returned; they are handled per
    /// <see cref="QueueSettings.PoisonMessageHandling"/>.
    /// </summary>
    Task<IReadOnlyList<ReceivedMessage<TMessage>>> ReceiveAsync(int maxMessages = 1, TimeSpan? waitTime = null, CancellationToken cancellationToken = default);

    /// <summary>Deletes the received message — the processing succeeded.</summary>
    Task DeleteAsync(ReceivedMessage<TMessage> message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes several received messages in provider batches (one API call per 10 for SQS). Throws
    /// <see cref="MessagingException"/> when some deletions fail; the failed messages stay in the
    /// queue and redeliver — the usual at-least-once outcome.
    /// </summary>
    Task DeleteAsync(IReadOnlyCollection<ReceivedMessage<TMessage>> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the message to the queue before its visibility timeout expires — the processing failed and
    /// the message should be retried. With <paramref name="delay"/> the message becomes visible after the
    /// delay instead of immediately.
    /// </summary>
    Task AbandonAsync(ReceivedMessage<TMessage> message, TimeSpan? delay = null, CancellationToken cancellationToken = default);
}
