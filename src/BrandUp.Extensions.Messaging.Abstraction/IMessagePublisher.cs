namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Provider-agnostic publish facade. The destination — a queue or a stream — is chosen by the message
/// type: whatever <see cref="IMessageSender{TMessage}"/> is registered for it in DI. This is the type to
/// inject into application services that only send messages and do not care about the transport.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes the message to the queue or stream registered for <typeparamref name="TMessage"/>.
    /// Throws <see cref="MessagingException"/> when no destination is registered for the type.
    /// </summary>
    Task<PublishResult> PublishAsync<TMessage>(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>
    /// Publishes several messages of one type to the destination registered for it, in the transport's
    /// own batches. See <see cref="IMessageSender{TMessage}.PublishAsync(IReadOnlyCollection{PublishMessage{TMessage}}, CancellationToken)"/>
    /// for what a batch does and does not promise.
    /// </summary>
    Task<IReadOnlyList<PublishResult>> PublishAsync<TMessage>(
        IReadOnlyCollection<PublishMessage<TMessage>> messages, CancellationToken cancellationToken = default)
        where TMessage : class;
}

/// <summary>
/// Send side of one destination bound to a message type. Implemented by both queues
/// (<see cref="IMessageQueue{TMessage}"/>) and streams (<see cref="IMessageStream{TMessage}"/>), so
/// <see cref="IMessagePublisher"/> can route by message type without knowing the transport.
/// </summary>
public interface IMessageSender<TMessage> where TMessage : class
{
    Task<PublishResult> PublishAsync(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes several messages in the transport's own batches — 10 messages per call on SQS, 500
    /// records on a stream — instead of one call per message. Results come back in the order the
    /// messages were passed in.
    /// <para>
    /// A batch is not a transaction: the transport accepts or rejects each message on its own. A
    /// rejected message does not stop the rest, and <see cref="BatchPublishException"/> then names the
    /// ones that failed, so exactly those can be retried. On a FIFO queue a gap like that breaks the
    /// order of its group — retry from the first failed message when order matters.
    /// </para>
    /// </summary>
    Task<IReadOnlyList<PublishResult>> PublishAsync(
        IReadOnlyCollection<PublishMessage<TMessage>> messages, CancellationToken cancellationToken = default);
}

/// <summary>
/// One message of a batch together with the options that apply to it — a batch is rarely uniform: on a
/// stream every record usually carries its own partition key, on a FIFO queue its own group.
/// </summary>
public sealed record PublishMessage<TMessage>(TMessage Message, PublishOptions? Options = null)
    where TMessage : class;
