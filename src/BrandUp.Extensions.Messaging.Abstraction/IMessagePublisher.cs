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
}

/// <summary>
/// Send side of one destination bound to a message type. Implemented by both queues
/// (<see cref="IMessageQueue{TMessage}"/>) and streams (<see cref="IMessageStream{TMessage}"/>), so
/// <see cref="IMessagePublisher"/> can route by message type without knowing the transport.
/// </summary>
public interface IMessageSender<TMessage> where TMessage : class
{
    Task<PublishResult> PublishAsync(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default);
}
