namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The routing <see cref="IMessagePublisher"/>: resolves the <see cref="IMessageSender{TMessage}"/>
/// registered for the message type — an SQS queue, a Kinesis stream or a fake — and forwards to it.
/// Registered by every provider package, so mixing transports in one application just works.
/// </summary>
internal sealed class ServiceProviderMessagePublisher(IServiceProvider serviceProvider) : IMessagePublisher
{
    public Task<PublishResult> PublishAsync<TMessage>(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);

        var sender = (IMessageSender<TMessage>?)serviceProvider.GetService(typeof(IMessageSender<TMessage>))
            ?? throw new MessagingException(
                $"No queue or stream is registered for message type {typeof(TMessage).FullName}. " +
                $"Register it with AddQueue<{typeof(TMessage).Name}>() or AddStream<{typeof(TMessage).Name}>().");

        return sender.PublishAsync(message, options, cancellationToken);
    }
}
