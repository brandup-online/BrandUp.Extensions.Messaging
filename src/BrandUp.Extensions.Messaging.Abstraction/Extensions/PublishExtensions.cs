namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Publishing a batch where every message carries the same options — or none at all. The typed form
/// (<see cref="PublishMessage{TMessage}"/>) is what a transport works with; this is the shorthand for
/// the common case of a plain list of messages.
/// </summary>
public static class PublishExtensions
{
    /// <inheritdoc cref="IMessageSender{TMessage}.PublishAsync(IReadOnlyCollection{PublishMessage{TMessage}}, CancellationToken)"/>
    public static Task<IReadOnlyList<PublishResult>> PublishAsync<TMessage>(
        this IMessageSender<TMessage> sender,
        IEnumerable<TMessage> messages,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(sender);

        return sender.PublishAsync(Batch(messages, options), cancellationToken);
    }

    /// <inheritdoc cref="IMessageSender{TMessage}.PublishAsync(IReadOnlyCollection{PublishMessage{TMessage}}, CancellationToken)"/>
    public static Task<IReadOnlyList<PublishResult>> PublishAsync<TMessage>(
        this IMessagePublisher publisher,
        IEnumerable<TMessage> messages,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(publisher);

        return publisher.PublishAsync(Batch(messages, options), cancellationToken);
    }

    static PublishMessage<TMessage>[] Batch<TMessage>(IEnumerable<TMessage> messages, PublishOptions? options)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(messages);

        // One options instance shared by every message: transports only read it.
        return [.. messages.Select(message => new PublishMessage<TMessage>(message, options))];
    }
}
