using System.Collections.ObjectModel;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// What publishing means on the fake, for a queue and for a stream alike: the payload goes through the
/// serializer, the captured message carries what a consumer would see, and dispatching it resolves the
/// handler in its own scope — the way the hosted consumer and the shard reader both do it.
/// </summary>
internal static class FakePublications
{
    /// <summary>
    /// A batch on the fake: every message is checked first and only then published, one after another —
    /// which is what a batch amounts to in memory. The limits that make a real transport split a batch
    /// have no meaning here, but the order matters: production rejects a batch holding a bad message
    /// before anything goes out, and a fake that published the first half would hide that.
    /// </summary>
    /// <remarks>
    /// <paramref name="validate"/> checks one message the way a single publish does; it is given the
    /// parameter name to blame, so an error points at the position in the batch.
    /// </remarks>
    public static async Task<IReadOnlyList<PublishResult>> PublishEachAsync<TMessage>(
        IReadOnlyCollection<PublishMessage<TMessage>> messages,
        Action<PublishMessage<TMessage>, string> validate,
        Func<TMessage, PublishOptions?, CancellationToken, Task<PublishResult>> publish,
        CancellationToken cancellationToken)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(messages);

        var position = 0;
        foreach (var item in messages)
        {
            ArgumentNullException.ThrowIfNull(item, nameof(messages));
            ArgumentNullException.ThrowIfNull(item.Message, nameof(messages));

            validate(item, $"{nameof(messages)}[{position}]");
            position++;
        }

        var results = new List<PublishResult>(messages.Count);
        foreach (var item in messages)
            results.Add(await publish(item.Message, item.Options, cancellationToken));

        return results;
    }

    /// <remarks>
    /// <paramref name="groupId"/> is the group the message arrives in — the FIFO message group of a
    /// queue, the partition key of a stream, or <see langword="null"/> on a standard queue, which has
    /// no groups.
    /// </remarks>
    public static FakePublishedMessage Publish<TMessage>(
        InMemoryMessageBus bus,
        string destinationName,
        TMessage message,
        PublishOptions? options,
        string? groupId,
        IMessageSerializer serializer)
        where TMessage : class
    {
        // Round-trip through the serializer exactly like a real transport: a payload that cannot be
        // serialized must fail in tests, and the handler must not receive the publisher's instance.
        var payload = serializer.Serialize(message);

        return bus.Add(new FakePublishedMessage
        {
            MessageId = "", // assigned by the bus
            MessageType = typeof(TMessage),
            Payload = payload,
            QueueName = destinationName,
            Options = options,
            GroupId = groupId,
            // Snapshot: PublishOptions is mutable and may be reused by the caller.
            Attributes = options is { Attributes.Count: > 0 }
                ? new Dictionary<string, string>(options.Attributes, StringComparer.Ordinal)
                : ReadOnlyDictionary<string, string>.Empty,
            PublishedAt = DateTimeOffset.UtcNow,
            Deserialize = payload => (TMessage)serializer.Deserialize(payload, typeof(TMessage)),
            DispatchCore = static async (serviceProvider, published, cancellationToken) =>
            {
                await using var scope = serviceProvider.CreateAsyncScope();
                var handler = scope.ServiceProvider.GetService<IMessageHandler<TMessage>>()
                    ?? throw new MessagingException(
                        $"No IMessageHandler<{typeof(TMessage).Name}> is registered to dispatch the pending message.");

                await handler.HandleAsync(new MessageContext<TMessage>(published.ToReceived<TMessage>(), published.QueueName), cancellationToken);
            },
        });
    }
}
