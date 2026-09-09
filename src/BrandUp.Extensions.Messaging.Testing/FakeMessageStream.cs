using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// In-memory <see cref="IMessageStream{TMessage}"/> over <see cref="InMemoryMessageBus"/>: a published
/// record is captured like a queue message and can be pumped into its handler with
/// <see cref="InMemoryMessageBus.DispatchPendingAsync"/> — the fake counterpart of the hosted shard
/// reader. Payloads go through the registered <see cref="IMessageSerializer"/>, and options a real
/// stream refuses (<see cref="PublishOptions.Delay"/>, <see cref="PublishOptions.DeduplicationId"/>)
/// are refused here too.
/// <para>
/// Shards are not emulated: records are kept in publish order and delivered in it, which is what a
/// single shard — or one partition key — guarantees in production.
/// </para>
/// </summary>
public class FakeMessageStream<TMessage>(InMemoryMessageBus bus, string name, IMessageSerializer serializer)
    : IMessageStream<TMessage>
    where TMessage : class
{
    public string Name { get; } = name;

    public Task<PublishResult> PublishAsync(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        // A stream always has a partition key, so a record always arrives grouped.
        var partitionKey = StreamLimits.ResolvePartitionKey(options, nameof(options));
        var added = FakePublications.Publish(bus, Name, message, options, partitionKey, serializer);

        // A real stream answers with the record's sequence number, and uses it as the message id.
        return Task.FromResult(new PublishResult { MessageId = added.MessageId, SequenceNumber = added.MessageId });
    }

    public Task<IReadOnlyList<PublishResult>> PublishAsync(
        IReadOnlyCollection<PublishMessage<TMessage>> messages, CancellationToken cancellationToken = default)
        => FakePublications.PublishEachAsync(
            messages,
            (item, parameterName) => StreamLimits.ResolvePartitionKey(item.Options, parameterName),
            PublishAsync,
            cancellationToken);
}
