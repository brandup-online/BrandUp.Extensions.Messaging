namespace BrandUp.Extensions.Messaging;

public interface IMessageStream
{
    /// <summary>
    /// Stream name as the provider expects it. For Yandex Data Streams this is the full path,
    /// e.g. <c>/ru-central1/b1g.../etn.../my-stream</c>.
    /// </summary>
    string Name { get; }
}

/// <summary>
/// Typed stream with log semantics: records are appended to shards and read by position, every consumer
/// group sees every record. Publishing goes through <see cref="IMessageSender{TMessage}"/>;
/// <see cref="PublishOptions.GroupId"/> becomes the partition key, so records sharing a group keep their
/// order. Consuming a stream (shard iteration, checkpoints) is out of scope of this abstraction for now.
/// </summary>
public interface IMessageStream<TMessage> : IMessageStream, IMessageSender<TMessage>
    where TMessage : class
{
}
