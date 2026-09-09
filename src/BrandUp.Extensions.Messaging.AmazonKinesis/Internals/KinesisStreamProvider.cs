using Amazon.Kinesis;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Runtime stream lookup: which connection a message type's stream lives on, and what its physical name
/// is there. Streams bound to different connections are read and written through different clients,
/// which is what lets one application span several accounts.
/// </summary>
internal interface IKinesisStreamProvider
{
    IAmazonKinesis ClientFor(Type messageType);

    string ResolveName(Type messageType);
}

internal sealed class KinesisStreamProvider(
    IKinesisClientFactory clientFactory,
    KinesisMessagingRegistry registry,
    IOptionsMonitor<KinesisMessagingOptions> optionsMonitor) : IKinesisStreamProvider
{
    public IAmazonKinesis ClientFor(Type messageType) => clientFactory.Get(registry.Get(messageType).ConnectionName);

    public string ResolveName(Type messageType)
    {
        var registration = registry.Get(messageType);

        // The physical name comes from the options of the connection the stream is bound to: the same
        // logical name may point at different streams in different accounts.
        var options = optionsMonitor.Get(registration.ConnectionName);
        var resolved = options.Streams.TryGetValue(registration.LogicalName, out var physicalName)
            ? physicalName
            : registration.LogicalName;

        // Fail with a clear error at first use instead of an opaque PutRecord rejection.
        if (string.IsNullOrWhiteSpace(resolved))
            throw new MessagingException($"Physical stream name mapped for '{registration.LogicalName}' is empty.");

        return resolved;
    }
}
