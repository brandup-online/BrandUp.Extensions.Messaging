using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class KinesisMessagingBuilder
{
    readonly KinesisMessagingRegistry registry;

    internal KinesisMessagingBuilder(IServiceCollection services, KinesisMessagingRegistry registry)
    {
        Services = services;
        this.registry = registry;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Takes the credentials of this connection from <typeparamref name="TProvider"/> instead of static
    /// keys: the provider is asked to renew them on a timer, and the client picks up what it caches
    /// without being rebuilt. What a provider is for — temporary (STS) credentials, a vault, a token
    /// exchanged for keys — see <see cref="IMessagingCredentialsProvider"/>.
    /// </summary>
    /// <param name="refreshInterval">
    /// How often the provider is asked to renew; one minute by default. The provider renews only what
    /// needs renewing, so this is a heartbeat rather than a rotation period.
    /// </param>
    public KinesisMessagingBuilder UseCredentialsProvider<TProvider>(TimeSpan? refreshInterval = null)
        where TProvider : class, IMessagingCredentialsProvider
    {
        CredentialsRegistrations.Add<TProvider>(Services, Options.DefaultName, refreshInterval, nameof(UseCredentialsProvider));

        return this;
    }

    /// <inheritdoc cref="UseCredentialsProvider{TProvider}(TimeSpan?)"/>
    public KinesisMessagingBuilder UseCredentialsProvider(
        Func<IServiceProvider, IMessagingCredentialsProvider> factory, TimeSpan? refreshInterval = null)
    {
        CredentialsRegistrations.Add(Services, Options.DefaultName, refreshInterval, factory, nameof(UseCredentialsProvider));

        return this;
    }

    /// <summary>
    /// Binds a message type to a stream. The logical name comes from <paramref name="streamName"/> or
    /// the type's <see cref="QueueAttribute"/>; the physical name (for Yandex Data Streams — the full
    /// stream path) is resolved via <see cref="KinesisMessagingOptions.Streams"/>. Registers
    /// <see cref="IMessageStream{TMessage}"/> and makes the type publishable through
    /// <see cref="IMessagePublisher"/>. A message type is bound to exactly one destination:
    /// binding it again — here or in another transport — throws.
    /// </summary>
    public KinesisMessagingBuilder AddStream<TMessage>(string? streamName = null)
        where TMessage : class
    {
        var logicalName = LogicalNames.Resolve(typeof(TMessage), streamName, nameof(streamName), nameof(AddStream));
        RegistrationGuards.EnsureNotBound<TMessage>(Services, nameof(AddStream));

        KinesisMessagingServiceCollectionExtensions.AddStreamCore(
            Services, registry, typeof(TMessage), Options.DefaultName, logicalName);

        return this;
    }

    /// <summary>
    /// Registers the hosted reader of <typeparamref name="TMessage"/>: it reads every shard of the
    /// stream, hands records to <typeparamref name="THandler"/> in their own DI scope and stores its
    /// position in the <see cref="ICheckpointStore"/> after each handled record.
    /// <para>
    /// A checkpoint store must be registered — <c>AddMongoMessagingCheckpoints(…)</c> in production, or
    /// <c>AddInMemoryMessagingCheckpoints()</c> from the Testing package. Without one the reader would restart
    /// from the beginning of the stream on every deploy, so registration fails instead.
    /// </para>
    /// <para>
    /// A shard is read by one instance. Register an <see cref="IShardLeaseStore"/>
    /// (<c>AddMongoMessagingShardLeases(…)</c>, or <c>AddInMemoryMessagingShardLeases()</c> in tests) to run several
    /// instances in one consumer group: they lease the shards between them and take over from an
    /// instance that stops. Without one, run a single instance per group — or give each instance its
    /// own <see cref="KinesisConsumerOptions.ConsumerGroup"/> if they should each see every record.
    /// </para>
    /// </summary>
    public KinesisMessagingBuilder AddConsumer<TMessage, THandler>(Action<KinesisConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        if (!registry.Contains(typeof(TMessage)))
            AddStream<TMessage>();

        KinesisMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }

}
