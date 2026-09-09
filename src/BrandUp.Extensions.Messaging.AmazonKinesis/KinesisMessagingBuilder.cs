using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class KinesisMessagingBuilder
{
    internal KinesisMessagingBuilder(IServiceCollection services)
    {
        Services = services;
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

        AddStreamCore(typeof(TMessage), logicalName);

        return this;
    }

    /// <summary>
    /// Binds the <see cref="IMessageStream{TMessage}"/> properties of a messaging context to this
    /// connection — the declarative counterpart of <see cref="AddStream{TMessage}"/>. Queues of the
    /// same context are bound separately, by the queue transport.
    /// </summary>
    public KinesisMessagingContextBuilder<TContext> AddContext<TContext>()
        where TContext : MessagingContext
    {
        var contextType = typeof(TContext);
        var model = MessagingModel.Build(contextType);      // property scan + validation at registration
        var method = $"AddContext<{contextType.Name}>";

        var streams = MessagingContexts.PropertiesFor(model, MessagingPropertyKind.Stream, method);

        // Guard the whole context before registering any of it: a failure halfway through would leave
        // earlier streams bound with no context to serve them.
        foreach (var property in streams)
            RegistrationGuards.EnsureNotBound(Services, property.MessageType, method);

        foreach (var property in streams)
            AddStreamCore(property.MessageType, property.LogicalName);

        MessagingContexts.EnsureRegistered(Services, contextType, model);

        return new KinesisMessagingContextBuilder<TContext>(Services, model);
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
        if (!Services.Any(d => d.ServiceType == typeof(IMessageStream<TMessage>)))
            AddStream<TMessage>();

        KinesisMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }

    /// <summary>The one meaning of "bind a message type to a stream": the typed stream and the sender alias.</summary>
    internal void AddStreamCore(Type messageType, string logicalName)
    {
        var streamServiceType = typeof(IMessageStream<>).MakeGenericType(messageType);
        var streamType = typeof(KinesisMessageStream<>).MakeGenericType(messageType);

        Services.AddSingleton(streamServiceType, sp => Activator.CreateInstance(
            streamType,
            sp.GetRequiredService<IKinesisClientFactory>(),
            sp.GetRequiredService<IMessageSerializer>(),
            sp.GetRequiredService<IOptions<KinesisMessagingOptions>>(),
            logicalName)!);

        Services.AddSingleton(
            typeof(IMessageSender<>).MakeGenericType(messageType), sp => sp.GetRequiredService(streamServiceType));
    }
}

/// <summary>Builder of one registered messaging context: the consumers of its streams.</summary>
public class KinesisMessagingContextBuilder<TContext>
    where TContext : MessagingContext
{
    readonly MessagingModel model;

    internal KinesisMessagingContextBuilder(IServiceCollection services, MessagingModel model)
    {
        Services = services;
        this.model = model;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers the hosted reader of <typeparamref name="TMessage"/> — one of the context's streams.
    /// One reader per message type; a second registration throws. See
    /// <see cref="KinesisMessagingBuilder.AddConsumer{TMessage, THandler}"/> for the semantics.
    /// </summary>
    public KinesisMessagingContextBuilder<TContext> AddConsumer<TMessage, THandler>(Action<KinesisConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        model.RequireProperty(typeof(TMessage), MessagingPropertyKind.Stream);
        KinesisMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }
}
