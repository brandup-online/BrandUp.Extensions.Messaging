using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public static class KinesisMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the default Kinesis-compatible connection — Amazon Kinesis Data Streams by region, or
    /// Yandex Data Streams by <see cref="KinesisMessagingOptions.ServiceUrl"/>. Bind message types to
    /// streams on the returned builder. For several accounts or a declarative stream set, see
    /// <see cref="AddKinesisMessaging{TContext}(IServiceCollection, Action{KinesisMessagingOptions}, bool)"/>.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="configure">Connection options.</param>
    /// <param name="validateOnStart">
    /// Validate the connection options eagerly at host start (default). Pass <see langword="false"/> for
    /// optional messaging: validation then happens lazily, on first use.
    /// </param>
    public static KinesisMessagingBuilder AddKinesisMessaging(
        this IServiceCollection services, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var registry = AddConnectionCore(services, Options.DefaultName, configure, validateOnStart);

        return new KinesisMessagingBuilder(services, registry);
    }

    /// <summary>
    /// Registers a named connection — one Kinesis-compatible account. Messaging contexts are bound to it
    /// with <see cref="AddKinesisMessaging{TContext}(IServiceCollection, string)"/>; several contexts
    /// sharing a name share one Kinesis client.
    /// </summary>
    /// <inheritdoc cref="AddKinesisMessaging(IServiceCollection, Action{KinesisMessagingOptions}, bool)" path="/param[@name='validateOnStart']"/>
    public static KinesisMessagingConnectionBuilder AddKinesisMessagingConnection(
        this IServiceCollection services, string name, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        AddConnectionCore(services, name, configure, validateOnStart);

        return new KinesisMessagingConnectionBuilder(services, name);
    }

    /// <summary>
    /// Registers the streams of a messaging context on its own connection, configured here: its
    /// <see cref="IMessageStream{TMessage}"/> properties are bound by the same naming rules as
    /// <see cref="KinesisMessagingBuilder.AddStream{TMessage}"/>. The connection is private to the
    /// context; to share one account between contexts use
    /// <see cref="AddKinesisMessagingConnection(IServiceCollection, string, Action{KinesisMessagingOptions}, bool)"/>.
    /// <para>
    /// A context that also declares queues is registered with the queue transport as well —
    /// <c>AddSqsMessaging&lt;TContext&gt;(…)</c> — and each fills the properties it serves.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="AddKinesisMessaging(IServiceCollection, Action{KinesisMessagingOptions}, bool)" path="/param[@name='validateOnStart']"/>
    public static KinesisMessagingContextBuilder<TContext> AddKinesisMessaging<TContext>(
        this IServiceCollection services, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
        where TContext : MessagingContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var connectionName = ConnectionNameOf(typeof(TContext));
        AddConnectionCore(services, connectionName, configure, validateOnStart);

        return services.AddKinesisMessaging<TContext>(connectionName);
    }

    /// <summary>Registers the streams of a messaging context on an existing named connection.</summary>
    public static KinesisMessagingContextBuilder<TContext> AddKinesisMessaging<TContext>(
        this IServiceCollection services, string connectionName)
        where TContext : MessagingContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);

        AddCore(services);
        var registry = GetOrAddRegistry(services);
        var contextType = typeof(TContext);
        var model = MessagingModel.Build(contextType);      // property scan + validation at registration
        var method = $"AddKinesisMessaging<{contextType.Name}>";

        // Only the streams: queues of the same context are bound by the queue transport.
        var streams = MessagingContexts.PropertiesFor(model, MessagingPropertyKind.Stream, method);

        // Guard the whole context before registering any of it: a failure halfway through would leave
        // earlier streams bound with no context to serve them.
        foreach (var property in streams)
            RegistrationGuards.EnsureNotBound(services, property.MessageType, method);

        foreach (var property in streams)
            AddStreamCore(services, registry, property.MessageType, connectionName, property.LogicalName);

        // The connection may be registered after the context, so this is checked when the context is
        // built rather than here.
        services.AddSingleton<IMessagingContextCheck>(new MessagingConnectionCheck(
            contextType, connectionName, registry.HasConnection, "AddKinesisMessagingConnection", "AddKinesisMessaging"));
        MessagingContexts.EnsureRegistered(services, contextType, model);

        return new KinesisMessagingContextBuilder<TContext>(services, model);
    }

    /// <summary>Connection name implied by a messaging context type when it owns its connection.</summary>
    internal static string ConnectionNameOf(Type contextType) => contextType.FullName ?? contextType.Name;

    static KinesisMessagingRegistry AddConnectionCore(
        IServiceCollection services, string name, Action<KinesisMessagingOptions> configure, bool validateOnStart)
    {
        var builder = services.AddOptions<KinesisMessagingOptions>(name).Configure(configure);
        if (validateOnStart)
            builder.ValidateOnStart();

        AddCore(services);

        var registry = GetOrAddRegistry(services);
        registry.AddConnection(name);

        return registry;
    }

    static void AddCore(IServiceCollection services)
    {
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisMessagingOptions>, KinesisMessagingOptionsValidator>());
        // Consumer options are validated when the hosted reader reads them at startup, so a
        // misconfigured reader fails the host instead of spinning in a read-error loop.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisConsumerOptions>, KinesisConsumerOptionsValidator>());

        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IKinesisClientFactory, KinesisClientFactory>();
        services.TryAddSingleton<IKinesisStreamProvider, KinesisStreamProvider>();
        services.TryAddSingleton<IMessagePublisher, ServiceProviderMessagePublisher>();
    }

    // The registry is shared registration-time state, so it lives in the service collection as an
    // instance and is picked up by every subsequent AddKinesisMessaging* call.
    static KinesisMessagingRegistry GetOrAddRegistry(IServiceCollection services)
        => RegistrationGuards.GetOrAddInstance<KinesisMessagingRegistry>(services);

    /// <summary>
    /// The one meaning of "bind a message type to a stream", shared by the typed builder and the context
    /// registration: registry entry, typed stream, and the sender alias the publisher routes on.
    /// </summary>
    internal static void AddStreamCore(
        IServiceCollection services, KinesisMessagingRegistry registry,
        Type messageType, string connectionName, string logicalName)
    {
        registry.AddStream(messageType, connectionName, logicalName);

        var streamServiceType = typeof(IMessageStream<>).MakeGenericType(messageType);
        services.AddSingleton(streamServiceType, typeof(KinesisMessageStream<>).MakeGenericType(messageType));
        services.AddSingleton(typeof(IMessageSender<>).MakeGenericType(messageType), sp => sp.GetRequiredService(streamServiceType));
    }

    /// <summary>Consumer registration shared by the connection and context builders.</summary>
    internal static void AddConsumerCore<TMessage, THandler>(
        IServiceCollection services, Action<KinesisConsumerOptions>? configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegistrationGuards.EnsureNoHandler<TMessage>(services, "AddConsumer");

        services.AddScoped<IMessageHandler<TMessage>, THandler>();
        if (configure is not null)
            services.Configure(KinesisConsumerOptions.NameFor(typeof(TMessage)), configure);

        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp =>
        {
            var checkpoints = sp.GetService<ICheckpointStore>()
                ?? throw new InvalidOperationException(
                    $"Reading stream {typeof(TMessage).Name} needs an {nameof(ICheckpointStore)}: register one " +
                    "(AddMongoMessagingCheckpoints in production, AddInMemoryMessagingCheckpoints in tests) before AddConsumer, " +
                    "or the reader would re-read the stream from the start after every restart.");

            // Optional: without a lease store the reader assumes it is the only one of its group.
            var leases = sp.GetService<IShardLeaseStore>();

            return leases is null
                ? ActivatorUtilities.CreateInstance<KinesisConsumerService<TMessage>>(sp, checkpoints)
                : ActivatorUtilities.CreateInstance<KinesisConsumerService<TMessage>>(sp, checkpoints, leases);
        });
    }
}
