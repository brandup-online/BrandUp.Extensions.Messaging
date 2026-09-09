using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public static class SqsMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the default SQS-compatible connection — Amazon SQS by region, or Yandex Message Queue
    /// and other compatible services by <see cref="SqsMessagingOptions.ServiceUrl"/>. Bind message
    /// types to queues on the returned builder. For several accounts or a declarative queue set, see
    /// <see cref="AddSqsMessaging{TContext}(IServiceCollection, Action{SqsMessagingOptions}, bool)"/>.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="configure">Connection options.</param>
    /// <param name="validateOnStart">
    /// Validate the connection options eagerly at host start (default). Pass <see langword="false"/> for
    /// optional messaging: validation then happens lazily, on first use.
    /// </param>
    public static SqsMessagingBuilder AddSqsMessaging(
        this IServiceCollection services, Action<SqsMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var registry = AddConnectionCore(services, Options.DefaultName, configure, validateOnStart);

        return new SqsMessagingBuilder(services, registry);
    }

    /// <summary>
    /// Registers a named connection — one SQS-compatible account. Messaging contexts are bound to it
    /// with <see cref="AddSqsMessaging{TContext}(IServiceCollection, string)"/>; several contexts
    /// sharing a name share one SQS client.
    /// </summary>
    /// <inheritdoc cref="AddSqsMessaging(IServiceCollection, Action{SqsMessagingOptions}, bool)" path="/param[@name='validateOnStart']"/>
    public static SqsMessagingConnectionBuilder AddSqsMessagingConnection(
        this IServiceCollection services, string name, Action<SqsMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        AddConnectionCore(services, name, configure, validateOnStart);

        return new SqsMessagingConnectionBuilder(services, name);
    }

    /// <summary>
    /// Registers the queues of a messaging context on its own connection, configured here. The
    /// connection is private to the context; to share one account between contexts use
    /// <see cref="AddSqsMessagingConnection(IServiceCollection, string, Action{SqsMessagingOptions}, bool)"/>.
    /// <para>
    /// A context that also declares <see cref="IMessageStream{TMessage}"/> properties is registered with
    /// the stream transport as well — <c>AddKinesisMessaging&lt;TContext&gt;(…)</c> — and each fills the
    /// properties it serves.
    /// </para>
    /// </summary>
    public static SqsMessagingContextBuilder<TContext> AddSqsMessaging<TContext>(
        this IServiceCollection services, Action<SqsMessagingOptions> configure, bool validateOnStart = true)
        where TContext : MessagingContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var connectionName = ConnectionNameOf(typeof(TContext));
        AddConnectionCore(services, connectionName, configure, validateOnStart);

        return services.AddSqsMessaging<TContext>(connectionName);
    }

    /// <summary>Registers the queues of a messaging context on an existing named connection.</summary>
    public static SqsMessagingContextBuilder<TContext> AddSqsMessaging<TContext>(
        this IServiceCollection services, string connectionName)
        where TContext : MessagingContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);

        AddCore(services);
        var registry = GetOrAddRegistry(services);
        var contextType = typeof(TContext);
        var model = MessagingModel.Build(contextType);      // property scan + validation at registration
        var method = $"AddSqsMessaging<{contextType.Name}>";

        // Only the queues: streams of the same context are bound by the stream transport.
        var queues = MessagingContexts.PropertiesFor(model, MessagingPropertyKind.Queue, method);

        // Guard the whole context before registering any of it: a failure halfway through would leave
        // earlier queues bound with no context to serve them.
        foreach (var property in queues)
            RegistrationGuards.EnsureNotBound(services, property.MessageType, method);

        foreach (var property in queues)
            AddQueueCore(services, registry, property.MessageType, connectionName, property.LogicalName, new QueueSettings());

        // The connection may be registered after the context, so this is checked when the context is
        // built rather than here.
        services.AddSingleton<IMessagingContextCheck>(new SqsConnectionCheck(contextType, connectionName, registry));
        MessagingContexts.EnsureRegistered(services, contextType, model);

        return new SqsMessagingContextBuilder<TContext>(services, registry, model);
    }

    /// <summary>Connection name implied by a messaging context type when it owns its connection.</summary>
    internal static string ConnectionNameOf(Type contextType) => contextType.FullName ?? contextType.Name;

    static SqsMessagingRegistry AddConnectionCore(
        IServiceCollection services, string name, Action<SqsMessagingOptions> configure, bool validateOnStart)
    {
        var builder = services.AddOptions<SqsMessagingOptions>(name).Configure(configure);
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
            ServiceDescriptor.Singleton<IValidateOptions<SqsMessagingOptions>, SqsMessagingOptionsValidator>());
        // Consumer options are validated when the hosted consumer reads them at startup, so a
        // misconfigured consumer fails the host instead of spinning in a receive-error loop.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<SqsConsumerOptions>, SqsConsumerOptionsValidator>());

        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<ISqsClientFactory, SqsClientFactory>();
        services.TryAddSingleton<ISqsQueueProvider, SqsQueueProvider>();
        // How a messaging context provisions its queues without knowing the transport.
        services.TryAddSingleton<IQueueProvisioner>(sp => sp.GetRequiredService<ISqsQueueProvider>());
        services.TryAddSingleton<IMessagePublisher, ServiceProviderMessagePublisher>();
    }

    // The registry is shared registration-time state, so it lives in the service collection as an
    // instance and is picked up by every subsequent AddSqsMessaging* call.
    static SqsMessagingRegistry GetOrAddRegistry(IServiceCollection services)
    {
        if (RegistrationGuards.FindInstance<SqsMessagingRegistry>(services) is { } existing)
            return existing;

        var registry = new SqsMessagingRegistry();
        services.AddSingleton(registry);

        return registry;
    }

    /// <summary>
    /// The one meaning of "bind a message type to a queue", shared by the typed builder and the
    /// context registration: registry entry, typed queue, and the sender alias the publisher routes on.
    /// </summary>
    internal static void AddQueueCore(
        IServiceCollection services, SqsMessagingRegistry registry,
        Type messageType, string connectionName, string logicalName, QueueSettings settings)
    {
        registry.AddQueue(messageType, connectionName, logicalName, settings);

        var queueServiceType = typeof(IMessageQueue<>).MakeGenericType(messageType);
        services.AddSingleton(queueServiceType, typeof(SqsMessageQueue<>).MakeGenericType(messageType));
        services.AddSingleton(typeof(IMessageSender<>).MakeGenericType(messageType), sp => sp.GetRequiredService(queueServiceType));
    }

    /// <summary>Consumer registration shared by the connection and context builders.</summary>
    internal static void AddConsumerCore<TMessage, THandler>(IServiceCollection services, Action<SqsConsumerOptions>? configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegistrationGuards.EnsureNoHandler<TMessage>(services, "AddConsumer");

        services.AddScoped<IMessageHandler<TMessage>, THandler>();
        if (configure is not null)
            services.Configure(SqsConsumerOptions.NameFor(typeof(TMessage)), configure);
        services.AddHostedService<SqsConsumerService<TMessage>>();
    }
}
