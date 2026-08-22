using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace BrandUp.Extensions.Messaging;

public static class FakeMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the in-memory messaging fake: the same <see cref="IMessagePublisher"/> and typed queues
    /// as in production, backed by <see cref="InMemoryMessageBus"/>. Bind message types on the returned
    /// builder, then assert on <see cref="InMemoryMessageBus.Published"/> or pump messages into handlers
    /// with <see cref="InMemoryMessageBus.DispatchPendingAsync"/>. Repeated calls share one bus, so
    /// composition modules can each register their own queues.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="bus">Bus to back the queues with; the already-registered or a new one when omitted.</param>
    public static FakeMessagingBuilder AddFakeMessaging(this IServiceCollection services, InMemoryMessageBus? bus = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        // One bus per service collection: a second bus would leave earlier queues publishing into an
        // instance the test cannot see.
        var registered = RegistrationGuards.FindInstance<InMemoryMessageBus>(services);
        if (registered is not null && bus is not null && !ReferenceEquals(registered, bus))
            throw new InvalidOperationException(
                "A different InMemoryMessageBus is already registered; pass the same instance or omit the parameter to reuse it.");

        bus ??= registered ?? new InMemoryMessageBus();
        if (registered is null)
            services.AddSingleton(bus);

        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IMessagePublisher, ServiceProviderMessagePublisher>();

        return new FakeMessagingBuilder(services, bus);
    }

    /// <summary>
    /// Registers a messaging context backed by the in-memory bus: the same context type as in
    /// production, its queues resolved by the same naming rules. Unlike a real connection, the logical
    /// name is the physical one — in tests a name is just a label. <c>EnsureQueuesAsync</c> is a no-op:
    /// fake queues always exist.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="bus">Bus to back the queues with; the already-registered or a new one when omitted.</param>
    public static FakeMessagingBuilder AddFakeMessaging<TContext>(this IServiceCollection services, InMemoryMessageBus? bus = null)
        where TContext : MessagingContext
    {
        var builder = services.AddFakeMessaging(bus);
        var contextType = typeof(TContext);
        var model = MessagingModel.Build(contextType);

        // Guard the whole context before registering any of it, like the real providers do.
        foreach (var property in model.Properties)
            RegistrationGuards.EnsureNotBound(services, property.MessageType, $"AddFakeMessaging<{contextType.Name}>");

        foreach (var property in model.Properties)
            builder.AddQueueCore(property.MessageType, property.LogicalName, new QueueSettings());

        services.AddSingleton(sp =>
        {
            var context = ActivatorUtilities.CreateInstance<TContext>(sp);
            context.Initialize(
                model,
                messageType => (IMessageQueue)sp.GetRequiredService(typeof(IMessageQueue<>).MakeGenericType(messageType)),
                static (_, _) => Task.CompletedTask);

            return context;
        });

        return builder;
    }
}

public class FakeMessagingBuilder
{
    readonly InMemoryMessageBus bus;

    internal FakeMessagingBuilder(IServiceCollection services, InMemoryMessageBus bus)
    {
        Services = services;
        this.bus = bus;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Binds a message type to a fake queue, by the same rule as a real transport: the name comes from
    /// <paramref name="queueName"/> or the type's <see cref="QueueAttribute"/> — a type that would fail
    /// production registration fails here too. Binding a type twice throws, like in production.
    /// <paramref name="configure"/> declares the queue's shape (e.g. <see cref="QueueSettings.Fifo"/>),
    /// so publish-time rules match the real queue; creation-time settings have no effect on a fake.
    /// </summary>
    public FakeMessagingBuilder AddQueue<TMessage>(string? queueName = null, Action<QueueSettings>? configure = null)
        where TMessage : class
    {
        var name = LogicalNames.Resolve(typeof(TMessage), queueName, nameof(queueName), nameof(AddQueue));
        RegistrationGuards.EnsureNotBound<TMessage>(Services, nameof(AddQueue));

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        SqsLimits.ValidateSettings(settings, nameof(configure));

        AddQueueCore(typeof(TMessage), name, settings);

        return this;
    }

    /// <summary>Registers the typed queue and the sender alias — the fake counterpart of a provider's queue binding.</summary>
    internal void AddQueueCore(Type messageType, string name, QueueSettings settings)
    {
        var queueServiceType = typeof(IMessageQueue<>).MakeGenericType(messageType);
        var queueType = typeof(FakeMessageQueue<>).MakeGenericType(messageType);

        Services.AddSingleton(queueServiceType, sp => Activator.CreateInstance(
            queueType, bus, name, sp.GetRequiredService<IMessageSerializer>(), settings)!);
        Services.AddSingleton(typeof(IMessageSender<>).MakeGenericType(messageType), sp => sp.GetRequiredService(queueServiceType));
    }

    /// <summary>
    /// Registers a handler, so pending messages can be dispatched into it by the bus. One handler per
    /// message type — a second registration throws, like <c>AddConsumer</c> in production.
    /// </summary>
    public FakeMessagingBuilder AddHandler<TMessage, THandler>()
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegistrationGuards.EnsureNoHandler<TMessage>(Services, nameof(AddHandler));

        Services.AddScoped<IMessageHandler<TMessage>, THandler>();
        return this;
    }
}
