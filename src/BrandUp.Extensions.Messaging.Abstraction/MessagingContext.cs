using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Base class for a typed messaging context: the destinations of one bounded piece of an application,
/// declared in one place. Declare queues as <see cref="IMessageQueue{TMessage}"/> and streams as
/// <see cref="IMessageStream{TMessage}"/> properties; they are filled in when the context is created by
/// the DI container. The logical name comes from the property's <see cref="QueueAttribute"/>, else the
/// message type's, else the property name.
/// <para>
/// Queues and streams may live side by side in one context, but they come from different transports:
/// register the context with each of them — <c>AddSqsMessaging&lt;TContext&gt;(…)</c> fills the queues,
/// <c>AddKinesisMessaging&lt;TContext&gt;(…)</c> the streams.
/// </para>
/// </summary>
/// <example>
/// <code>
/// public class OrderMessaging : MessagingContext
/// {
///     public IMessageQueue&lt;OrderCreated&gt; Created { get; private set; } = null!;
///     [Queue("order-cancelled")] public IMessageQueue&lt;OrderCancelled&gt; Cancelled { get; private set; } = null!;
///     public IMessageStream&lt;OrderEvent&gt; Events { get; private set; } = null!;
/// }
/// </code>
/// </example>
public abstract class MessagingContext
{
    readonly Dictionary<Type, IMessageQueue> queues = [];
    readonly Dictionary<Type, IMessageStream> streams = [];
    IQueueProvisioner? provisioner;
    bool initialized;

    /// <summary>Queues of this context, keyed by message type.</summary>
    public IReadOnlyCollection<IMessageQueue> Queues
        => initialized ? queues.Values : throw NotInitialized();

    /// <summary>Streams of this context, keyed by message type.</summary>
    public IReadOnlyCollection<IMessageStream> Streams
        => initialized ? streams.Values : throw NotInitialized();

    /// <summary>Queue serving <typeparamref name="TMessage"/>.</summary>
    public IMessageQueue<TMessage> Queue<TMessage>()
        where TMessage : class
    {
        if (!initialized)
            throw NotInitialized();

        return queues.TryGetValue(typeof(TMessage), out var queue)
            ? (IMessageQueue<TMessage>)queue
            : throw new InvalidOperationException(
                $"Messaging context {GetType().Name} has no queue for message type {typeof(TMessage).FullName}.");
    }

    /// <summary>Stream serving <typeparamref name="TMessage"/>.</summary>
    public IMessageStream<TMessage> Stream<TMessage>()
        where TMessage : class
    {
        if (!initialized)
            throw NotInitialized();

        return streams.TryGetValue(typeof(TMessage), out var stream)
            ? (IMessageStream<TMessage>)stream
            : throw new InvalidOperationException(
                $"Messaging context {GetType().Name} has no stream for message type {typeof(TMessage).FullName}.");
    }

    /// <summary>
    /// Creates the queues of this context that do not exist yet, with the settings declared at
    /// registration (dead-letter queues included) — provisioning without repeating queue names, and
    /// without waiting for the lazy creation of <c>AutoCreateQueues</c>. Settings apply at creation
    /// only: an existing queue is never reconfigured.
    /// <para>
    /// Streams are not provisioned: a stream is created with a shard count and a retention period,
    /// which belong to infrastructure rather than to an application's start-up. A context without
    /// queues does nothing here.
    /// </para>
    /// </summary>
    public Task EnsureQueuesAsync(CancellationToken cancellationToken = default)
    {
        if (!initialized)
            throw NotInitialized();

        if (queues.Count == 0)
            return Task.CompletedTask;

        var queueProvisioner = provisioner
            ?? throw new InvalidOperationException(
                $"Queues of messaging context {GetType().Name} cannot be provisioned: no queue transport is registered. " +
                $"Call AddSqsMessaging<{GetType().Name}>(…), or AddFakeMessaging<{GetType().Name}>() in tests.");

        // Queues are provisioned independently; serially they would add up to N round-trips of
        // startup delay, and a context is provisioned on every deploy.
        return Task.WhenAll(queues.Keys.Select(messageType => queueProvisioner.EnsureQueueAsync(messageType, cancellationToken)));
    }

    // Called by the DI registration right after construction: the context itself stays free of any
    // knowledge about connections, options and credentials. Destinations are resolved by their property
    // type, so whichever transport registered one fills it - that is what lets one context span both.
    internal void Initialize(MessagingModel model, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(services);

        foreach (var property in model.Properties)
        {
            var destination = services.GetService(property.ServiceType) ?? throw Unbound(property);
            property.SetValue(this, destination);

            if (property.Kind == MessagingPropertyKind.Queue)
                queues[property.MessageType] = (IMessageQueue)destination;
            else
                streams[property.MessageType] = (IMessageStream)destination;
        }

        provisioner = services.GetService(typeof(IQueueProvisioner)) as IQueueProvisioner;
        initialized = true;
    }

    InvalidOperationException Unbound(MessagingProperty property)
    {
        var registration = property.Kind == MessagingPropertyKind.Queue
            ? $"AddSqsMessaging<{GetType().Name}>(…)"
            : $"AddKinesisMessaging<{GetType().Name}>(…)";

        return new InvalidOperationException(
            $"Messaging context {GetType().Name}: property {property.Property.Name} has no {MessagingModel.Word(property.Kind)} " +
            $"for message type {property.MessageType.FullName}. Register the transport serving it — {registration}, " +
            $"or AddFakeMessaging<{GetType().Name}>() in tests.");
    }

    InvalidOperationException NotInitialized()
        => new($"Messaging context {GetType().Name} is not initialized. Register it with a messaging provider, " +
            $"e.g. AddSqsMessaging<{GetType().Name}>(…), AddKinesisMessaging<{GetType().Name}>(…), " +
            $"or AddFakeMessaging<{GetType().Name}>() in tests.");
}
