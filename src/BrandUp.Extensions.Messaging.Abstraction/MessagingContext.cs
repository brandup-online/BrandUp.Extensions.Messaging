using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Base class for a typed messaging context: a set of queues bound to one connection (one cloud
/// account). Declare queues as <see cref="IMessageQueue{TMessage}"/> properties; they are filled in
/// when the context is created by the DI container. The logical queue name comes from the property's
/// <see cref="QueueAttribute"/>, else the message type's, else the property name.
/// </summary>
/// <example>
/// <code>
/// public class OrderMessaging : MessagingContext
/// {
///     public IMessageQueue&lt;OrderCreated&gt; Created { get; private set; } = null!;
///     [Queue("order-cancelled")] public IMessageQueue&lt;OrderCancelled&gt; Cancelled { get; private set; } = null!;
/// }
/// </code>
/// </example>
public abstract class MessagingContext
{
    readonly Dictionary<Type, IMessageQueue> queues = [];
    Func<Type, CancellationToken, Task>? ensureQueue;
    bool initialized;

    /// <summary>Queues of this context, keyed by message type.</summary>
    public IReadOnlyCollection<IMessageQueue> Queues
        => initialized ? queues.Values : throw NotInitialized();

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

    /// <summary>
    /// Creates the queues of this context that do not exist yet, with the settings declared at
    /// registration (dead-letter queues included) — provisioning without repeating queue names, and
    /// without waiting for the lazy creation of <c>AutoCreateQueues</c>. Settings apply at creation
    /// only: an existing queue is never reconfigured.
    /// </summary>
    public Task EnsureQueuesAsync(CancellationToken cancellationToken = default)
    {
        if (!initialized)
            throw NotInitialized();

        // Queues are provisioned independently; serially they would add up to N round-trips of
        // startup delay, and a context is provisioned on every deploy.
        return Task.WhenAll(queues.Keys.Select(messageType => ensureQueue!(messageType, cancellationToken)));
    }

    // Called by the DI registration right after construction: the context itself stays free of any
    // knowledge about connections, options and credentials.
    internal void Initialize(
        MessagingModel model,
        Func<Type, IMessageQueue> queueFactory,
        Func<Type, CancellationToken, Task> ensureQueue)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(queueFactory);
        ArgumentNullException.ThrowIfNull(ensureQueue);

        foreach (var property in model.Properties)
        {
            var queue = queueFactory(property.MessageType);
            property.SetValue(this, queue);
            queues[property.MessageType] = queue;
        }

        this.ensureQueue = ensureQueue;
        initialized = true;
    }

    InvalidOperationException NotInitialized()
        => new($"Messaging context {GetType().Name} is not initialized. Register it with a messaging provider, " +
            $"e.g. AddSqsMessaging<{GetType().Name}>(…) or AddFakeMessaging<{GetType().Name}>() in tests.");
}
