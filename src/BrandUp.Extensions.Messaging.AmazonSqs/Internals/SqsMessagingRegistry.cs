using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Registration-time state shared by the AddSqsMessaging builder calls: which message type is bound to
/// which logical queue on which connection, with what settings.
/// </summary>
internal sealed class SqsMessagingRegistry
{
    readonly Dictionary<Type, SqsQueueRegistration> queues = [];
    readonly HashSet<string> connections = [];

    public void AddConnection(string name) => connections.Add(name);

    public bool HasConnection(string name) => connections.Contains(name);

    public void AddQueue(Type messageType, string connectionName, string logicalName, QueueSettings settings)
    {
        if (!queues.TryAdd(messageType, new SqsQueueRegistration(connectionName, logicalName, settings)))
            throw new InvalidOperationException(
                $"Message type {messageType.FullName} is already bound to queue '{queues[messageType].LogicalName}'.");
    }

    public SqsQueueRegistration Get(Type messageType)
        => queues.TryGetValue(messageType, out var registration)
            ? registration
            : throw new MessagingException(
                $"Message type {messageType.FullName} is not bound to a queue. Register it with AddQueue<{messageType.Name}>() or a messaging context.");

    public bool Contains(Type messageType) => queues.ContainsKey(messageType);
}

/// <summary>The connection name is the options-monitor name; <see cref="Options.DefaultName"/> for the default connection.</summary>
internal sealed record SqsQueueRegistration(string ConnectionName, string LogicalName, QueueSettings Settings);
