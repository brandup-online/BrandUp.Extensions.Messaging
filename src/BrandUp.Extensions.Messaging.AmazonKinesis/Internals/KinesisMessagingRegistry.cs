namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Registration-time state shared by the AddKinesisMessaging builder calls: which message type is bound
/// to which logical stream on which connection.
/// </summary>
internal sealed class KinesisMessagingRegistry
{
    readonly Dictionary<Type, KinesisStreamRegistration> streams = [];
    readonly HashSet<string> connections = [];

    public void AddConnection(string name) => connections.Add(name);

    public bool HasConnection(string name) => connections.Contains(name);

    public void AddStream(Type messageType, string connectionName, string logicalName)
    {
        if (!streams.TryAdd(messageType, new KinesisStreamRegistration(connectionName, logicalName)))
            throw new InvalidOperationException(
                $"Message type {messageType.FullName} is already bound to stream '{streams[messageType].LogicalName}'.");
    }

    public bool Contains(Type messageType) => streams.ContainsKey(messageType);

    public KinesisStreamRegistration Get(Type messageType)
        => streams.TryGetValue(messageType, out var registration)
            ? registration
            : throw new MessagingException(
                $"Message type {messageType.FullName} is not bound to a stream. Register it with AddStream<{messageType.Name}>() or a messaging context.");
}

/// <summary>The connection name is the options-monitor name; <see cref="Microsoft.Extensions.Options.Options.DefaultName"/> for the default connection.</summary>
internal sealed record KinesisStreamRegistration(string ConnectionName, string LogicalName);
