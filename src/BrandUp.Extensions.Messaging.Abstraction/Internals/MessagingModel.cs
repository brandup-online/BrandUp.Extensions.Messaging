using System.Collections.Concurrent;
using System.Reflection;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>What a context property is bound to — the two transports a message type can live on.</summary>
internal enum MessagingPropertyKind
{
    Queue,
    Stream,
}

/// <summary>
/// One destination property of a messaging context: which message type it serves, whether it is a queue
/// or a stream, and its logical name. The physical name is resolved later, from the options of the
/// connection the context is bound to.
/// </summary>
internal sealed class MessagingProperty(
    PropertyInfo property, Type messageType, MessagingPropertyKind kind, string logicalName)
{
    public PropertyInfo Property { get; } = property;
    public Type MessageType { get; } = messageType;
    public MessagingPropertyKind Kind { get; } = kind;

    /// <summary>Logical queue or stream name — property [Queue], else the message type's [Queue], else the property name.</summary>
    public string LogicalName { get; } = logicalName;

    /// <summary>What the property is filled from: <c>IMessageQueue&lt;T&gt;</c> or <c>IMessageStream&lt;T&gt;</c>.</summary>
    public Type ServiceType => Property.PropertyType;

    /// <summary>Assigns the destination to the property; works with <c>init</c> and non-public setters.</summary>
    public void SetValue(object context, object destination) => Property.SetValue(context, destination);
}

/// <summary>
/// Destination composition of a messaging context type. Built once per type: property scan and
/// validation happen at registration, not on first access.
/// <para>
/// Queues and streams may be declared side by side. Each transport registers only the properties it
/// serves — the SQS registration the queues, the Kinesis one the streams — so a context spanning both
/// is registered twice, once per transport.
/// </para>
/// </summary>
internal sealed class MessagingModel
{
    static readonly ConcurrentDictionary<Type, MessagingModel> Cache = new();

    MessagingModel(Type contextType, IReadOnlyList<MessagingProperty> properties)
    {
        ContextType = contextType;
        Properties = properties;
        Queues = [.. properties.Where(p => p.Kind == MessagingPropertyKind.Queue)];
        Streams = [.. properties.Where(p => p.Kind == MessagingPropertyKind.Stream)];
    }

    public Type ContextType { get; }
    public IReadOnlyList<MessagingProperty> Properties { get; }
    public IReadOnlyList<MessagingProperty> Queues { get; }
    public IReadOnlyList<MessagingProperty> Streams { get; }

    /// <summary>Property by message type and kind; throws when the context has no such destination.</summary>
    public MessagingProperty RequireProperty(Type messageType, MessagingPropertyKind kind)
        => Properties.FirstOrDefault(p => p.MessageType == messageType && p.Kind == kind)
            ?? throw new InvalidOperationException(
                $"Messaging context {ContextType.Name} has no {Word(kind)} for message type {messageType.FullName}.");

    public static MessagingModel Build(Type contextType)
    {
        ArgumentNullException.ThrowIfNull(contextType);
        return Cache.GetOrAdd(contextType, Create);
    }

    static MessagingModel Create(Type contextType)
    {
        if (!typeof(MessagingContext).IsAssignableFrom(contextType))
            throw new InvalidOperationException($"Type {contextType.FullName} does not derive from {nameof(MessagingContext)}.");

        if (contextType.IsAbstract)
            throw new InvalidOperationException($"Messaging context {contextType.FullName} cannot be abstract.");

        var properties = new List<MessagingProperty>();

        foreach (var property in contextType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (DestinationOf(property.PropertyType) is not var (messageType, kind))
                continue;

            if (property.SetMethod is null)
                throw new InvalidOperationException(
                    $"Property {contextType.Name}.{property.Name} must have a setter (private set or init is enough).");

            // One message type, one destination — the same rule the transports enforce across packages.
            if (properties.FirstOrDefault(p => p.MessageType == messageType) is { } existing)
                throw new InvalidOperationException(
                    $"Message type {messageType.FullName} is mapped twice in {contextType.Name}: " +
                    $"{existing.Property.Name} and {property.Name}.");

            properties.Add(new MessagingProperty(property, messageType, kind, LogicalNames.ResolveForMember(messageType, property)));
        }

        if (properties.Count == 0)
            throw new InvalidOperationException(
                $"Messaging context {contextType.Name} declares no IMessageQueue<TMessage> or IMessageStream<TMessage> properties.");

        return new MessagingModel(contextType, properties);
    }

    static (Type MessageType, MessagingPropertyKind Kind)? DestinationOf(Type propertyType)
    {
        if (!propertyType.IsGenericType)
            return null;

        var definition = propertyType.GetGenericTypeDefinition();

        if (definition == typeof(IMessageQueue<>))
            return (propertyType.GetGenericArguments()[0], MessagingPropertyKind.Queue);

        if (definition == typeof(IMessageStream<>))
            return (propertyType.GetGenericArguments()[0], MessagingPropertyKind.Stream);

        return null;
    }

    internal static string Word(MessagingPropertyKind kind) => kind == MessagingPropertyKind.Queue ? "queue" : "stream";
}
