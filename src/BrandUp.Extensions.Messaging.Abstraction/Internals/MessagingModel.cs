using System.Collections.Concurrent;
using System.Reflection;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// One <see cref="IMessageQueue{TMessage}"/> property of a messaging context: which message type it
/// serves and its logical queue name. The physical name is resolved later, from the options of the
/// connection the context is bound to.
/// </summary>
internal sealed class MessagingProperty(PropertyInfo property, Type messageType, string logicalName)
{
    public PropertyInfo Property { get; } = property;
    public Type MessageType { get; } = messageType;

    /// <summary>Logical queue name — property [Queue], else the message type's [Queue], else the property name.</summary>
    public string LogicalName { get; } = logicalName;

    /// <summary>Assigns the queue to the property; works with <c>init</c> and non-public setters.</summary>
    public void SetValue(object context, object queue) => Property.SetValue(context, queue);
}

/// <summary>
/// Queue composition of a messaging context type. Built once per type: property scan and validation
/// happen at registration, not on first access.
/// </summary>
internal sealed class MessagingModel
{
    static readonly ConcurrentDictionary<Type, MessagingModel> Cache = new();

    MessagingModel(Type contextType, IReadOnlyList<MessagingProperty> properties)
    {
        ContextType = contextType;
        Properties = properties;
    }

    public Type ContextType { get; }
    public IReadOnlyList<MessagingProperty> Properties { get; }

    /// <summary>Property by message type; throws when the context has no queue for it.</summary>
    public MessagingProperty RequireProperty(Type messageType)
        => Properties.FirstOrDefault(p => p.MessageType == messageType)
            ?? throw new InvalidOperationException(
                $"Messaging context {ContextType.Name} has no queue for message type {messageType.FullName}.");

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
            var messageType = MessageTypeOf(property.PropertyType);
            if (messageType is null)
                continue;

            if (property.SetMethod is null)
                throw new InvalidOperationException(
                    $"Property {contextType.Name}.{property.Name} must have a setter (private set or init is enough).");

            if (properties.FirstOrDefault(p => p.MessageType == messageType) is { } existing)
                throw new InvalidOperationException(
                    $"Message type {messageType.FullName} is mapped twice in {contextType.Name}: " +
                    $"{existing.Property.Name} and {property.Name}.");

            properties.Add(new MessagingProperty(property, messageType, LogicalNames.ResolveForMember(messageType, property)));
        }

        if (properties.Count == 0)
            throw new InvalidOperationException(
                $"Messaging context {contextType.Name} declares no IMessageQueue<TMessage> properties.");

        return new MessagingModel(contextType, properties);
    }

    static Type? MessageTypeOf(Type propertyType)
    {
        if (!propertyType.IsGenericType)
            return null;

        var definition = propertyType.GetGenericTypeDefinition();
        if (definition == typeof(IMessageQueue<>))
            return propertyType.GetGenericArguments()[0];

        if (definition == typeof(IMessageStream<>))
            throw new NotSupportedException(
                "IMessageStream<TMessage> properties on a messaging context are not supported yet; " +
                "register streams directly on the stream provider's builder.");

        return null;
    }
}
