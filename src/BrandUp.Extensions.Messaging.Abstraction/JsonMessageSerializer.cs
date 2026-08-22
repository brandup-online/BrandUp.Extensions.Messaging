using System.Text.Json;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Default serializer: System.Text.Json with web defaults (camelCase properties, case-insensitive
/// reading). The type name is the CLR full name without assembly, so renaming or moving a message type
/// changes it — consumers matching by type name must be updated together.
/// </summary>
public class JsonMessageSerializer(JsonSerializerOptions? options = null) : IMessageSerializer
{
    readonly JsonSerializerOptions options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);

    public virtual string GetTypeName(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        return messageType.FullName ?? messageType.Name;
    }

    public virtual string Serialize(object message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return JsonSerializer.Serialize(message, message.GetType(), options);
    }

    public virtual byte[] SerializeToUtf8Bytes(object message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), options);
    }

    public virtual object Deserialize(string payload, Type messageType)
    {
        ArgumentException.ThrowIfNullOrEmpty(payload);
        ArgumentNullException.ThrowIfNull(messageType);

        try
        {
            return JsonSerializer.Deserialize(payload, messageType, options)
                ?? throw new MessagingException($"Payload deserialized to null for message type {messageType.FullName}.");
        }
        catch (JsonException ex)
        {
            throw new MessagingException($"Payload is not valid JSON for message type {messageType.FullName}.", ex);
        }
    }
}
