using System.Text;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Turns messages into transport payloads. One serializer serves every queue and stream of the
/// application; replace the default JSON one by registering an implementation before the provider.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Stable name of the message type, carried next to the payload (SQS message attribute). On
    /// receive it is compared with the expected type: a mismatch marks the message as poison.
    /// </summary>
    string GetTypeName(Type messageType);

    string Serialize(object message);

    /// <summary>
    /// Payload as UTF-8 bytes, for byte-oriented transports (streams). The default implementation
    /// encodes <see cref="Serialize"/>; override to serialize straight to bytes.
    /// </summary>
    byte[] SerializeToUtf8Bytes(object message) => Encoding.UTF8.GetBytes(Serialize(message));

    /// <summary>Deserializes the payload; throws when the payload does not represent <paramref name="messageType"/>.</summary>
    object Deserialize(string payload, Type messageType);
}
