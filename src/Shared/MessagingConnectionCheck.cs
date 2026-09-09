// Linked as a shared source file into the transports that have named connections (AmazonSqs,
// AmazonKinesis): the rule is the same for both, only the registration methods it names differ.
namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The connection a messaging context was bound to has to exist by the time the context is built.
/// Registration order is free — a context may be registered before its connection — so this is checked
/// when the context is created, where the error can still name what to call.
/// </summary>
internal sealed class MessagingConnectionCheck(
    Type contextType,
    string connectionName,
    Func<string, bool> hasConnection,
    string connectionMethod,
    string contextMethod) : IMessagingContextCheck
{
    public Type ContextType { get; } = contextType;

    public void Validate()
    {
        if (hasConnection(connectionName))
            return;

        throw new InvalidOperationException(
            $"Connection '{connectionName}' required by messaging context {ContextType.FullName} is not registered. " +
            $"Call {connectionMethod}(\"{connectionName}\", …) or {contextMethod}<{ContextType.Name}>(configure).");
    }
}
