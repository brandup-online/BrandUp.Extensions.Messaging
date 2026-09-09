namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The connection a messaging context's queues were bound to has to exist by the time the context is
/// built. Registration order is free — a context may be registered before its connection — so this is
/// checked when the context is created, where the error can still name what to call.
/// </summary>
internal sealed class SqsConnectionCheck(Type contextType, string connectionName, SqsMessagingRegistry registry)
    : IMessagingContextCheck
{
    public Type ContextType { get; } = contextType;

    public void Validate()
    {
        if (registry.HasConnection(connectionName))
            return;

        throw new InvalidOperationException(
            $"Connection '{connectionName}' required by messaging context {ContextType.FullName} is not registered. " +
            $"Call AddSqsMessagingConnection(\"{connectionName}\", …) or AddSqsMessaging<{ContextType.Name}>(configure).");
    }
}
