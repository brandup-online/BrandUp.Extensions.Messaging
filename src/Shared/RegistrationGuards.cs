// Linked as a shared source file into every transport package (AmazonSqs, AmazonKinesis, Testing), so
// duplicate-binding rules are identical everywhere — including across packages, which no single
// package's registry can see.
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging.Internals;

internal static class RegistrationGuards
{
    /// <summary>
    /// The singleton instance of <typeparamref name="T"/> already put into the collection, if any —
    /// how the packages find registration-time state shared across repeated Add* calls.
    /// </summary>
    public static T? FindInstance<T>(IServiceCollection services) where T : class
    {
        foreach (var descriptor in services)
        {
            if (!descriptor.IsKeyedService
                && descriptor.ServiceType == typeof(T)
                && descriptor.ImplementationInstance is T existing)
                return existing;
        }

        return null;
    }

    /// <summary>
    /// The registration-time state of a transport: the instance already in the collection, or a new one
    /// put there. Every Add* call of a transport works on the same instance, which is what lets them
    /// see each other's bindings.
    /// </summary>
    public static T GetOrAddInstance<T>(IServiceCollection services) where T : class, new()
    {
        if (FindInstance<T>(services) is { } existing)
            return existing;

        var created = new T();
        services.AddSingleton(created);

        return created;
    }

    /// <summary>
    /// A message type is bound to exactly one destination. Without this guard a second registration —
    /// a queue in one package, a stream in another — would silently last-win in DI and reroute
    /// messages.
    /// </summary>
    public static void EnsureNotBound<TMessage>(IServiceCollection services, string registrationMethod)
        where TMessage : class
        => EnsureNotBound(services, typeof(TMessage), registrationMethod);

    public static void EnsureNotBound(IServiceCollection services, Type messageType, string registrationMethod)
    {
        var senderType = typeof(IMessageSender<>).MakeGenericType(messageType);
        foreach (var descriptor in services)
        {
            if (!descriptor.IsKeyedService && descriptor.ServiceType == senderType)
                throw new InvalidOperationException(
                    $"Message type {messageType.FullName} is already bound to a queue or stream; {registrationMethod} was called twice, or the type is bound in another transport.");
        }
    }

    /// <summary>
    /// One handler per message type. Without this guard a second registration would silently replace
    /// the first: the hosted service is deduplicated by type and GetRequiredService resolves the last
    /// handler only.
    /// </summary>
    public static void EnsureNoHandler<TMessage>(IServiceCollection services, string registrationMethod)
        where TMessage : class
    {
        foreach (var descriptor in services)
        {
            if (!descriptor.IsKeyedService && descriptor.ServiceType == typeof(IMessageHandler<TMessage>))
                throw new InvalidOperationException(
                    $"Message type {typeof(TMessage).FullName} already has a handler; only one {registrationMethod} per message type is supported.");
        }
    }
}
