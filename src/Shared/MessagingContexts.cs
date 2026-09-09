// Linked as a shared source file into every transport package (AmazonSqs, AmazonKinesis, Testing), so a
// context is created the same way whichever transport registers it - and so a context spanning queues
// and streams is registered once, not once per transport.
using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging.Internals;

internal static class MessagingContexts
{
    /// <summary>
    /// Registers the context as a singleton, unless another transport already did. Every property is
    /// resolved by its own service type, so the registration does not care which transport bound what;
    /// the preconditions each transport declared are checked first, while the error can still name what
    /// is missing.
    /// </summary>
    public static void EnsureRegistered(IServiceCollection services, Type contextType, MessagingModel model)
    {
        // Only our own registration counts: a context the application happens to have registered itself
        // would otherwise be left uninitialized, with no property filled in.
        foreach (var descriptor in services)
        {
            if (!descriptor.IsKeyedService
                && descriptor.ImplementationInstance is MessagingContextRegistration marker
                && marker.ContextType == contextType)
                return;
        }

        services.AddSingleton(new MessagingContextRegistration(contextType));

        services.AddSingleton(contextType, serviceProvider =>
        {
            foreach (var check in serviceProvider.GetServices<IMessagingContextCheck>())
            {
                if (check.ContextType == contextType)
                    check.Validate();
            }

            var context = (MessagingContext)ActivatorUtilities.CreateInstance(serviceProvider, contextType);
            context.Initialize(model, serviceProvider);

            return context;
        });
    }

    /// <summary>
    /// The destinations of <paramref name="model"/> one transport serves — and a clear error when the
    /// context declares none of them, so a misdirected registration is caught at once.
    /// </summary>
    public static IReadOnlyList<MessagingProperty> PropertiesFor(
        MessagingModel model, MessagingPropertyKind kind, string registrationMethod)
    {
        var isQueue = kind == MessagingPropertyKind.Queue;
        var properties = isQueue ? model.Queues : model.Streams;

        if (properties.Count == 0)
            throw new InvalidOperationException(
                $"Messaging context {model.ContextType.Name} declares no {(isQueue ? "IMessageQueue" : "IMessageStream")}<TMessage> " +
                $"properties, so {registrationMethod} has nothing to bind.");

        return properties;
    }
}
