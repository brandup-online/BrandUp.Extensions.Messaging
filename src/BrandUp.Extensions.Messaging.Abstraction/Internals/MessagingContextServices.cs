namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// How <see cref="MessagingContext.EnsureQueuesAsync"/> reaches the transport that owns the queues.
/// Implemented by the queue transport (SQS) and by the testing fake; an application that only uses
/// streams registers none, and provisions nothing.
/// </summary>
internal interface IQueueProvisioner
{
    Task EnsureQueueAsync(Type messageType, CancellationToken cancellationToken);
}

/// <summary>
/// Marks a messaging context as already registered by one of the transports. It lives in the
/// abstraction rather than in the shared sources on purpose: the transports must recognise each
/// other's registrations, and a type compiled into each package separately would not match.
/// </summary>
internal sealed class MessagingContextRegistration(Type contextType)
{
    public Type ContextType { get; } = contextType;
}

/// <summary>
/// A precondition one transport puts on one context — checked when the context is built, because what
/// it needs (a named connection, say) may be registered after it. Every transport that serves part of
/// a context adds its own, so a context spanning queues and streams is checked by both.
/// </summary>
internal interface IMessagingContextCheck
{
    Type ContextType { get; }

    void Validate();
}
