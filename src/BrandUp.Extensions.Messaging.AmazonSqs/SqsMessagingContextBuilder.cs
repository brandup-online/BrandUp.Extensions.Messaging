using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>Builder of one registered messaging context: per-queue settings and consumers.</summary>
public class SqsMessagingContextBuilder<TContext>
    where TContext : MessagingContext
{
    readonly SqsMessagingRegistry registry;
    readonly MessagingModel model;

    internal SqsMessagingContextBuilder(IServiceCollection services, SqsMessagingRegistry registry, MessagingModel model)
    {
        Services = services;
        this.registry = registry;
        this.model = model;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Settings of one queue of the context — FIFO, dead-letter, visibility, poison policy. Applied
    /// when the queue is created (auto-creation or <see cref="MessagingContext.EnsureQueuesAsync"/>).
    /// </summary>
    public SqsMessagingContextBuilder<TContext> ConfigureQueue<TMessage>(Action<QueueSettings> configure)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(configure);

        // Validates the message type belongs to this context, then mutates the settings instance the
        // registry already holds - the queue reads them lazily, at creation time.
        model.RequireProperty(typeof(TMessage));
        var settings = registry.Get(typeof(TMessage)).Settings;
        configure(settings);
        SqsLimits.ValidateSettings(settings, nameof(configure));

        return this;
    }

    /// <summary>
    /// Registers the hosted consumer of <typeparamref name="TMessage"/> — one of the context's queues.
    /// One consumer per message type; a second registration throws. See
    /// <see cref="SqsMessagingBuilder.AddConsumer{TMessage, THandler}"/> for the semantics.
    /// </summary>
    public SqsMessagingContextBuilder<TContext> AddConsumer<TMessage, THandler>(Action<SqsConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        model.RequireProperty(typeof(TMessage));
        SqsMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }
}
