using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>Builder of one registered messaging context: the consumers of its streams.</summary>
public class KinesisMessagingContextBuilder<TContext>
    where TContext : MessagingContext
{
    readonly MessagingModel model;

    internal KinesisMessagingContextBuilder(IServiceCollection services, MessagingModel model)
    {
        Services = services;
        this.model = model;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers the hosted reader of <typeparamref name="TMessage"/> — one of the context's streams.
    /// One reader per message type; a second registration throws. See
    /// <see cref="KinesisMessagingBuilder.AddConsumer{TMessage, THandler}"/> for the semantics.
    /// </summary>
    public KinesisMessagingContextBuilder<TContext> AddConsumer<TMessage, THandler>(Action<KinesisConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        model.RequireProperty(typeof(TMessage), MessagingPropertyKind.Stream);
        KinesisMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }
}
