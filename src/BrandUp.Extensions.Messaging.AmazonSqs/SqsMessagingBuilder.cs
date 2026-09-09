using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class SqsMessagingBuilder
{
    readonly SqsMessagingRegistry registry;

    internal SqsMessagingBuilder(IServiceCollection services, SqsMessagingRegistry registry)
    {
        Services = services;
        this.registry = registry;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Takes the credentials of this connection from <typeparamref name="TProvider"/> instead of static
    /// keys: the provider is asked to renew them on a timer, and the client picks up what it caches
    /// without being rebuilt. What a provider is for — temporary (STS) credentials, a vault, a token
    /// exchanged for keys — see <see cref="IMessagingCredentialsProvider"/>.
    /// </summary>
    /// <param name="refreshInterval">
    /// How often the provider is asked to renew; one minute by default. The provider renews only what
    /// needs renewing, so this is a heartbeat rather than a rotation period.
    /// </param>
    public SqsMessagingBuilder UseCredentialsProvider<TProvider>(TimeSpan? refreshInterval = null)
        where TProvider : class, IMessagingCredentialsProvider
    {
        CredentialsRegistrations.Add<TProvider>(Services, Options.DefaultName, refreshInterval, nameof(UseCredentialsProvider));

        return this;
    }

    /// <inheritdoc cref="UseCredentialsProvider{TProvider}(TimeSpan?)"/>
    public SqsMessagingBuilder UseCredentialsProvider(
        Func<IServiceProvider, IMessagingCredentialsProvider> factory, TimeSpan? refreshInterval = null)
    {
        CredentialsRegistrations.Add(Services, Options.DefaultName, refreshInterval, factory, nameof(UseCredentialsProvider));

        return this;
    }

    /// <summary>
    /// Binds a message type to a queue. The logical name comes from <paramref name="queueName"/> or the
    /// type's <see cref="QueueAttribute"/>; the physical name is resolved via
    /// <see cref="SqsMessagingOptions"/> (overrides, prefix/suffix, <c>.fifo</c>).
    /// Registers <see cref="IMessageQueue{TMessage}"/> for injection and makes the type publishable
    /// through <see cref="IMessagePublisher"/>. A message type is bound to exactly one destination:
    /// binding it again — here or in another transport — throws.
    /// </summary>
    public SqsMessagingBuilder AddQueue<TMessage>(string? queueName = null, Action<QueueSettings>? configure = null)
        where TMessage : class
    {
        var logicalName = LogicalNames.Resolve(typeof(TMessage), queueName, nameof(queueName), nameof(AddQueue));
        RegistrationGuards.EnsureNotBound<TMessage>(Services, nameof(AddQueue));

        var settings = new QueueSettings();
        configure?.Invoke(settings);
        SqsLimits.ValidateSettings(settings, nameof(configure));

        SqsMessagingServiceCollectionExtensions.AddQueueCore(
            Services, registry, typeof(TMessage), Options.DefaultName, logicalName, settings);

        return this;
    }

    /// <summary>
    /// Registers the hosted consumer of <typeparamref name="TMessage"/>: a background long-polling loop
    /// dispatching to <typeparamref name="THandler"/> in its own DI scope. One consumer per message
    /// type — a second registration throws. The queue must be bound with <see cref="AddQueue{TMessage}"/>
    /// first (or carry a <see cref="QueueAttribute"/> — then it is bound here). To bind
    /// <paramref name="configure"/> from configuration instead, use the named options
    /// <see cref="SqsConsumerOptions.NameFor"/>.
    /// </summary>
    public SqsMessagingBuilder AddConsumer<TMessage, THandler>(Action<SqsConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        if (!registry.Contains(typeof(TMessage)))
            AddQueue<TMessage>();

        SqsMessagingServiceCollectionExtensions.AddConsumerCore<TMessage, THandler>(Services, configure);

        return this;
    }
}
