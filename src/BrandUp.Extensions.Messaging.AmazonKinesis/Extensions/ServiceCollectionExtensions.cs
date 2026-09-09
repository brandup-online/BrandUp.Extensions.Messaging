using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public static class KinesisMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Kinesis-compatible connection — Amazon Kinesis Data Streams by region, or Yandex
    /// Data Streams by <see cref="KinesisMessagingOptions.ServiceUrl"/>. Bind message types to streams on
    /// the returned builder.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="configure">Connection options.</param>
    /// <param name="validateOnStart">
    /// Validate the connection options eagerly at host start (default). Pass <see langword="false"/> for
    /// optional messaging: validation then happens lazily, on first use.
    /// </param>
    public static KinesisMessagingBuilder AddKinesisMessaging(
        this IServiceCollection services, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = services.AddOptions<KinesisMessagingOptions>().Configure(configure);
        if (validateOnStart)
            builder.ValidateOnStart();

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisMessagingOptions>, KinesisMessagingOptionsValidator>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisConsumerOptions>, KinesisConsumerOptionsValidator>());

        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IKinesisClientFactory, KinesisClientFactory>();
        services.TryAddSingleton<IMessagePublisher, ServiceProviderMessagePublisher>();

        return new KinesisMessagingBuilder(services);
    }

    /// <summary>
    /// Registers a messaging context on a Kinesis-compatible connection configured here: its
    /// <see cref="IMessageStream{TMessage}"/> properties are bound to streams, by the same naming rules
    /// as <see cref="KinesisMessagingBuilder.AddStream{TMessage}"/>. A context that also declares
    /// queues is registered with the queue transport as well — each fills the properties it serves.
    /// </summary>
    /// <inheritdoc cref="AddKinesisMessaging(IServiceCollection, Action{KinesisMessagingOptions}, bool)" path="/param[@name='validateOnStart']"/>
    public static KinesisMessagingContextBuilder<TContext> AddKinesisMessaging<TContext>(
        this IServiceCollection services, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
        where TContext : MessagingContext
        => services.AddKinesisMessaging(configure, validateOnStart).AddContext<TContext>();

    /// <summary>Consumer registration shared by the connection and context builders.</summary>
    internal static void AddConsumerCore<TMessage, THandler>(
        IServiceCollection services, Action<KinesisConsumerOptions>? configure)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegistrationGuards.EnsureNoHandler<TMessage>(services, "AddConsumer");

        services.AddScoped<IMessageHandler<TMessage>, THandler>();
        if (configure is not null)
            services.Configure(KinesisConsumerOptions.NameFor(typeof(TMessage)), configure);

        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp =>
        {
            var checkpoints = sp.GetService<ICheckpointStore>()
                ?? throw new InvalidOperationException(
                    $"Reading stream {typeof(TMessage).Name} needs an {nameof(ICheckpointStore)}: register one " +
                    "(AddMongoMessagingCheckpoints in production, AddInMemoryMessagingCheckpoints in tests) before AddConsumer, " +
                    "or the reader would re-read the stream from the start after every restart.");

            // Optional: without a lease store the reader assumes it is the only one of its group.
            var leases = sp.GetService<IShardLeaseStore>();

            return leases is null
                ? ActivatorUtilities.CreateInstance<KinesisConsumerService<TMessage>>(sp, checkpoints)
                : ActivatorUtilities.CreateInstance<KinesisConsumerService<TMessage>>(sp, checkpoints, leases);
        });
    }
}
