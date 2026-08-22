using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class KinesisMessagingBuilder
{
    internal KinesisMessagingBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public IServiceCollection Services { get; }

    /// <summary>
    /// Binds a message type to a stream. The logical name comes from <paramref name="streamName"/> or
    /// the type's <see cref="QueueAttribute"/>; the physical name (for Yandex Data Streams — the full
    /// stream path) is resolved via <see cref="KinesisMessagingOptions.Streams"/>. Registers
    /// <see cref="IMessageStream{TMessage}"/> and makes the type publishable through
    /// <see cref="IMessagePublisher"/>. A message type is bound to exactly one destination:
    /// binding it again — here or in another transport — throws.
    /// </summary>
    public KinesisMessagingBuilder AddStream<TMessage>(string? streamName = null)
        where TMessage : class
    {
        var logicalName = LogicalNames.Resolve(typeof(TMessage), streamName, nameof(streamName), nameof(AddStream));
        RegistrationGuards.EnsureNotBound<TMessage>(Services, nameof(AddStream));

        Services.AddSingleton<IMessageStream<TMessage>>(sp => new KinesisMessageStream<TMessage>(
            sp.GetRequiredService<IKinesisClientFactory>(),
            sp.GetRequiredService<IMessageSerializer>(),
            sp.GetRequiredService<IOptions<KinesisMessagingOptions>>(),
            logicalName));
        Services.AddSingleton<IMessageSender<TMessage>>(sp => sp.GetRequiredService<IMessageStream<TMessage>>());

        return this;
    }

    /// <summary>
    /// Registers the hosted reader of <typeparamref name="TMessage"/>: it reads every shard of the
    /// stream, hands records to <typeparamref name="THandler"/> in their own DI scope and stores its
    /// position in the <see cref="ICheckpointStore"/> after each handled record.
    /// <para>
    /// A checkpoint store must be registered — <c>AddMongoCheckpoints(…)</c> in production, or
    /// <c>AddInMemoryCheckpoints()</c> from the Testing package. Without one the reader would restart
    /// from the beginning of the stream on every deploy, so registration fails instead.
    /// </para>
    /// <para>
    /// One reader per shard is assumed: run one instance per consumer group, or give each instance its
    /// own <see cref="KinesisConsumerOptions.ConsumerGroup"/> if they should each see every record.
    /// </para>
    /// </summary>
    public KinesisMessagingBuilder AddConsumer<TMessage, THandler>(Action<KinesisConsumerOptions>? configure = null)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        RegistrationGuards.EnsureNoHandler<TMessage>(Services, nameof(AddConsumer));

        if (!Services.Any(d => d.ServiceType == typeof(IMessageStream<TMessage>)))
            AddStream<TMessage>();

        Services.AddScoped<IMessageHandler<TMessage>, THandler>();
        if (configure is not null)
            Services.Configure(KinesisConsumerOptions.NameFor(typeof(TMessage)), configure);

        Services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp =>
        {
            var checkpoints = sp.GetService<ICheckpointStore>()
                ?? throw new InvalidOperationException(
                    $"Reading stream {typeof(TMessage).Name} needs an {nameof(ICheckpointStore)}: register one " +
                    "(AddMongoCheckpoints in production, AddInMemoryCheckpoints in tests) before AddConsumer, " +
                    "or the reader would re-read the stream from the start after every restart.");

            return ActivatorUtilities.CreateInstance<KinesisConsumerService<TMessage>>(sp, checkpoints);
        });

        return this;
    }
}
