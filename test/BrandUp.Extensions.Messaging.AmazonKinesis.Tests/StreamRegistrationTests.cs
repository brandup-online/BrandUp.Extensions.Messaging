using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class StreamRegistrationTests
{
    static IServiceCollection CreateServices(Action<KinesisMessagingOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging(configure ?? (options =>
        {
            options.ServiceUrl = "https://yds.serverless.yandexcloud.net";
            options.Region = "ru-central1";
            options.AccessKeyId = "x";
            options.SecretAccessKey = "x";
        }), validateOnStart: false);
        return services;
    }

    [Fact]
    public void AddStream_RegistersTypedStreamAndPublisher()
    {
        var services = CreateServices();
        services.AddKinesisMessaging(_ => { }, validateOnStart: false).AddStream<OrderEvent>();

        using var provider = services.BuildServiceProvider();

        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();
        Assert.Equal("order-events", stream.Name);
        Assert.Same(stream, provider.GetRequiredService<IMessageSender<OrderEvent>>());
        Assert.NotNull(provider.GetRequiredService<IMessagePublisher>());
    }

    [Fact]
    public void StreamName_UsesPhysicalNameOverride()
    {
        var services = CreateServices(options =>
        {
            options.ServiceUrl = "https://yds.serverless.yandexcloud.net";
            options.AccessKeyId = "x";
            options.SecretAccessKey = "x";
            options.Streams["order-events"] = "/ru-central1/b1g/etn/order-events";
        });
        services.AddKinesisMessaging(_ => { }, validateOnStart: false).AddStream<OrderEvent>();

        using var provider = services.BuildServiceProvider();

        Assert.Equal("/ru-central1/b1g/etn/order-events", provider.GetRequiredService<IMessageStream<OrderEvent>>().Name);
    }

    [Fact]
    public void AddStream_SameTypeTwice_Throws()
    {
        var services = CreateServices();
        var builder = services.AddKinesisMessaging(_ => { }, validateOnStart: false);
        builder.AddStream<OrderEvent>();

        Assert.Throws<InvalidOperationException>(() => builder.AddStream<OrderEvent>("other"));
    }

    [Fact]
    public async Task Publish_RejectsOptionsStreamsCannotHonour()
    {
        var services = CreateServices();
        services.AddKinesisMessaging(_ => { }, validateOnStart: false).AddStream<OrderEvent>();

        await using var provider = services.BuildServiceProvider();
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        // Silently dropping these would make a message type behave differently on a stream than on a queue.
        await Assert.ThrowsAsync<ArgumentException>(
            () => stream.PublishAsync(new OrderEvent(), new PublishOptions { Delay = TimeSpan.FromMinutes(5) }));
        await Assert.ThrowsAsync<ArgumentException>(
            () => stream.PublishAsync(new OrderEvent(), new PublishOptions { DeduplicationId = "d1" }));
    }

    [Fact]
    public void AddConsumer_WithoutCheckpointStore_ThrowsWithHint()
    {
        var services = CreateServices();
        services.AddKinesisMessaging(_ => { }, validateOnStart: false)
            .AddStream<OrderEvent>()
            .AddConsumer<OrderEvent, OrderEventHandler>();

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetServices<IHostedService>().ToList());
        Assert.Contains(nameof(ICheckpointStore), exception.Message);
    }

    [Fact]
    public void AddConsumer_WithCheckpointStore_RegistersReader()
    {
        var services = CreateServices();
        services.AddInMemoryCheckpoints();
        services.AddKinesisMessaging(_ => { }, validateOnStart: false)
            .AddStream<OrderEvent>()
            .AddConsumer<OrderEvent, OrderEventHandler>(options => options.ConsumerGroup = "billing");

        using var provider = services.BuildServiceProvider();

        Assert.NotEmpty(provider.GetServices<IHostedService>());
        using var scope = provider.CreateScope();
        Assert.IsType<OrderEventHandler>(scope.ServiceProvider.GetRequiredService<IMessageHandler<OrderEvent>>());
    }

    [Fact]
    public void AddConsumer_SameTypeTwice_Throws()
    {
        var services = CreateServices();
        services.AddInMemoryCheckpoints();
        var builder = services.AddKinesisMessaging(_ => { }, validateOnStart: false)
            .AddStream<OrderEvent>()
            .AddConsumer<OrderEvent, OrderEventHandler>();

        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<OrderEvent, OrderEventHandler>());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(20_000)]
    public void ConsumerOptions_InvalidBatchSize_FailValidation(int batchSize)
    {
        var services = CreateServices();
        services.AddInMemoryCheckpoints();
        services.AddKinesisMessaging(_ => { }, validateOnStart: false)
            .AddStream<OrderEvent>()
            .AddConsumer<OrderEvent, OrderEventHandler>(options => options.BatchSize = batchSize);

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<KinesisConsumerOptions>>();

        Assert.Throws<OptionsValidationException>(() => monitor.Get(KinesisConsumerOptions.NameFor(typeof(OrderEvent))));
    }

    [Fact]
    public void AddInMemoryCheckpoints_RepeatedCalls_ShareOneStore()
    {
        var services = new ServiceCollection();
        services.AddInMemoryCheckpoints();
        services.AddInMemoryCheckpoints();

        using var provider = services.BuildServiceProvider();

        // What a test inspects must be what the reader writes to.
        Assert.Same(provider.GetRequiredService<InMemoryCheckpointStore>(), provider.GetRequiredService<ICheckpointStore>());
    }

    [Fact]
    public void Options_NoStaticKeys_IsValid_DefaultCredentialChain()
    {
        var services = CreateServices(options => options.Region = "ru-central1");

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IOptions<KinesisMessagingOptions>>().Value);
    }

    [Queue("order-events")]
    public class OrderEvent
    {
        public Guid OrderId { get; set; }
    }

    public class OrderEventHandler : IMessageHandler<OrderEvent>
    {
        public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
