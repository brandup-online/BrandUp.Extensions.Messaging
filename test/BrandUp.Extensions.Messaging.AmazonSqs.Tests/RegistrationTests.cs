using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class RegistrationTests
{
    static IServiceCollection CreateServices(Action<SqsMessagingOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging(configure ?? (options =>
        {
            options.ServiceUrl = "http://localhost:9324";
            options.Region = "elasticmq";
            options.AccessKeyId = "x";
            options.SecretAccessKey = "x";
        }), validateOnStart: false);
        return services;
    }

    [Fact]
    public void AddQueue_RegistersTypedQueueAndPublisher()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services).AddQueue<OrderCreated>();

        using var provider = services.BuildServiceProvider();

        var queue = provider.GetRequiredService<IMessageQueue<OrderCreated>>();
        Assert.Equal("order-created", queue.Name);
        Assert.Same(queue, provider.GetRequiredService<IMessageSender<OrderCreated>>());
        Assert.NotNull(provider.GetRequiredService<IMessagePublisher>());
    }

    [Fact]
    public void AddQueue_WithoutNameOrAttribute_Throws()
    {
        var services = CreateServices();

        Assert.Throws<ArgumentException>(() => new SqsMessagingBuilder_Accessor(services).AddQueue<PlainMessage>());
    }

    [Fact]
    public void AddQueue_SameTypeTwice_Throws()
    {
        var services = CreateServices();
        var builder = new SqsMessagingBuilder_Accessor(services);
        builder.AddQueue<OrderCreated>();

        Assert.Throws<InvalidOperationException>(() => builder.AddQueue<OrderCreated>("other"));
    }

    [Fact]
    public void AddQueue_Fifo_NameGetsSuffix()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services).AddQueue<OrderCreated>(configure: settings => settings.Fifo = true);

        using var provider = services.BuildServiceProvider();

        Assert.Equal("order-created.fifo", provider.GetRequiredService<IMessageQueue<OrderCreated>>().Name);
    }

    [Fact]
    public void QueueName_OverrideIsExact_PrefixAppliesToOthersOnly()
    {
        var services = CreateServices(options =>
        {
            options.ServiceUrl = "http://localhost:9324";
            options.AccessKeyId = "x";
            options.SecretAccessKey = "x";
            options.QueueNamePrefix = "dev-";
            options.Queues["order-created"] = "orders";
        });
        new SqsMessagingBuilder_Accessor(services).AddQueue<OrderCreated>();

        using var provider = services.BuildServiceProvider();

        // The override is the exact physical name; the environment prefix is not applied to it.
        Assert.Equal("orders", provider.GetRequiredService<IMessageQueue<OrderCreated>>().Name);
    }

    [Theory]
    [InlineData("MaxReceiveCount zero")]
    [InlineData("ContentBasedDeduplication without Fifo")]
    [InlineData("sub-second VisibilityTimeout")]
    [InlineData("VisibilityTimeout over 12 hours")]
    [InlineData("DeliveryDelay over 15 minutes")]
    [InlineData("DeadLetterQueueName without MaxReceiveCount")]
    public void AddQueue_InvalidSettings_ThrowAtRegistration(string scenario)
    {
        var services = CreateServices();
        var builder = new SqsMessagingBuilder_Accessor(services);

        Action<QueueSettings> configure = scenario switch
        {
            "MaxReceiveCount zero" => s => s.MaxReceiveCount = 0,
            "ContentBasedDeduplication without Fifo" => s => s.ContentBasedDeduplication = true,
            "sub-second VisibilityTimeout" => s => s.VisibilityTimeout = TimeSpan.FromMilliseconds(500),
            "VisibilityTimeout over 12 hours" => s => s.VisibilityTimeout = TimeSpan.FromHours(13),
            "DeliveryDelay over 15 minutes" => s => s.DeliveryDelay = TimeSpan.FromMinutes(20),
            _ => s => s.DeadLetterQueueName = "orphan-dlq",
        };

        Assert.Throws<ArgumentException>(() => builder.AddQueue<OrderCreated>(configure: configure));
    }

    [Fact]
    public void AddQueue_ValidSettings_Pass()
    {
        var services = CreateServices();

        new SqsMessagingBuilder_Accessor(services).AddQueue<OrderCreated>(configure: settings =>
        {
            settings.Fifo = true;
            settings.ContentBasedDeduplication = true;
            settings.VisibilityTimeout = TimeSpan.FromSeconds(45);
            settings.MessageRetention = TimeSpan.FromDays(4);
            settings.MaxReceiveCount = 5;
            settings.DeadLetterQueueName = "orders-dead";
        });

        using var provider = services.BuildServiceProvider();
        Assert.Equal("order-created.fifo", provider.GetRequiredService<IMessageQueue<OrderCreated>>().Name);
    }

    [Fact]
    public void AddConsumer_SameTypeTwice_Throws()
    {
        var services = CreateServices();
        var builder = new SqsMessagingBuilder_Accessor(services);
        builder.AddQueue<OrderCreated>().AddConsumer<OrderCreated, OrderCreatedHandler>();

        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<OrderCreated, OrderCreatedHandler>());
    }

    [Fact]
    public void ConsumerOptions_NoWaitAnywhere_FailValidation()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services)
            .AddQueue<OrderCreated>()
            .AddConsumer<OrderCreated, OrderCreatedHandler>(options =>
            {
                // Neither server-side long polling nor a client-side pause: a tight billed poll loop.
                options.WaitTime = TimeSpan.Zero;
                options.EmptyReadDelay = TimeSpan.Zero;
            });

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<SqsConsumerOptions>>();

        Assert.Throws<OptionsValidationException>(() => monitor.Get(SqsConsumerOptions.NameFor(typeof(OrderCreated))));
    }

    [Fact]
    public void ConsumerOptions_ZeroWaitWithEmptyReadDelay_IsValid()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services)
            .AddQueue<OrderCreated>()
            .AddConsumer<OrderCreated, OrderCreatedHandler>(options =>
            {
                options.WaitTime = TimeSpan.Zero;
                options.EmptyReadDelay = TimeSpan.FromSeconds(1);
            });

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<SqsConsumerOptions>>();

        Assert.Equal(TimeSpan.FromSeconds(1), monitor.Get(SqsConsumerOptions.NameFor(typeof(OrderCreated))).EmptyReadDelay);
    }

    [Fact]
    public void ConsumerOptions_OutOfRange_FailValidationOnRead()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services)
            .AddQueue<OrderCreated>()
            .AddConsumer<OrderCreated, OrderCreatedHandler>(options => options.BatchSize = 20);

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<SqsConsumerOptions>>();

        // The hosted consumer reads the options with Get at startup, so this is what fails the host.
        Assert.Throws<OptionsValidationException>(() => monitor.Get(SqsConsumerOptions.NameFor(typeof(OrderCreated))));
    }

    [Fact]
    public async Task Publish_UnregisteredType_Throws()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services).AddQueue<OrderCreated>();

        using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        await Assert.ThrowsAsync<MessagingException>(() => publisher.PublishAsync(new PlainMessage()));
    }

    [Fact]
    public void AddConsumer_RegistersHandlerAndHostedService()
    {
        var services = CreateServices();
        new SqsMessagingBuilder_Accessor(services)
            .AddQueue<OrderCreated>()
            .AddConsumer<OrderCreated, OrderCreatedHandler>();

        using var provider = services.BuildServiceProvider();

        using var scope = provider.CreateScope();
        Assert.IsType<OrderCreatedHandler>(scope.ServiceProvider.GetRequiredService<IMessageHandler<OrderCreated>>());
        Assert.NotEmpty(provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>());
    }

    [Theory]
    [InlineData(null, null)] // no endpoint at all
    [InlineData("http://localhost:9324", "key-without-secret")] // half-set static keys
    public void Options_Validation(string? serviceUrl, string? accessKeyId)
    {
        var services = CreateServices(options =>
        {
            options.ServiceUrl = serviceUrl;
            options.AccessKeyId = accessKeyId;
        });

        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<SqsMessagingOptions>>().Value);
    }

    [Fact]
    public void Options_NoStaticKeys_IsValid_DefaultCredentialChain()
    {
        var services = CreateServices(options => options.ServiceUrl = "http://localhost:9324");

        using var provider = services.BuildServiceProvider();

        // Both keys omitted -> the SDK default credential chain (IAM role, environment, profile).
        Assert.NotNull(provider.GetRequiredService<IOptions<SqsMessagingOptions>>().Value);
    }

    // AddSqsMessaging returns the builder, but tests register queues in steps — this re-obtains a builder
    // over the same service collection, verifying the registry is shared between builder instances.
    class SqsMessagingBuilder_Accessor
    {
        readonly SqsMessagingBuilder builder;

        public SqsMessagingBuilder_Accessor(IServiceCollection services)
        {
            builder = services.AddSqsMessaging(_ => { }, validateOnStart: false);
        }

        public SqsMessagingBuilder_Accessor AddQueue<TMessage>(string? queueName = null, Action<QueueSettings>? configure = null)
            where TMessage : class
        {
            builder.AddQueue<TMessage>(queueName, configure);
            return this;
        }

        public SqsMessagingBuilder_Accessor AddConsumer<TMessage, THandler>(Action<SqsConsumerOptions>? configure = null)
            where TMessage : class
            where THandler : class, IMessageHandler<TMessage>
        {
            builder.AddConsumer<TMessage, THandler>(configure);
            return this;
        }
    }

    [Queue("order-created")]
    public class OrderCreated
    {
        public Guid OrderId { get; set; }
    }

    public class PlainMessage;

    public class OrderCreatedHandler : IMessageHandler<OrderCreated>
    {
        public Task HandleAsync(MessageContext<OrderCreated> context, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
