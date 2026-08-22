using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BrandUp.Extensions.Messaging;

public class ContextRegistrationTests
{
    static void ConfigureConnection(SqsMessagingOptions options)
    {
        options.ServiceUrl = "http://localhost:9324";
        options.Region = "elasticmq";
        options.AccessKeyId = "x";
        options.SecretAccessKey = "x";
    }

    [Fact]
    public void Context_OwnConnection_ResolvesQueues()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging<OrderMessaging>(ConfigureConnection);

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<OrderMessaging>();

        // Properties are filled and are the same instances DI hands out directly.
        Assert.Same(context.Created, provider.GetRequiredService<IMessageQueue<OrderCreated>>());
        Assert.Same(context.Cancelled, provider.GetRequiredService<IMessageQueue<OrderCancelled>>());
        Assert.Same(context.Created, context.Queue<OrderCreated>());
        Assert.Equal(2, context.Queues.Count);

        // Names: message type [Queue], property [Queue].
        Assert.Equal("order-created", context.Created.Name);
        Assert.Equal("orders-cancelled", context.Cancelled.Name);

        // The routing publisher covers context queues too.
        Assert.NotNull(provider.GetRequiredService<IMessagePublisher>());
    }

    [Fact]
    public void Context_OnNamedConnection_UsesItsOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessagingConnection("orders", options =>
        {
            ConfigureConnection(options);
            options.QueueNamePrefix = "dev-";
        });
        services.AddSqsMessaging<OrderMessaging>("orders");

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<OrderMessaging>();

        Assert.Equal("dev-order-created", context.Created.Name);
    }

    [Fact]
    public void Context_MissingConnection_ThrowsWithHint()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging<OrderMessaging>("not-registered");

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(provider.GetRequiredService<OrderMessaging>);
        Assert.Contains("not-registered", exception.Message);
    }

    [Fact]
    public void ConfigureQueue_AppliesSettings()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging<OrderMessaging>(ConfigureConnection)
            .ConfigureQueue<OrderCreated>(settings => settings.Fifo = true);

        using var provider = services.BuildServiceProvider();

        Assert.Equal("order-created.fifo", provider.GetRequiredService<OrderMessaging>().Created.Name);
    }

    [Fact]
    public void ConfigureQueue_ForeignMessageType_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddSqsMessaging<OrderMessaging>(ConfigureConnection);

        Assert.Throws<InvalidOperationException>(() => builder.ConfigureQueue<ForeignMessage>(_ => { }));
    }

    [Fact]
    public void Context_AddConsumer_RegistersHandlerAndHostedService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging<OrderMessaging>(ConfigureConnection)
            .AddConsumer<OrderCreated, OrderCreatedHandler>();

        using var provider = services.BuildServiceProvider();

        using var scope = provider.CreateScope();
        Assert.IsType<OrderCreatedHandler>(scope.ServiceProvider.GetRequiredService<IMessageHandler<OrderCreated>>());
        Assert.NotEmpty(provider.GetServices<IHostedService>());
    }

    [Fact]
    public void Context_MessageTypeAlreadyBound_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging(ConfigureConnection).AddQueue<OrderCreated>();

        Assert.Throws<InvalidOperationException>(() => services.AddSqsMessaging<OrderMessaging>("orders"));
    }

    [Queue("order-created")]
    public class OrderCreated
    {
        public Guid OrderId { get; set; }
    }

    public class OrderCancelled
    {
        public Guid OrderId { get; set; }
    }

    public class ForeignMessage;

    public class OrderMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
        [Queue("orders-cancelled")] public IMessageQueue<OrderCancelled> Cancelled { get; private set; } = null!;
    }

    public class OrderCreatedHandler : IMessageHandler<OrderCreated>
    {
        public Task HandleAsync(MessageContext<OrderCreated> context, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}
