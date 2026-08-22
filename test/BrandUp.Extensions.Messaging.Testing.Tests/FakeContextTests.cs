using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

public class FakeContextTests
{
    static ServiceProvider CreateProvider(out InMemoryMessageBus bus)
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging<OrderMessaging>();

        var provider = services.BuildServiceProvider();
        bus = provider.GetRequiredService<InMemoryMessageBus>();
        return provider;
    }

    [Fact]
    public async Task Context_PublishThroughProperty_IsCaptured()
    {
        await using var provider = CreateProvider(out var bus);
        var context = provider.GetRequiredService<OrderMessaging>();

        Assert.Same(context.Created, provider.GetRequiredService<IMessageQueue<OrderCreated>>());
        Assert.Equal("order-created", context.Created.Name);

        await context.Created.PublishAsync(new OrderCreated { OrderId = Guid.NewGuid() });

        Assert.Single(bus.PublishedOf<OrderCreated>());
        Assert.Single(await context.Created.ReceiveAsync());
    }

    [Fact]
    public async Task Context_PublisherFacade_RoutesToContextQueue()
    {
        await using var provider = CreateProvider(out var bus);

        await provider.GetRequiredService<IMessagePublisher>().PublishAsync(new OrderCancelled());

        Assert.Equal("orders-cancelled", Assert.Single(bus.Published).QueueName);
    }

    [Fact]
    public async Task Context_EnsureQueues_IsNoOp()
    {
        await using var provider = CreateProvider(out _);

        // Fake queues always exist; the call must simply succeed.
        await provider.GetRequiredService<OrderMessaging>().EnsureQueuesAsync();
    }

    [Queue("order-created")]
    public class OrderCreated
    {
        public Guid OrderId { get; set; }
    }

    public class OrderCancelled;

    public class OrderMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
        [Queue("orders-cancelled")] public IMessageQueue<OrderCancelled> Cancelled { get; private set; } = null!;
    }
}
