using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// A messaging context whose properties are streams: the same declarative shape the queue transport
/// has, and the same rule that a context is served by whichever transport owns each property.
/// </summary>
public class StreamContextRegistrationTests
{
    static void ConfigureConnection(KinesisMessagingOptions options)
    {
        options.ServiceUrl = "https://yds.serverless.yandexcloud.net";
        options.Region = "ru-central1";
        options.AccessKeyId = "x";
        options.SecretAccessKey = "x";
    }

    [Fact]
    public void Context_ResolvesStreams()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging<OrderEvents>(options =>
        {
            ConfigureConnection(options);
            options.Streams["order-placed"] = "/ru-central1/b1g/etn/order-placed";
        }, validateOnStart: false);

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<OrderEvents>();

        // Properties are filled and are the same instances DI hands out directly.
        Assert.Same(context.Placed, provider.GetRequiredService<IMessageStream<OrderPlaced>>());
        Assert.Same(context.Placed, context.Stream<OrderPlaced>());
        Assert.Equal(2, context.Streams.Count);
        Assert.Empty(context.Queues);

        // Names: message type [Queue], then the property [Queue] override; the physical name comes
        // from the connection's map.
        Assert.Equal("/ru-central1/b1g/etn/order-placed", context.Placed.Name);
        Assert.Equal("order-shipping", context.Shipped.Name);

        // The routing publisher covers context streams too.
        Assert.Same(context.Placed, provider.GetRequiredService<IMessageSender<OrderPlaced>>());
    }

    [Fact]
    public async Task Context_EnsureQueues_WithoutQueues_IsNoOp()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging<OrderEvents>(ConfigureConnection, validateOnStart: false);

        await using var provider = services.BuildServiceProvider();

        // Streams are not provisioned, and a context without queues has nothing else to create.
        await provider.GetRequiredService<OrderEvents>().EnsureQueuesAsync();
    }

    [Fact]
    public void Context_QueueProperty_IsLeftToTheQueueTransport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging<MixedMessaging>(ConfigureConnection, validateOnStart: false);

        using var provider = services.BuildServiceProvider();

        // The stream half is bound; the queue half is not, and the error says which registration is missing.
        Assert.NotNull(provider.GetRequiredService<IMessageStream<OrderPlaced>>());

        var exception = Assert.Throws<InvalidOperationException>(provider.GetRequiredService<MixedMessaging>);
        Assert.Contains("AddSqsMessaging", exception.Message);
        Assert.Contains(nameof(MixedMessaging.Created), exception.Message);
    }

    [Fact]
    public void Context_HalvesBoundByDifferentTransports_AreBothFilled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging<MixedMessaging>(ConfigureConnection, validateOnStart: false);
        services.AddFakeMessaging().AddQueue<OrderCreated>("orders");

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<MixedMessaging>();

        // Two independent registrations, one context: each transport filled the properties it serves.
        Assert.Same(context.Created, provider.GetRequiredService<IMessageQueue<OrderCreated>>());
        Assert.Same(context.Placed, provider.GetRequiredService<IMessageStream<OrderPlaced>>());
        Assert.Same(context, provider.GetRequiredService<MixedMessaging>());
    }

    [Fact]
    public void Context_AlsoRegisteredByTheApplication_IsStillInitialized()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<OrderEvents>();       // the application registering the type itself
        services.AddKinesisMessaging<OrderEvents>(ConfigureConnection, validateOnStart: false);

        using var provider = services.BuildServiceProvider();

        // Only the transport's registration fills the properties, so it has to be the one that wins.
        Assert.NotNull(provider.GetRequiredService<OrderEvents>().Placed);
    }

    [Fact]
    public void Context_OnANamedConnection_UsesItsOptions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessagingConnection("orders", options =>
        {
            ConfigureConnection(options);
            options.Streams["order-placed"] = "/ru-central1/b1g/etn/dev-order-placed";
        }, validateOnStart: false);
        services.AddKinesisMessaging<OrderEvents>("orders");

        using var provider = services.BuildServiceProvider();

        // The physical name comes from the connection the context is bound to, not from a global map.
        Assert.Equal("/ru-central1/b1g/etn/dev-order-placed", provider.GetRequiredService<OrderEvents>().Placed.Name);
    }

    [Fact]
    public void Contexts_OnDifferentAccounts_ReadEachTheirOwn()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessagingConnection("main", options =>
        {
            ConfigureConnection(options);
            options.Streams["order-placed"] = "/main/order-placed";
        }, validateOnStart: false);
        services.AddKinesisMessagingConnection("archive", options =>
        {
            options.ServiceUrl = "https://kinesis.us-east-1.amazonaws.com";
            options.Region = "us-east-1";
            // Static keys on purpose: a client built without any would send the SDK down its default
            // credential chain, and probing instance metadata takes minutes off an EC2 host.
            options.AccessKeyId = "x";
            options.SecretAccessKey = "x";
            options.Streams["order-archived"] = "/archive/order-archived";
        }, validateOnStart: false);

        services.AddKinesisMessaging<OrderEvents>("main");
        services.AddKinesisMessaging<ArchiveEvents>("archive");

        using var provider = services.BuildServiceProvider();

        Assert.Equal("/main/order-placed", provider.GetRequiredService<OrderEvents>().Placed.Name);
        Assert.Equal("/archive/order-archived", provider.GetRequiredService<ArchiveEvents>().Archived.Name);

        // Different accounts mean different clients; streams of one connection share theirs.
        var streams = provider.GetRequiredService<IKinesisStreamProvider>();
        Assert.NotSame(streams.ClientFor(typeof(OrderPlaced)), streams.ClientFor(typeof(OrderArchived)));
        Assert.Same(streams.ClientFor(typeof(OrderPlaced)), streams.ClientFor(typeof(OrderShipped)));
    }

    [Fact]
    public void Context_MissingConnection_ThrowsWithHint()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging<OrderEvents>("not-registered");

        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<InvalidOperationException>(provider.GetRequiredService<OrderEvents>);
        Assert.Contains("not-registered", exception.Message);
        Assert.Contains("AddKinesisMessagingConnection", exception.Message);
    }

    [Fact]
    public void Context_WithoutStreams_Throws()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(
            () => services.AddKinesisMessaging<QueuesOnlyMessaging>(ConfigureConnection, validateOnStart: false));

        Assert.Contains("IMessageStream", exception.Message);
    }

    [Fact]
    public void Context_BindingAMessageTypeTwice_Throws()
    {
        var services = new ServiceCollection();
        services.AddKinesisMessaging(ConfigureConnection, validateOnStart: false).AddStream<OrderPlaced>();

        Assert.Throws<InvalidOperationException>(
            () => services.AddKinesisMessaging<OrderEvents>(ConfigureConnection, validateOnStart: false));
    }

    [Fact]
    public void AddConsumer_OnAContextStream_RegistersTheReader()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInMemoryMessagingCheckpoints();
        services.AddKinesisMessaging<OrderEvents>(ConfigureConnection, validateOnStart: false)
            .AddConsumer<OrderPlaced, OrderPlacedHandler>(options => options.ConsumerGroup = "billing");

        using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetServices<IHostedService>());
        using var scope = provider.CreateScope();
        Assert.IsType<OrderPlacedHandler>(scope.ServiceProvider.GetRequiredService<IMessageHandler<OrderPlaced>>());
    }

    [Fact]
    public void AddConsumer_ForAMessageOutsideTheContext_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddKinesisMessaging<OrderEvents>(ConfigureConnection, validateOnStart: false);

        var exception = Assert.Throws<InvalidOperationException>(
            () => builder.AddConsumer<OrderCreated, OrderCreatedHandler>());

        Assert.Contains("has no stream", exception.Message);
    }

    [Queue("order-placed")]
    public class OrderPlaced;

    public class OrderShipped;

    public class OrderCreated;

    public class OrderEvents : MessagingContext
    {
        public IMessageStream<OrderPlaced> Placed { get; private set; } = null!;
        [Queue("order-shipping")] public IMessageStream<OrderShipped> Shipped { get; private set; } = null!;
    }

    public class MixedMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
        public IMessageStream<OrderPlaced> Placed { get; private set; } = null!;
    }

    public class OrderArchived;

    public class ArchiveEvents : MessagingContext
    {
        [Queue("order-archived")] public IMessageStream<OrderArchived> Archived { get; private set; } = null!;
    }

    public class QueuesOnlyMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
    }

    public class OrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public class OrderCreatedHandler : IMessageHandler<OrderCreated>
    {
        public Task HandleAsync(MessageContext<OrderCreated> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
