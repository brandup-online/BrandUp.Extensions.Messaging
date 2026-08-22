using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BrandUp.Extensions.Messaging.Integration;

[Trait("Category", "Integration")]
public class SqsConsumerTests
{
    [SqsFact]
    public async Task HostedConsumer_HandlesPublishedMessages()
    {
        var handled = new Handled();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddSqsMessaging(options =>
        {
            SqsEnvironment.Apply(options);
            options.QueueNameSuffix = "-" + Guid.NewGuid().ToString("N")[..8];
        })
        .AddQueue<TestEvent>("test-events")
        .AddConsumer<TestEvent, TestEventHandler>(options => options.WaitTime = TimeSpan.FromSeconds(1));

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            var publisher = host.Services.GetRequiredService<IMessagePublisher>();
            var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
            foreach (var id in ids)
                await publisher.PublishAsync(new TestEvent { Id = id }, cancellationToken: TestContext.Current.CancellationToken);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (handled.Ids.Count < ids.Length)
                await Task.Delay(100, timeout.Token);

            Assert.Equivalent(ids.ToHashSet(), handled.Ids.ToHashSet());
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    public class TestEvent
    {
        public Guid Id { get; set; }
    }

    public class Handled
    {
        public ConcurrentBag<Guid> Ids { get; } = [];
    }

    public class TestEventHandler(Handled handled) : IMessageHandler<TestEvent>
    {
        public Task HandleAsync(MessageContext<TestEvent> context, CancellationToken cancellationToken)
        {
            handled.Ids.Add(context.Message.Id);
            return Task.CompletedTask;
        }
    }
}
