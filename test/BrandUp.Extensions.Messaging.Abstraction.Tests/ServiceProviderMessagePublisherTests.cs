using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

public class ServiceProviderMessagePublisherTests
{
    [Fact]
    public async Task Publish_RoutesToRegisteredSender()
    {
        var sender = new RecordingSender();
        var publisher = new ServiceProviderMessagePublisher(new SingleServiceProvider(typeof(IMessageSender<TestMessage>), sender));
        var message = new TestMessage();

        var result = await publisher.PublishAsync(message);

        Assert.Same(message, sender.LastMessage);
        Assert.Equal("1", result.MessageId);
    }

    [Fact]
    public async Task Publish_UnknownMessageType_Throws()
    {
        var publisher = new ServiceProviderMessagePublisher(new SingleServiceProvider(typeof(string), "unused"));

        var exception = await Assert.ThrowsAsync<MessagingException>(() => publisher.PublishAsync(new TestMessage()));
        Assert.Contains(nameof(TestMessage), exception.Message);
    }

    class TestMessage;

    class RecordingSender : IMessageSender<TestMessage>
    {
        public TestMessage? LastMessage { get; private set; }

        public Task<PublishResult> PublishAsync(TestMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
        {
            LastMessage = message;
            return Task.FromResult(new PublishResult { MessageId = "1" });
        }
    }

    class SingleServiceProvider(Type serviceType, object instance) : IServiceProvider
    {
        public object? GetService(Type requestedType) => requestedType == serviceType ? instance : null;
    }
}
