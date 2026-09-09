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

    [Fact]
    public async Task PublishBatch_GoesToTheSenderOfTheType()
    {
        var sender = new RecordingSender();
        var publisher = new ServiceProviderMessagePublisher(
            new SingleServiceProvider(typeof(IMessageSender<TestMessage>), sender));

        var results = await publisher.PublishAsync([new TestMessage(), new TestMessage()]);

        Assert.Equal(2, results.Count);
        Assert.Equal(2, sender.LastBatch?.Count);
    }

    [Fact]
    public async Task PublishBatch_WithoutASender_ThrowsWithHint()
    {
        var publisher = new ServiceProviderMessagePublisher(new SingleServiceProvider(typeof(string), "x"));

        var exception = await Assert.ThrowsAsync<MessagingException>(
            () => publisher.PublishAsync([new TestMessage()]));

        Assert.Contains(nameof(TestMessage), exception.Message);
    }

    class TestMessage;

    class RecordingSender : IMessageSender<TestMessage>
    {
        public TestMessage? LastMessage { get; private set; }

        public IReadOnlyCollection<PublishMessage<TestMessage>>? LastBatch { get; private set; }

        public Task<PublishResult> PublishAsync(TestMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
        {
            LastMessage = message;
            return Task.FromResult(new PublishResult { MessageId = "1" });
        }

        public Task<IReadOnlyList<PublishResult>> PublishAsync(
            IReadOnlyCollection<PublishMessage<TestMessage>> messages, CancellationToken cancellationToken = default)
        {
            LastBatch = messages;
            return Task.FromResult<IReadOnlyList<PublishResult>>(
                [.. messages.Select((_, index) => new PublishResult { MessageId = index.ToString() })]);
        }
    }

    class SingleServiceProvider(Type serviceType, object instance) : IServiceProvider
    {
        public object? GetService(Type requestedType) => requestedType == serviceType ? instance : null;
    }
}
