using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

public class MessagingModelTests
{
    [Fact]
    public void Build_ResolvesLogicalNames_ByPrecedence()
    {
        var model = MessagingModel.Build(typeof(SampleContext));

        Assert.Equal(3, model.Properties.Count);
        // Property attribute wins over the message type's one.
        Assert.Equal("overridden", model.RequireProperty(typeof(AttributedMessage), MessagingPropertyKind.Queue).LogicalName);
        // Message type attribute.
        Assert.Equal("typed-message", model.RequireProperty(typeof(TypedMessage), MessagingPropertyKind.Queue).LogicalName);
        // Property name fallback.
        Assert.Equal("Plain", model.RequireProperty(typeof(PlainMessage), MessagingPropertyKind.Queue).LogicalName);
    }

    [Fact]
    public void Build_WithoutDestinations_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => MessagingModel.Build(typeof(EmptyContext)));
    }

    [Fact]
    public void Build_DuplicateMessageType_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => MessagingModel.Build(typeof(DuplicateContext)));
    }

    [Fact]
    public void Build_PropertyWithoutSetter_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => MessagingModel.Build(typeof(NoSetterContext)));
    }

    [Fact]
    public void Build_SplitsQueuesAndStreams()
    {
        var model = MessagingModel.Build(typeof(MixedContext));

        // Each transport binds its own half, so the split is what the registrations work from.
        Assert.Equal("Plain", Assert.Single(model.Queues).LogicalName);
        Assert.Equal("typed-message", Assert.Single(model.Streams).LogicalName);
    }

    [Fact]
    public void RequireProperty_OfTheOtherKind_Throws()
    {
        var model = MessagingModel.Build(typeof(MixedContext));

        // A stream of the context is not a queue of it: configuring it as one is a registration error.
        var exception = Assert.Throws<InvalidOperationException>(
            () => model.RequireProperty(typeof(TypedMessage), MessagingPropertyKind.Queue));
        Assert.Contains("has no queue", exception.Message);

        Assert.NotNull(model.RequireProperty(typeof(TypedMessage), MessagingPropertyKind.Stream));
    }

    [Fact]
    public void Build_MessageTypeAsQueueAndStream_Throws()
    {
        // One message type, one destination — mapping it twice is ambiguous even across kinds.
        Assert.Throws<InvalidOperationException>(() => MessagingModel.Build(typeof(QueueAndStreamContext)));
    }

    [Queue("typed-message")]
    public class TypedMessage;

    [Queue("attributed")]
    public class AttributedMessage;

    public class PlainMessage;

    public class SampleContext : MessagingContext
    {
        [Queue("overridden")] public IMessageQueue<AttributedMessage> Attributed { get; private set; } = null!;
        public IMessageQueue<TypedMessage> Typed { get; private set; } = null!;
        public IMessageQueue<PlainMessage> Plain { get; private set; } = null!;
    }

    public class EmptyContext : MessagingContext;

    public class DuplicateContext : MessagingContext
    {
        public IMessageQueue<PlainMessage> First { get; private set; } = null!;
        public IMessageQueue<PlainMessage> Second { get; private set; } = null!;
    }

    public class NoSetterContext : MessagingContext
    {
        public IMessageQueue<PlainMessage> Plain => null!;
    }

    public class MixedContext : MessagingContext
    {
        public IMessageQueue<PlainMessage> Plain { get; private set; } = null!;
        public IMessageStream<TypedMessage> Typed { get; private set; } = null!;
    }

    public class QueueAndStreamContext : MessagingContext
    {
        public IMessageQueue<PlainMessage> Queue { get; private set; } = null!;
        public IMessageStream<PlainMessage> Stream { get; private set; } = null!;
    }
}
