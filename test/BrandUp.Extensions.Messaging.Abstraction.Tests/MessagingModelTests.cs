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
        Assert.Equal("overridden", model.RequireProperty(typeof(AttributedMessage)).LogicalName);
        // Message type attribute.
        Assert.Equal("typed-message", model.RequireProperty(typeof(TypedMessage)).LogicalName);
        // Property name fallback.
        Assert.Equal("Plain", model.RequireProperty(typeof(PlainMessage)).LogicalName);
    }

    [Fact]
    public void Build_WithoutQueues_Throws()
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
    public void Build_StreamProperty_NotSupportedYet()
    {
        Assert.Throws<NotSupportedException>(() => MessagingModel.Build(typeof(StreamContext)));
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

    public class StreamContext : MessagingContext
    {
        public IMessageStream<PlainMessage> Plain { get; private set; } = null!;
    }
}
