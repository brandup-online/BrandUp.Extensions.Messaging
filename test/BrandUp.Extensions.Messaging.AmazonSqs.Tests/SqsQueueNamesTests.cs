using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

public class SqsQueueNamesTests
{
    [Theory]
    [InlineData("orders queue")]
    [InlineData("orders.queue")]
    [InlineData("заказы")]
    public void Validate_InvalidCharacters_Throws(string name)
    {
        Assert.Throws<MessagingException>(() => SqsQueueNames.Validate(name));
    }

    [Fact]
    public void Validate_TooLong_Throws()
    {
        Assert.Throws<MessagingException>(() => SqsQueueNames.Validate(new string('a', 81)));
    }

    [Fact]
    public void Validate_MaxLength_Passes()
    {
        SqsQueueNames.Validate(new string('a', 80));
    }

    [Fact]
    public void Validate_FifoSuffix_AcceptedRegardlessOfBinding()
    {
        // An override may point at an existing FIFO queue while this binding declares a standard one.
        SqsQueueNames.Validate("team-orders.fifo");
    }

    [Fact]
    public void Resolve_FifoOverride_OnNonFifoBinding_Passes()
    {
        var options = new SqsMessagingOptions { QueueNamePrefix = "dev-" };
        options.Queues["orders"] = "team-orders.fifo";

        Assert.Equal("team-orders.fifo", SqsQueueNames.Resolve("orders", fifo: false, options));
    }

    [Fact]
    public void Resolve_AppliesPrefixToNonOverriddenName()
    {
        var options = new SqsMessagingOptions { QueueNamePrefix = "dev-" };

        Assert.Equal("dev-orders", SqsQueueNames.Resolve("orders", fifo: false, options));
    }
}
