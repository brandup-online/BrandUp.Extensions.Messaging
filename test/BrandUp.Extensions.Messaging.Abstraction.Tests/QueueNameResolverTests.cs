using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The provider-neutral half of naming: logical name to physical name. Length and character rules are
/// per provider — see the SQS package's SqsQueueNamesTests.
/// </summary>
public class QueueNameResolverTests
{
    [Fact]
    public void Resolve_LogicalNameOnly()
    {
        Assert.Equal("orders", QueueNameResolver.Resolve("orders", null, null, null, fifo: false));
    }

    [Fact]
    public void Resolve_AppliesPrefixAndSuffix()
    {
        Assert.Equal("dev-orders-eu", QueueNameResolver.Resolve("orders", null, "dev-", "-eu", fifo: false));
    }

    [Fact]
    public void Resolve_Override_IsExactPhysicalName_PrefixNotApplied()
    {
        var overrides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["orders"] = "company-shared-orders" };

        Assert.Equal("company-shared-orders", QueueNameResolver.Resolve("orders", overrides, "dev-", "-eu", fifo: false));
    }

    [Fact]
    public void Resolve_Fifo_AppendsSuffix()
    {
        Assert.Equal("orders.fifo", QueueNameResolver.Resolve("orders", null, null, null, fifo: true));
    }

    [Fact]
    public void Resolve_Fifo_KeepsExistingSuffix()
    {
        var overrides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["orders"] = "orders.fifo" };

        Assert.Equal("orders.fifo", QueueNameResolver.Resolve("orders", overrides, null, null, fifo: true));
    }

    [Fact]
    public void Resolve_EmptyLogicalName_Throws()
    {
        Assert.Throws<ArgumentException>(() => QueueNameResolver.Resolve("  ", null, null, null, fifo: false));
    }
}
