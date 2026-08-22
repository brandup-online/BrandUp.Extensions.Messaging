namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Logical name → physical name, the provider-neutral half of naming. An override from the connection
/// options is the exact physical name (the environment prefix/suffix is not applied to it — that is
/// what overrides are for); a name without an override gets the prefix/suffix. A FIFO name gets the
/// <c>.fifo</c> suffix when it does not already carry one.
/// <para>
/// Length and character rules differ per provider (SQS allows 80 characters of
/// <c>[A-Za-z0-9_-]</c>, other brokers are far more permissive), so validation belongs to the
/// provider — see <c>SqsQueueNames.Validate</c>.
/// </para>
/// </summary>
internal static class QueueNameResolver
{
    public const string FifoSuffix = ".fifo";

    public static string Resolve(
        string logicalName,
        IReadOnlyDictionary<string, string>? overrides,
        string? namePrefix,
        string? nameSuffix,
        bool fifo)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalName);

        var name = overrides is not null && overrides.TryGetValue(logicalName, out var overridden)
            ? overridden
            : $"{namePrefix}{logicalName}{nameSuffix}";

        if (fifo && !name.EndsWith(FifoSuffix, StringComparison.Ordinal))
            name += FifoSuffix;

        return name;
    }
}
