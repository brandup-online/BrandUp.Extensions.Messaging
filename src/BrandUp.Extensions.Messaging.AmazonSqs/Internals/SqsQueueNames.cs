namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>SQS naming rules: 80 characters of letters, digits, <c>-</c> and <c>_</c>, plus the <c>.fifo</c> suffix.</summary>
internal static class SqsQueueNames
{
    // The SQS limit; the .fifo suffix counts toward it.
    const int MaxNameLength = 80;

    public static string Resolve(string logicalName, bool fifo, SqsMessagingOptions options)
    {
        var name = QueueNameResolver.Resolve(
            logicalName, options.Queues, options.QueueNamePrefix, options.QueueNameSuffix, fifo);

        Validate(name);

        return name;
    }

    public static void Validate(string name)
    {
        if (name.Length > MaxNameLength)
            throw new MessagingException($"Queue name '{name}' is longer than {MaxNameLength} characters.");

        // The .fifo suffix is part of a valid name whether or not this binding declares Fifo: an
        // override may point at an existing FIFO queue owned by someone else.
        var baseName = name.EndsWith(QueueNameResolver.FifoSuffix, StringComparison.Ordinal)
            ? name[..^QueueNameResolver.FifoSuffix.Length]
            : name;

        if (baseName.Length == 0)
            throw new MessagingException($"Queue name '{name}' is empty.");

        foreach (var c in baseName)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '-' && c != '_')
                throw new MessagingException(
                    $"Queue name '{name}' is invalid: only letters, digits, '-' and '_' are allowed (plus the '.fifo' suffix).");
        }
    }
}
