// Linked as a shared source file into the SQS package and the Testing package: the testing fake
// promises that "a loop that passes here does not crash in production", which only holds while both
// enforce one set of limits from one place.
namespace BrandUp.Extensions.Messaging.Internals;

internal static class SqsLimits
{
    /// <summary>Messages per receive, per delete batch and per publish batch.</summary>
    public const int MaxBatch = 10;

    /// <summary>
    /// Payload a single request may carry — 256 KiB, for one message and for a whole batch alike. Used
    /// to cut a publish batch before SQS rejects it.
    /// </summary>
    public const int MaxBatchBytes = 262_144;

    public static readonly TimeSpan MaxWaitTime = TimeSpan.FromSeconds(20);
    public static readonly TimeSpan MaxPublishDelay = TimeSpan.FromMinutes(15);

    /// <summary>The visibility-timeout ceiling, and with it the ceiling of an abandon delay.</summary>
    public static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromHours(12);

    public static readonly TimeSpan MaxMessageRetention = TimeSpan.FromDays(14);
    public static readonly TimeSpan MinMessageRetention = TimeSpan.FromMinutes(1);

    public static void ValidateReceive(int maxMessages, TimeSpan? waitTime)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessages, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxMessages, MaxBatch);

        if (waitTime > MaxWaitTime || waitTime < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(waitTime), waitTime, $"Long polling is between 0 and {MaxWaitTime.TotalSeconds} seconds.");
    }

    /// <summary>
    /// Publish-side limits: the reserved type attribute must stay ours, delays are capped, and FIFO
    /// queues take no per-message delay. Enforced by the real queue and mirrored by the fake.
    /// </summary>
    public static void ValidatePublish(PublishOptions? options, bool fifo, string parameterName)
    {
        if (options is null)
            return;

        if (options.Attributes.ContainsKey(MessagingConstants.TypeAttributeName))
            throw new ArgumentException(
                $"Attribute '{MessagingConstants.TypeAttributeName}' is reserved for the message type name and cannot be set by the publisher.",
                parameterName);

        if (options.Delay is TimeSpan delay)
        {
            if (fifo)
                throw new ArgumentException(
                    "FIFO queues do not support per-message delays; use QueueSettings.DeliveryDelay of the queue instead.",
                    parameterName);

            EnsureInRange(delay, MaxPublishDelay, parameterName);
        }
    }

    /// <summary>
    /// Queue settings are checked at registration, so a bad value fails host startup instead of the
    /// first auto-creation in production. Sub-second durations are rejected outright: SQS takes whole
    /// seconds, and silently truncating a 500ms visibility timeout to 0 would redeliver every message
    /// immediately.
    /// </summary>
    public static void ValidateSettings(QueueSettings settings, string parameterName)
    {
        if (settings.MaxReceiveCount is int maxReceiveCount && maxReceiveCount < 1)
            throw new ArgumentException(
                $"{nameof(QueueSettings.MaxReceiveCount)} must be at least 1; leave it unset to disable dead-lettering.",
                parameterName);

        if (settings.ContentBasedDeduplication && !settings.Fifo)
            throw new ArgumentException(
                $"{nameof(QueueSettings.ContentBasedDeduplication)} applies to FIFO queues only; set {nameof(QueueSettings.Fifo)} as well.",
                parameterName);

        ValidateDuration(settings.VisibilityTimeout, TimeSpan.Zero, MaxVisibilityTimeout, nameof(QueueSettings.VisibilityTimeout), parameterName);
        ValidateDuration(settings.DeliveryDelay, TimeSpan.Zero, MaxPublishDelay, nameof(QueueSettings.DeliveryDelay), parameterName);
        ValidateDuration(settings.MessageRetention, MinMessageRetention, MaxMessageRetention, nameof(QueueSettings.MessageRetention), parameterName);

        if (settings.DeadLetterQueueName is not null && settings.MaxReceiveCount is null)
            throw new ArgumentException(
                $"{nameof(QueueSettings.DeadLetterQueueName)} has no effect without {nameof(QueueSettings.MaxReceiveCount)}, which turns dead-lettering on.",
                parameterName);
    }

    static void ValidateDuration(TimeSpan? value, TimeSpan min, TimeSpan max, string settingName, string parameterName)
    {
        if (value is not TimeSpan duration)
            return;

        if (duration < min || duration > max)
            throw new ArgumentException($"{settingName} must be between {min} and {max}.", parameterName);

        if (duration.Ticks % TimeSpan.TicksPerSecond != 0)
            throw new ArgumentException($"{settingName} must be a whole number of seconds.", parameterName);
    }

    public static void EnsureInRange(TimeSpan value, TimeSpan max, string parameterName)
    {
        if (value < TimeSpan.Zero || value > max)
            throw new ArgumentOutOfRangeException(parameterName, value, $"Must be between 0 and {max}.");
    }

    /// <summary>
    /// Whole seconds for the SQS API: sub-second values round up, so 500ms of backoff never becomes an
    /// immediate redelivery and a sub-second wait never turns into short polling.
    /// </summary>
    public static int WholeSeconds(TimeSpan value) => (int)Math.Ceiling(value.TotalSeconds);
}
