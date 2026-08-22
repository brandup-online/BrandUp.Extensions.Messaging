namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Settings of one queue binding. Most apply when the queue is created (see the provider's queue
/// auto-creation option) — an existing queue is used as is, the settings are not reconciled.
/// <see cref="PoisonMessageHandling"/> is client-side behavior and always applies.
/// </summary>
public class QueueSettings
{
    /// <summary>
    /// FIFO queue: strict ordering inside a message group and exactly-once publishing within the
    /// deduplication window. The physical name gets the <c>.fifo</c> suffix automatically.
    /// </summary>
    public bool Fifo { get; set; }

    /// <summary>
    /// FIFO only: deduplicate by a SHA-256 of the message body instead of an explicit
    /// <see cref="PublishOptions.DeduplicationId"/>.
    /// </summary>
    public bool ContentBasedDeduplication { get; set; }

    /// <summary>How long a received message stays invisible to other consumers. Provider default: 30 seconds.</summary>
    public TimeSpan? VisibilityTimeout { get; set; }

    /// <summary>How long an unconsumed message is kept. Provider default: 4 days.</summary>
    public TimeSpan? MessageRetention { get; set; }

    /// <summary>Default delivery delay of every published message. Provider default: none.</summary>
    public TimeSpan? DeliveryDelay { get; set; }

    /// <summary>
    /// After this many failed deliveries the message is moved to the dead-letter queue. Setting it turns
    /// dead-lettering on: the DLQ (<see cref="DeadLetterQueueName"/>, by default <c>{name}-dlq</c>) is
    /// created together with the queue.
    /// </summary>
    public int? MaxReceiveCount { get; set; }

    /// <summary>
    /// Logical name of the dead-letter queue. Defaults to the queue's logical name with the <c>-dlq</c>
    /// suffix. Only used when <see cref="MaxReceiveCount"/> is set. A FIFO queue gets a FIFO DLQ.
    /// </summary>
    public string? DeadLetterQueueName { get; set; }

    /// <summary>
    /// What to do with a poison message — one whose payload cannot be deserialized or whose type
    /// attribute does not match the queue's message type. Client-side behavior, applies regardless of
    /// how the queue was created.
    /// </summary>
    public PoisonMessageHandling PoisonMessageHandling { get; set; } = PoisonMessageHandling.Redeliver;
}

public enum PoisonMessageHandling
{
    /// <summary>
    /// Leave the message in the queue: it redelivers after each visibility timeout and dead-letters
    /// once <see cref="QueueSettings.MaxReceiveCount"/> is exceeded. The default. Without a dead-letter
    /// queue the message redelivers forever — and on a FIFO queue it blocks its message group, so
    /// configure <see cref="QueueSettings.MaxReceiveCount"/> or choose <see cref="Delete"/>.
    /// </summary>
    Redeliver,

    /// <summary>Log and delete the message. The payload is lost, the queue keeps flowing.</summary>
    Delete,
}
