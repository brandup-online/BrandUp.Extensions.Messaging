namespace BrandUp.Extensions.Messaging;

public class MessagingException : Exception
{
    public MessagingException(string message)
        : base(message)
    {
    }

    public MessagingException(string message, Exception inner)
        : base(message, inner)
    {
    }

    public MessagingException(string message, string? errorCode, Exception inner)
        : this(message, MessagingErrorKind.Unknown, errorCode, inner)
    {
    }

    public MessagingException(string message, MessagingErrorKind kind, string? errorCode, Exception inner)
        : base(message, inner)
    {
        Kind = kind;
        ErrorCode = errorCode;
    }

    /// <summary>For failures the provider reports as data rather than as an exception of its own.</summary>
    protected MessagingException(string message, MessagingErrorKind kind, string? errorCode)
        : base(message)
    {
        Kind = kind;
        ErrorCode = errorCode;
    }

    /// <summary>
    /// What went wrong, in provider-neutral terms. The provider classifies the failure where it still
    /// has the typed SDK exception, so callers never match on provider error strings.
    /// </summary>
    public MessagingErrorKind Kind { get; } = MessagingErrorKind.Unknown;

    /// <summary>Provider error code, e.g. <c>QueueDoesNotExist</c>; <see langword="null"/> if unknown.</summary>
    public string? ErrorCode { get; }

    /// <summary>The destination queue does not exist. Create it or enable queue auto-creation.</summary>
    public bool IsQueueNotFound => Kind == MessagingErrorKind.QueueNotFound;
}

/// <summary>
/// Some messages of a batch were not published. The transport reports these one by one, so the batch is
/// neither wholly published nor wholly rejected: <see cref="FailedIndexes"/> says which messages to
/// retry, and everything else is already out.
/// </summary>
public class BatchPublishException(string message, string? errorCode, IReadOnlyList<int> failedIndexes)
    : MessagingException(message, MessagingErrorKind.Unknown, errorCode)
{
    /// <summary>
    /// Positions in the batch that was passed in, of the messages the transport refused — in ascending
    /// order. Retry exactly these; the rest were published.
    /// </summary>
    public IReadOnlyList<int> FailedIndexes { get; } = failedIndexes;
}

public enum MessagingErrorKind
{
    Unknown,

    /// <summary>The queue or stream does not exist.</summary>
    QueueNotFound,

    /// <summary>The destination already exists with different settings, or was created concurrently.</summary>
    QueueConflict,

    /// <summary>The payload could not be serialized or deserialized.</summary>
    Serialization,
}
