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
