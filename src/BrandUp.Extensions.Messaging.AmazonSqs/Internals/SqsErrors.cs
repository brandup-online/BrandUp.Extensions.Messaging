using Amazon.SQS;
using Amazon.SQS.Model;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The one translation from SDK errors to the library contract. Classification happens here, where the
/// typed SQS exception is still in hand, so callers never match on provider error strings.
/// </summary>
internal static class SqsErrors
{
    public static MessagingException Wrap(string message, AmazonSQSException ex)
        => new(message, KindOf(ex), ex.ErrorCode, ex);

    static MessagingErrorKind KindOf(AmazonSQSException ex) => ex switch
    {
        QueueDoesNotExistException => MessagingErrorKind.QueueNotFound,
        QueueNameExistsException => MessagingErrorKind.QueueConflict,
        _ => ex.ErrorCode switch
        {
            "QueueDoesNotExist" or "AWS.SimpleQueueService.NonExistentQueue" => MessagingErrorKind.QueueNotFound,
            "QueueAlreadyExists" or "AWS.SimpleQueueService.QueueNameExists" => MessagingErrorKind.QueueConflict,
            _ => MessagingErrorKind.Unknown,
        },
    };
}
