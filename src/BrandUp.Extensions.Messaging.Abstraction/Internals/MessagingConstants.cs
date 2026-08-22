namespace BrandUp.Extensions.Messaging.Internals;

internal static class MessagingConstants
{
    /// <summary>SQS message attribute carrying <see cref="IMessageSerializer.GetTypeName"/> of the payload.</summary>
    public const string TypeAttributeName = "BrandUp-MessageType";
}
