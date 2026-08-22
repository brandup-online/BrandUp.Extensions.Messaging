using System.Reflection;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The one rule for resolving a message type's logical destination name: an explicit name, else the
/// declaring member's <see cref="QueueAttribute"/>, else the message type's, else the caller's
/// fallback (or an error). Shared by every builder — providers, contexts and the testing fake must
/// agree, so tests fail exactly where production registration would.
/// </summary>
internal static class LogicalNames
{
    public static string Resolve(Type messageType, string? explicitName, string parameterName, string registrationMethod)
        => Resolve(messageType, explicitName, member: null)
            ?? throw new ArgumentException(
                $"Name of message type {messageType.FullName} is not set: pass {parameterName} to {registrationMethod} or declare [Queue] on the type.",
                parameterName);

    /// <summary>
    /// Name for a context property: the property's <see cref="QueueAttribute"/> wins over the message
    /// type's, and the property name is the last resort — a context property always has a name.
    /// </summary>
    public static string ResolveForMember(Type messageType, MemberInfo member)
        => Resolve(messageType, explicitName: null, member) ?? member.Name;

    static string? Resolve(Type messageType, string? explicitName, MemberInfo? member)
        => explicitName
            ?? member?.GetCustomAttribute<QueueAttribute>()?.Name
            ?? messageType.GetCustomAttribute<QueueAttribute>()?.Name;
}
