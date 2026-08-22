namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Declares the logical queue (or stream) name of a message type, so registrations can omit it:
/// <c>AddQueue&lt;OrderCreated&gt;()</c>. On a <see cref="MessagingContext"/> property it names that
/// queue and wins over the attribute of the message type. The physical name is resolved from the
/// logical one via the connection options — overrides, environment prefix/suffix and the <c>.fifo</c>
/// suffix.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Property, Inherited = false)]
public sealed class QueueAttribute : Attribute
{
    public QueueAttribute(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }

    public string Name { get; }
}
