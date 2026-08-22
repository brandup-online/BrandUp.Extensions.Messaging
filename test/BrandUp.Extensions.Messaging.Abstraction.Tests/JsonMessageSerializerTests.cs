namespace BrandUp.Extensions.Messaging;

public class JsonMessageSerializerTests
{
    readonly JsonMessageSerializer serializer = new();

    [Fact]
    public void Roundtrip()
    {
        var message = new TestMessage { Id = Guid.NewGuid(), Title = "hello", Count = 5 };

        var payload = serializer.Serialize(message);
        var restored = (TestMessage)serializer.Deserialize(payload, typeof(TestMessage));

        Assert.Equal(message.Id, restored.Id);
        Assert.Equal(message.Title, restored.Title);
        Assert.Equal(message.Count, restored.Count);
    }

    [Fact]
    public void Serialize_UsesCamelCase()
    {
        var payload = serializer.Serialize(new TestMessage { Title = "hello" });

        Assert.Contains("\"title\"", payload);
        Assert.DoesNotContain("\"Title\"", payload);
    }

    [Fact]
    public void GetTypeName_IsClrFullName()
    {
        Assert.Equal(typeof(TestMessage).FullName, serializer.GetTypeName(typeof(TestMessage)));
    }

    [Fact]
    public void Deserialize_InvalidJson_ThrowsMessagingException()
    {
        Assert.Throws<MessagingException>(() => serializer.Deserialize("{not json", typeof(TestMessage)));
    }

    [Fact]
    public void Deserialize_NullLiteral_ThrowsMessagingException()
    {
        Assert.Throws<MessagingException>(() => serializer.Deserialize("null", typeof(TestMessage)));
    }

    class TestMessage
    {
        public Guid Id { get; set; }
        public string? Title { get; set; }
        public int Count { get; set; }
    }
}
