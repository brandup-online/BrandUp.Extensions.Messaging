using Amazon.Runtime;
using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Credentials that come from a provider rather than from static keys: what the SDK signs with, when it
/// re-reads them, and who keeps the provider's cache warm.
/// </summary>
public class CredentialsProviderTests
{
    static void ConfigureConnection(SqsMessagingOptions options)
    {
        options.ServiceUrl = "http://localhost:9324";
        options.Region = "elasticmq";
    }

    [Fact]
    public void Credentials_ComeFromTheProvider_AndOutrankStaticKeys()
    {
        var options = new SqsMessagingOptions { ServiceUrl = "http://localhost:9324", AccessKeyId = "static", SecretAccessKey = "static" };
        var provider = new StubProvider(new MessagingCredentials("ak", "sk", "token", DateTimeOffset.UtcNow.AddHours(1)));

        var credentials = AwsConnection.CreateCredentials(options, provider);

        var immutable = Assert.IsAssignableFrom<AWSCredentials>(credentials).GetCredentials();
        Assert.Equal("ak", immutable.AccessKey);
        Assert.Equal("sk", immutable.SecretKey);
        Assert.Equal("token", immutable.Token);
    }

    [Fact]
    public void Credentials_WithoutAProvider_StayAsBefore()
    {
        var withKeys = new SqsMessagingOptions { ServiceUrl = "x", AccessKeyId = "ak", SecretAccessKey = "sk" };
        Assert.IsType<BasicAWSCredentials>(AwsConnection.CreateCredentials(withKeys));

        var withSession = new SqsMessagingOptions { ServiceUrl = "x", AccessKeyId = "ak", SecretAccessKey = "sk", SessionToken = "t" };
        Assert.IsType<SessionAWSCredentials>(AwsConnection.CreateCredentials(withSession));

        // Neither a provider nor keys: the client is built without credentials, so the SDK chain applies.
        Assert.Null(AwsConnection.CreateCredentials(new SqsMessagingOptions { ServiceUrl = "x" }));
    }

    [Fact]
    public async Task RotatedCredentials_AreReReadFromTheProvider()
    {
        var provider = new StubProvider(new MessagingCredentials("first", "sk", null, DateTimeOffset.UtcNow.AddSeconds(1)));
        var credentials = AwsConnection.CreateCredentials(new SqsMessagingOptions { ServiceUrl = "x" }, provider)!;

        Assert.Equal("first", credentials.GetCredentials().AccessKey);

        // A rotation takes effect without the client being rebuilt: once what it holds has expired, the
        // SDK asks the provider again.
        provider.Current = new MessagingCredentials("second", "sk", null, DateTimeOffset.UtcNow.AddHours(1));
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        Assert.Equal("second", credentials.GetCredentials().AccessKey);
    }

    [Fact]
    public void StaleCredentials_FailWithAnActionableError()
    {
        // The provider's cache went stale — the SDK's own message would only talk about its refresh
        // cycle, so the bridge says what actually has to be fixed.
        var provider = new StubProvider(new MessagingCredentials("ak", "sk", null, DateTimeOffset.UtcNow.AddSeconds(-1)));
        var credentials = AwsConnection.CreateCredentials(new SqsMessagingOptions { ServiceUrl = "x" }, provider)!;

        var exception = Assert.Throws<MessagingException>(credentials.GetCredentials);
        Assert.Contains(nameof(StubProvider), exception.Message);
        Assert.Contains("RefreshAsync", exception.Message);
    }

    [Fact]
    public void EmptyCredentials_FailWithAnActionableError()
    {
        // A provider that has never fetched anything hands out blanks; signing with them would fail far
        // from the cause, with a message about a malformed request.
        var provider = new StubProvider(new MessagingCredentials("", ""));
        var credentials = AwsConnection.CreateCredentials(new SqsMessagingOptions { ServiceUrl = "x" }, provider)!;

        var exception = Assert.Throws<MessagingException>(credentials.GetCredentials);
        Assert.Contains(nameof(StubProvider), exception.Message);
        Assert.Contains("RefreshAsync", exception.Message);
    }

    [Fact]
    public void Provider_IsBoundToItsConnection()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessagingConnection("orders", ConfigureConnection, validateOnStart: false)
            .UseCredentialsProvider<StubProvider>();
        services.AddSqsMessaging(ConfigureConnection, validateOnStart: false);

        using var serviceProvider = services.BuildServiceProvider();
        var registrations = serviceProvider.GetServices<MessagingCredentialsRegistration>();

        Assert.NotNull(CredentialsRegistrations.Resolve(serviceProvider, registrations, "orders"));
        // The default connection has none of its own, and must not borrow another account's credentials.
        Assert.Null(CredentialsRegistrations.Resolve(serviceProvider, registrations, Options.DefaultName));
    }

    [Fact]
    public void Provider_IsResolvedOnce_AndSharedWithTheRefresher()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging(ConfigureConnection, validateOnStart: false).UseCredentialsProvider<StubProvider>();

        using var serviceProvider = services.BuildServiceProvider();
        var registrations = serviceProvider.GetServices<MessagingCredentialsRegistration>();

        var first = CredentialsRegistrations.Resolve(serviceProvider, registrations, Options.DefaultName);
        var second = CredentialsRegistrations.Resolve(serviceProvider, registrations, Options.DefaultName);

        // The client signs with what the refresher renews, so both must be the same instance.
        Assert.Same(first, second);
        Assert.Same(first, serviceProvider.GetRequiredService<StubProvider>());
    }

    [Fact]
    public void Provider_RegisteredTwiceForOneConnection_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddSqsMessaging(ConfigureConnection, validateOnStart: false).UseCredentialsProvider<StubProvider>();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.UseCredentialsProvider<StubProvider>());
        Assert.Contains("default connection", exception.Message);
    }

    [Fact]
    public void RefreshInterval_MustBePositive()
    {
        var services = new ServiceCollection();
        var builder = services.AddSqsMessaging(ConfigureConnection, validateOnStart: false);

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.UseCredentialsProvider<StubProvider>(TimeSpan.Zero));
    }

    [Fact]
    public async Task Refresher_PrimesTheProvider_AndKeepsRefreshing()
    {
        var provider = new StubProvider(new MessagingCredentials("ak", "sk"));
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSqsMessaging(ConfigureConnection, validateOnStart: false)
            .UseCredentialsProvider(_ => provider, TimeSpan.FromMilliseconds(20));

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            // Primed at start-up, then kept warm: signing must never wait for a token endpoint.
            await WaitForAsync(() => provider.Refreshes >= 3, TimeSpan.FromSeconds(10));
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Refresher_KeepsGoingAfterAFailedRefresh()
    {
        var provider = new StubProvider(new MessagingCredentials("ak", "sk")) { Fails = true };
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSqsMessaging(ConfigureConnection, validateOnStart: false)
            .UseCredentialsProvider(_ => provider, TimeSpan.FromMilliseconds(20));

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            // A token endpoint being down must not take the host with it: credentials in hand may still
            // be valid, and the next tick may succeed.
            await WaitForAsync(() => provider.Refreshes >= 3, TimeSpan.FromSeconds(10));
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
            await Task.Delay(20, cts.Token);
    }

    public class StubProvider(MessagingCredentials? credentials = null) : IMessagingCredentialsProvider
    {
        int refreshes;

        public MessagingCredentials Current { get; set; } = credentials ?? new MessagingCredentials("ak", "sk");
        public bool Fails { get; init; }
        public int Refreshes => Volatile.Read(ref refreshes);

        public MessagingCredentials GetCurrent() => Current;

        public Task RefreshAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref refreshes);

            return Fails ? Task.FromException(new InvalidOperationException("token endpoint is down")) : Task.CompletedTask;
        }
    }
}
