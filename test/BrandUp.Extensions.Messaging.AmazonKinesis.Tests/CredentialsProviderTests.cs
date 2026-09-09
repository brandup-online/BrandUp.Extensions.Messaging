using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The stream transport takes its credentials from a provider the same way the queue transport does —
/// each package carries its own copy of that machinery, so each is checked.
/// </summary>
public class CredentialsProviderTests
{
    static void ConfigureConnection(KinesisMessagingOptions options)
    {
        options.ServiceUrl = "https://yds.serverless.yandexcloud.net";
        options.Region = "ru-central1";
    }

    [Fact]
    public void Provider_ServesTheConnection_AndOutranksStaticKeys()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKinesisMessaging(options =>
        {
            ConfigureConnection(options);
            options.AccessKeyId = "static";
            options.SecretAccessKey = "static";
        }, validateOnStart: false).UseCredentialsProvider<StubProvider>();

        using var serviceProvider = services.BuildServiceProvider();
        var registrations = serviceProvider.GetServices<MessagingCredentialsRegistration>();

        var provider = CredentialsRegistrations.Resolve(serviceProvider, registrations, Options.DefaultName);
        Assert.Same(serviceProvider.GetRequiredService<StubProvider>(), provider);

        var credentials = AwsConnection.CreateCredentials(
            serviceProvider.GetRequiredService<IOptions<KinesisMessagingOptions>>().Value, provider)!;
        Assert.Equal("from-provider", credentials.GetCredentials().AccessKey);
    }

    [Fact]
    public async Task Refresher_KeepsTheProviderWarm()
    {
        var provider = new StubProvider();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddKinesisMessaging(ConfigureConnection, validateOnStart: false)
            .UseCredentialsProvider(_ => provider, TimeSpan.FromMilliseconds(20));

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (provider.Refreshes < 3)
                await Task.Delay(20, cts.Token);
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    public class StubProvider : IMessagingCredentialsProvider
    {
        int refreshes;

        public int Refreshes => Volatile.Read(ref refreshes);

        public MessagingCredentials GetCurrent()
            => new("from-provider", "sk", "token", DateTimeOffset.UtcNow.AddHours(1));

        public Task RefreshAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref refreshes);
            return Task.CompletedTask;
        }
    }
}
