// Linked as a shared source file into every AWS provider package (AmazonSqs, AmazonKinesis): each
// package keeps the credentials of its own connections warm, and the Abstraction package must stay free
// of the hosting and logging dependencies this needs.
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// The credentials provider of one connection, as declared at registration. The provider is resolved
/// once and kept here, so the client that signs with it and the refresher that keeps it fresh work on
/// the same instance.
/// </summary>
internal sealed class MessagingCredentialsRegistration(
    string connectionName, TimeSpan refreshInterval, Func<IServiceProvider, IMessagingCredentialsProvider> factory)
{
    readonly Lock sync = new();
    IMessagingCredentialsProvider? provider;

    /// <summary>Connection the provider serves; empty for the default one.</summary>
    public string ConnectionName { get; } = connectionName;

    /// <summary>How often <see cref="IMessagingCredentialsProvider.RefreshAsync"/> is called.</summary>
    public TimeSpan RefreshInterval { get; } = refreshInterval;

    public IMessagingCredentialsProvider Resolve(IServiceProvider services)
    {
        lock (sync)
            return provider ??= factory(services);
    }
}

internal static class CredentialsRegistrations
{
    /// <summary>
    /// How often a provider is asked to renew what it caches. A provider renews only what needs
    /// renewing, so this is a heartbeat rather than a rotation period.
    /// </summary>
    public static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Binds a credentials provider to one connection, and makes sure something keeps it fresh. One
    /// provider per connection: a second one would leave which credentials are used to registration
    /// order.
    /// </summary>
    public static void Add(
        IServiceCollection services,
        string connectionName,
        TimeSpan? refreshInterval,
        Func<IServiceProvider, IMessagingCredentialsProvider> factory,
        string registrationMethod)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(factory);

        if (refreshInterval is TimeSpan interval && interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(refreshInterval), interval, "The refresh interval must be positive.");

        foreach (var descriptor in services)
        {
            if (!descriptor.IsKeyedService
                && descriptor.ImplementationInstance is MessagingCredentialsRegistration existing
                && string.Equals(existing.ConnectionName, connectionName, StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"A credentials provider is already registered for {Describe(connectionName)}; {registrationMethod} was called twice.");
        }

        services.AddSingleton(
            new MessagingCredentialsRegistration(connectionName, refreshInterval ?? DefaultRefreshInterval, factory));

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, MessagingCredentialsRefresher>());
    }

    /// <summary>
    /// The same, for a provider named by its type: it is registered as a singleton unless the
    /// application registered it itself, so one instance serves the client and the refresher — and
    /// every connection that names the same type.
    /// </summary>
    public static void Add<TProvider>(
        IServiceCollection services, string connectionName, TimeSpan? refreshInterval, string registrationMethod)
        where TProvider : class, IMessagingCredentialsProvider
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<TProvider>();
        Add(services, connectionName, refreshInterval, sp => sp.GetRequiredService<TProvider>(), registrationMethod);
    }

    /// <summary>The provider of one connection, or <see langword="null"/> when it has none.</summary>
    public static IMessagingCredentialsProvider? Resolve(
        IServiceProvider services, IEnumerable<MessagingCredentialsRegistration> registrations, string connectionName)
    {
        foreach (var registration in registrations)
        {
            if (string.Equals(registration.ConnectionName, connectionName, StringComparison.Ordinal))
                return registration.Resolve(services);
        }

        return null;
    }

    public static string Describe(string connectionName)
        => connectionName.Length == 0 ? "the default connection" : $"connection '{connectionName}'";
}

/// <summary>
/// Keeps every registered provider's cache warm: each is primed at start-up and asked to renew on its
/// own interval, so signing a request never waits for a token endpoint. A provider that throws is
/// logged and tried again on the next tick — credentials that are still valid must not take the host
/// down with them.
/// </summary>
internal sealed class MessagingCredentialsRefresher(
    IServiceProvider services,
    IEnumerable<MessagingCredentialsRegistration> registrations,
    ILogger<MessagingCredentialsRefresher> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => Task.WhenAll(registrations.Select(registration => RefreshAsync(registration, stoppingToken)));

    async Task RefreshAsync(MessagingCredentialsRegistration registration, CancellationToken stoppingToken)
    {
        // Yield first: a BackgroundService that runs synchronously to its first await would hold up
        // host start-up, and priming credentials may take a network call.
        await Task.Yield();

        var provider = registration.Resolve(services);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await provider.RefreshAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Refreshing the credentials of {Connection} failed; retrying in {Delay}.",
                    CredentialsRegistrations.Describe(registration.ConnectionName), registration.RefreshInterval);
            }

            try
            {
                await Task.Delay(registration.RefreshInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
