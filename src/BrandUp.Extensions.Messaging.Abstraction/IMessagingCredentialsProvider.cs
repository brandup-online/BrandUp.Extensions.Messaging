namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Where a transport takes its credentials from, when they are not static keys and not the SDK's own
/// chain — temporary (STS) credentials, a vault, a token exchanged for keys. The provider keeps the
/// current credentials cached; the library keeps that cache warm by calling
/// <see cref="RefreshAsync"/> on a timer, so the client never blocks on a network call while signing
/// a request and never has to be rebuilt when the credentials change.
/// </summary>
public interface IMessagingCredentialsProvider
{
    /// <summary>
    /// The credentials to sign with, from the cache. Called synchronously by the SDK, so it must not
    /// block on I/O — return what the last refresh produced.
    /// </summary>
    MessagingCredentials GetCurrent();

    /// <summary>
    /// Renews the cached credentials when they have expired or are about to. Called on a timer while
    /// the host runs — and once at start-up, so a provider that has never fetched anything is primed
    /// before the first message is published.
    /// </summary>
    Task RefreshAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Credentials as they stand at one moment.
/// </summary>
/// <param name="AccessKeyId">Access key id.</param>
/// <param name="SecretAccessKey">Secret access key.</param>
/// <param name="SessionToken">Session token of temporary credentials; <see langword="null"/> for permanent ones.</param>
/// <param name="ExpiresUtc">
/// When these credentials stop working. The client re-reads the provider after this instant, so it is
/// what makes a rotation take effect without rebuilding anything. <see langword="null"/> means they do
/// not expire on their own.
/// </param>
public sealed record MessagingCredentials(
    string AccessKeyId,
    string SecretAccessKey,
    string? SessionToken = null,
    DateTimeOffset? ExpiresUtc = null);
