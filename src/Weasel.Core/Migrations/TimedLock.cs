using JasperFx.Core;

namespace Weasel.Core.Migrations;

internal class TimedLock
{
    private readonly SemaphoreSlim _toLock;

    public TimedLock()
    {
        _toLock = new SemaphoreSlim(1, 1);
    }

    /// <param name="timeout">How long to wait for the other thread to finish.</param>
    /// <param name="description">
    ///     What another thread is busy doing, for the timeout message -- the feature whose storage
    ///     is being applied, or the database being created. There is nothing else to go on: the
    ///     lock is in-process, so no database, no session and no statement is involved.
    /// </param>
    /// <param name="ct"></param>
    /// <exception cref="TimeoutException">
    ///     The lock was not attained in time. This used to be thrown with no message at all
    ///     (weasel#602), which is a hard thing to act on at three in the morning.
    /// </exception>
    public async Task<LockReleaser> Lock(TimeSpan timeout, string? description = null,
        CancellationToken ct = default)
    {
        if (await _toLock.WaitAsync(timeout, ct).ConfigureAwait(false))
        {
            return new LockReleaser(_toLock);
        }

        var what = description.IsEmpty() ? "applying the schema" : description!;

        throw new TimeoutException(
            $"Timed out after {timeout.TotalSeconds:0.##}s waiting for another thread in this process to finish "
            + $"{what}. Apply migrations eagerly at startup (ApplyAllConfiguredChangesToDatabaseAsync or "
            + "db-apply) so runtime storage checks find them already applied.");
    }

    public readonly struct LockReleaser: IDisposable
    {
        private readonly SemaphoreSlim toRelease;

        public LockReleaser(SemaphoreSlim toRelease)
        {
            this.toRelease = toRelease;
        }

        public void Dispose()
        {
            toRelease.Release();
        }
    }
}
