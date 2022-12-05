namespace Weasel.Core.Migrations;

internal class TimedLock
{
    private readonly SemaphoreSlim _toLock;

    public TimedLock()
    {
        _toLock = new SemaphoreSlim(1, 1);
    }

    public async Task<LockReleaser> Lock(TimeSpan timeout)
    {
        if (await _toLock.WaitAsync(timeout).ConfigureAwait(false))
        {
            return new LockReleaser(_toLock);
        }

        throw new TimeoutException();
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
