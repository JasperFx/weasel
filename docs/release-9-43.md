# Upgrading to 9.43

One change: the SQL Server advisory lock's monitor, added in 9.42, becomes configurable.

**Nothing in this release is a breaking change, and nothing behaves differently unless you ask it
to.** The new settings default to 9.42.0's hard-coded values exactly, and the existing constructor is
untouched.

## The SQL Server advisory lock monitor is configurable

[#700](https://github.com/JasperFx/weasel/issues/700) /
[#701](https://github.com/JasperFx/weasel/pull/701).
[9.42's fix](/release-9-42#sql-server-advisory-locks-end-with-their-session) gave
`Weasel.SqlServer.AdvisoryLock` a monitor that asks SQL Server, every 5 seconds, whether this session
still holds the locks `HasLock` reports. Defaulting it on was right, but it shipped as a hard-coded
timer with no seam. 9.43 adds the seam:

```csharp
public sealed class SqlServerAdvisoryLockOptions
{
    public TimeSpan MonitoringInterval { get; set; } = 5.Seconds();
    public TimeSpan ProbeTimeout { get; set; } = 5.Seconds();
}

// new; the existing three-argument constructor is unchanged
var advisoryLock = new AdvisoryLock(source, logger, databaseName, new SqlServerAdvisoryLockOptions
{
    MonitoringInterval = 10.Seconds(),
    ProbeTimeout = 3.Seconds()
});
```

If you use Polecat, you will set this through Polecat's own options rather than constructing the lock
yourself. Polecat is the only consumer of this lock in the Critter Stack, and Weasel's half does
nothing on its own until Polecat exposes it.

### Opting out re-opens the split brain

A non-positive or infinite `MonitoringInterval` starts no monitor at all. **That re-opens the failure
9.42 fixed**: a node that loses its session goes on reporting locks the server has already released,
and two nodes run the same shard. The probe already costs nothing while a node holds no locks, so
what opting out actually saves is one parked timer per database. That only matters for a deployment
with a very large number of databases.

### Keep the probe timeout under the interval

`DisposeAsync` stops the monitor from inside the same gate the probe holds, so an in-flight probe
delays disposal by up to `ProbeTimeout`, and a probe that runs to its limit blocks
`TryAttainLockAsync` for the same stretch. `ProbeTimeout` therefore bounds shutdown as much as it
bounds a query.

`SqlCommand.CommandTimeout` is whole seconds and reads zero as "wait forever", so a sub-second
`ProbeTimeout` is rounded **up** to one second. There is deliberately no way to ask for an unbounded
probe.

### Why there is no `ReleaseTimeout`

The PostgreSQL lock's `AdvisoryLockOptions` has a `ReleaseTimeout`, and #700 originally asked for
the same thing here. It is not in this release, because the reasoning that makes it safe on PostgreSQL
does not hold on SQL Server, and that was **measured**:

| Holder's connection | After `CloseAsync()` |
|---|---|
| pooled (the default) | lock **still held** |
| `Pooling=false` | lock released |

Closing a pooled `SqlConnection` returns it to the pool together with its session and its
`sp_getapplock` holds. The lock is not freed until that pooled connection is next reset or the
process exits. A release abandoned on a deadline would therefore leave the lock held against the next
node that needs it. Both rows are pinned by tests, and `DisposeAsync`'s remarks record why its
releases stay unbounded.

### Why a new name, not `AdvisoryLockOptions`

Reusing the PostgreSQL type's name would be a source break (CS0104) in any file that imports both
provider namespaces and uses the bare name, which today can only mean the PostgreSQL type.

## New public API

- `Weasel.SqlServer.SqlServerAdvisoryLockOptions`: `MonitoringInterval` and `ProbeTimeout`.
- `AdvisoryLock(Func<SqlConnection>, ILogger, string, SqlServerAdvisoryLockOptions)`: a new
  four-argument constructor. The three-argument constructor keeps its signature, so code compiled
  against 9.42 binds unchanged.
