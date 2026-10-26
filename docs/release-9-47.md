# Upgrading to 9.47

One additive change, for event store dialects built on `Weasel.Storage`.

A dialect can now say that its per-event append SQL returns the `seq_id` the insert drew, and
`Weasel.Storage` will read that value back onto `IEvent.Sequence`.

**Nothing public was removed and no signature changed.** The two additions are `init` properties on
the Quick descriptor classes, and both default to `false` — a dialect that does not set the flag gets
exactly the operation it got in 9.46.

## A per-event Quick append can read back the sequence it drew

[#747](https://github.com/JasperFx/weasel/pull/747), contributed by
[@vpetrevski](https://github.com/vpetrevski), as the Weasel half of
[marten#5454](https://github.com/JasperFx/marten/issues/5454).

The Quick append modes have two routes. Most appends go through the dialect-supplied bulk operation,
which already reads back each event's sequence. The other route is the per-event
`QuickAppendEventWithVersion` INSERT, taken when a stream is starting or when an existing stream has
`ExpectedVersionOnServer` set — and on that route `seq_id` comes from an inlined server-side
`nextval()` that **nothing reads back**. The event keeps `Sequence` 0.

That matters because of what consumes `Sequence` afterwards. A unit of work that fails after the
INSERT has already burned the sequence numbers it drew, and the tombstone batch written to fill those
numbers **skips events whose `Sequence` is 0** — it has nothing to write a tombstone against. So the
numbers stay unfilled, and a reader that treats the event sequence as gapless sees a hole that never
closes.

[marten#5456](https://github.com/JasperFx/marten/pull/5456) moved a plain `QuickWithServerTimestamps`
`StartStream` off the per-event route, which is why this is narrower than it sounds — but plain
`Quick`, archived stream partitioning and strict stream identity all still take it, and so does
`FetchForWriting` against a missing stream, because `StartEmptyStream` is a Start.

### The seam

`QuickEventStorageDescriptor` and `QuickWithServerTimestampsEventStorageDescriptor` each gain:

```csharp
public bool AppendEventSqlReturnsSequence { get; init; }
```

Setting it asserts that `AppendEventSqlSuffix` ends in a clause returning `seq_id` — on PostgreSQL,
`returning seq_id`. The storage then wraps the operation so that its `PostprocessAsync` writes the
returned value onto `IEvent.Sequence`, which is what the bulk append path already does.

The wrapping is the whole subtlety. `NoDataReturnedCall` is the marker that tells the unit-of-work
walker **not** to advance the batched reader for an operation, and it sits on
`AppendEventOperationBase`, so a subclass cannot drop it. The flag therefore selects a wrapper that
delegates everything except that marker, rather than a subclass.

## What changes for you

- **Nothing, unless you write an event store dialect on `Weasel.Storage` and set the new flag.** The
  default is `false`, which is the 9.46 behaviour exactly.
- **If you do set it, your `AppendEventSqlSuffix` must return `seq_id`.** The flag is an assertion
  about your SQL, not a request for Weasel to rewrite it. A suffix that returns nothing while the flag
  is set leaves the operation expecting a result row that never arrives.
- **Rich mode is untouched.** The side-effect replay path keeps its old suffix, since its operation
  reads no result.

## New public API

| Member | Default |
| --- | --- |
| `QuickEventStorageDescriptor.AppendEventSqlReturnsSequence` | `false` |
| `QuickWithServerTimestampsEventStorageDescriptor.AppendEventSqlReturnsSequence` | `false` |
