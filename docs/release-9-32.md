# Upgrading to 9.32

9.32.0 adds two seams that let a document store answer a question only the store can answer, in
places where Weasel previously had to guess. Both are additive, both default to today's behaviour,
and neither changes what a migration does to an existing database.

::: tip Coming from 9.31?
Nothing here changes on upgrade. The two additions are opt-in: an interface member with a default
implementation, and a new constructor overload. A store adopts them separately — Marten does so in
its own release.
:::

## A storage can expose a document's mapped version / revision member

[#590](https://github.com/JasperFx/weasel/issues/590), raised from
[JasperFx/marten#5372](https://github.com/JasperFx/marten/issues/5372).

`IDocumentStorage<T>` gains two members, both defaulting to `null`:

```csharp
Guid? MappedVersionFor(T document) => null;
long? MappedRevisionFor(T document) => null;
```

The problem they solve is a store's session needing the expected version for a write's concurrency
guard. A session seeds that from the document's versioned marker interface, which it can test for
directly. A document that instead *maps* a plain member as its version — Marten's
`Metadata(m => m.Version.MapTo(x => x.Etag))` — carries the same information, but the session holds
the document only as `IDocumentStorage<T>` and had no way to ask for it. In Marten the consequence
was that the upsert bound `DBNull` into its `WHERE … mt_version = ?` guard, so every cross-session
write of such a document came back as a `ConcurrencyException` — a documented feature that could not
do the one thing it is named for.

The alternatives all involved a runtime type test on the storage plus explicit forwarding through
each decorator, which fails *silently* when a decorator is missed. That is the wrong failure mode for
a concurrency guard, hence the interface member.

`MappedRevisionFor` returns a `long` whatever the member's own width is, matching
`DocumentRevisionBinder`, which already accepts an `int` or a `long` revision member.

## The enum `in` fragments can be told the name the serializer stored

[#591](https://github.com/JasperFx/weasel/issues/591), raised from
[JasperFx/marten#5376](https://github.com/JasperFx/marten/issues/5376).

`EnumIsOneOfWhereFragment` and `EnumIsNotOneOfWhereFragment` each gain a constructor overload taking
an optional renderer:

```csharp
public EnumIsOneOfWhereFragment(object values, EnumStorage enumStorage, string locator,
    Func<object, string>? nameForValue)
```

Under `EnumStorage.AsString` both fragments rendered every value with `ToString()`, which is the enum
member's **declared** name. That is the stored name only until somebody renames the member —
`[JsonStringEnumMemberName]` on System.Text.Json, `[EnumMember]` on Newtonsoft. Then the filter
compares against a name that is not in the data, matches nothing, and reports it as *no rows* rather
than as an error, which reads as "no such data".

Only the serializer knows which name it wrote, so the caller supplies it. Passing nothing keeps the
previous behaviour byte for byte, which is why the existing three-argument constructors are
unchanged.

A delegate rather than an `ISerializer` keeps `Weasel.Postgresql.SqlGeneration` free of a
serialization dependency — and `Weasel.Core.ISerializer` could not have served here anyway, since it
carries no enum rendering.

Worth recording, because it is the tempting fix: reflecting over the enum's fields for the rename
attribute is **not** a substitute. It agrees with the serializer under a JIT and finds no attribute
at all in a trimmed Native AOT binary, so a reflection-based fix would quietly fall back to the
declared name in exactly the deployment where the symptom is hardest to diagnose.
