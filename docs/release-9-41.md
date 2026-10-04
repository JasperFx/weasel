# Upgrading to 9.41

9.41 makes Weasel's identity runtime reachable from a host published with Native AOT. That is the
whole release: two additions, both in `Weasel.Core.Identity`, both additive.

**Nothing in this release is a breaking change.** Nothing public was removed, no behaviour changed on
any existing code path, and the one new base interface carries default implementations for every
member — see [the note below](#about-that-base-interface) for why that detail is load-bearing rather
than a formality.

## The problem, in one paragraph

`Type.MakeGenericType` can close an instantiation whose type arguments are all **reference** types,
because those share one canonical body. It cannot close one with a **value**-type argument, because
that needs native code the ILC compiler never generated. A consumer whose document metadata is
`Type`-based — Marten's `ProviderGraph`, Polecat's `DocumentMapping` — had no choice but to close
Weasel's identity generics at runtime, and an id type is routinely `Guid`, `int` or `long`. So a
natively published store could not be built at all.

```
System.NotSupportedException: 'Polecat.Internal.IdentityAssigner`2[DeadLetterEvent,System.Guid]'
is missing native code or metadata.
```

Reflection is not a way around it, which is worth recording because it looks like one. Holding the
strategy as `object` and invoking `AssignIfMissing` through `MethodInfo` fails differently, with
`Sequence contains no matching element`: `GetMethods()` comes back empty, because nothing statically
references those members and the trimmer took their metadata too. **You cannot reflect your way out
of generics under AOT.** The seam has to be something referenced.

## Using a strategy without naming its type arguments

[#691](https://github.com/JasperFx/weasel/pull/691), from
[#689](https://github.com/JasperFx/weasel/issues/689).

`Weasel.Core.Identity.IIdentification` is a new non-generic facade that
`IIdentification<TDoc, TId>` satisfies:

```csharp
public interface IIdentification
{
    object Identity(object document);
    object AssignIfMissing(object document, ISequenceSource sequences);
    object ToRawSqlValue(object id);
    Type RawSqlType { get; }
    object ReadIdFromReader(DbDataReader reader, int columnOrdinal);
}
```

`AssignIfMissing` is the one that blocked store construction. `RawSqlType` is the one a caller
holding `object` most often cannot work out for itself: for a strong-typed id it is the **inner**
primitive, not the id type.

Every member boxes, by construction. That is the point and also the cost — if you *can* name your
type arguments, keep using `IIdentification<TDoc, TId>`, which allocates nothing on the path where a
document already has an id. The facade is an opt-in for code that does not know the types.

### About that base interface

Adding a base interface to a public interface is only safe because every one of the facade's members
has a **default implementation** on `IIdentification<TDoc, TId>` that forwards to the generic member.
A type already compiled against the generic interface alone — including one already published and
loaded against this release by NuGet resolving Weasel up on its own — still loads, because the
runtime finds those bodies.

Had any member been abstract instead, this would have been a **runtime** break that no amount of
building or restoring reveals beforehand: Weasel builds, Weasel's tests pass, downstream restores and
compiles, and then the CLR refuses the type the first time a host loads it. That is exactly what
[9.40.0 shipped and #682 caught](/release-9-40#icommandbuilder-parametercount), one release ago.

Verified for this release the only way that counts — against the **published** Marten 9.45.0 and
Polecat 5.35.0, with this Weasel.Core resolved up, forcing the interface map the way a host does on
first load. Neither assembly implements anything in `Weasel.Core.Identity` yet, and all 2477 and 1073
types load.

**If you implement `IIdentification<TDoc, TId>` yourself, you do not have to change anything.**

## Building a strategy without closing a generic

[#692](https://github.com/JasperFx/weasel/pull/692), from
[#690](https://github.com/JasperFx/weasel/issues/690).

The use side is only half of it: a `Type`-keyed consumer also has to *construct* the strategy.
`Weasel.Core.Identity.Identifications` is a non-generic entry point for all seven:

```csharp
// Nothing here names TDoc or TId, so nothing here needs MakeGenericType.
IIdentification identification = Identifications.ForSequentialGuid(documentType, idMember);
IIdentification wrapped = Identifications.ForValueType(
    documentType, idMember, ValueTypeInfo.ForType(wrapperType), documentType);
```

Six of the seven close a generic over the document type alone, which is a class, so they already
worked natively. `ValueTypeIdentification<TDoc, TWrapper, TInner>` — strong-typed ids, Vogen /
StronglyTypedId wrappers and F# single-case discriminated unions — is the one that did not:
`TInner` is the wrapped primitive and `TWrapper` is usually a `readonly record struct`, so two of the
three arguments are value types. Measured on macOS arm64, ILC, net9.0:

```
IsDynamicCodeSupported = False
SequentialGuidIdentification<Doc>: constructed OK
ValueTypeIdentification<Doc,FooId,Guid>: NotSupportedException:
  'Weasel.Core.Identity.ValueTypeIdentification`3[Doc,FooId,System.Guid]'
  is missing native code or metadata.
```

`Identifications.ForValueType` chooses:

- **Where dynamic code is available** — every process that is not natively published, which is most
  of them — it closes `ValueTypeIdentification<TDoc, TWrapper, TInner>` exactly as a caller used to.
  The hot path stays an FEC-compiled delegate. **Nothing gets slower.**
- **Under Native AOT** it returns the new `ReflectedValueTypeIdentification`, which closes no generic
  and compiles no expression: it reaches the id member through `PropertyInfo.GetValue` / `SetValue`
  and the wrapper through `ValueTypeInfo`'s own constructor or static factory.

::: tip Why the branch is on a feature switch
`RuntimeFeature.IsDynamicCodeSupported` is a published feature switch. ILC substitutes it to `false`
and then trims the dead branch, so an AOT build neither warns about the `MakeGenericType` it will
never reach nor carries it. Catching `NotSupportedException` instead would do neither, and would
swallow real failures.
:::

The two implementations must stay in step, which is the real cost of having two. Weasel holds them
against each other case for case — Guid / int / long / string inner types, already-assigned ids,
`Nullable<wrapper>` properties, `ToRawSqlValue`, `RawSqlType`, `ReadIdFromReader`, and the one place
they reach the same outcome by different routes.

### What this deliberately does not do

There is no mapping from `(documentType, idType)` to a strategy. **Which** strategy fits an id is a
store's policy, not a fact about the id: the same `Guid` is a sequential id in one configuration and
a caller-assigned one in another, and an `int` may be Hi-Lo or externally assigned. Marten and
Polecat do not agree, so a mapping in Weasel would be a guess wearing a library's authority. Weasel
owns the construction; the store keeps the choice.

## New public API

- `Weasel.Core.Identity.IIdentification` — the non-generic facade, described above. Satisfied by
  default implementations on `IIdentification<TDoc, TId>`, so existing strategies are unaffected.
- `Weasel.Core.Identity.Identifications` — `ForSequentialGuid`, `ForRandomGuid`, `ForHiloInt`,
  `ForHiloLong`, `ForIdentityKey`, `ForExternallyAssignedString` and `ForValueType`, each building a
  strategy from runtime `Type`s.
- `Weasel.Core.Identity.ReflectedValueTypeIdentification` — the strong-typed id strategy with no
  closed generic and no compiled expression. `Identifications.ForValueType` hands it out under Native
  AOT; you can also construct it directly if you want the reflective path on purpose.
