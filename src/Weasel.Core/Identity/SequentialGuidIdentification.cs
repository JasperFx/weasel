using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     <see cref="IIdentification{TDoc,TId}" /> for documents whose id is a <see cref="Guid" />
///     generated sequentially via <see cref="Guid.CreateVersion7()" /> (a time-ordered UUIDv7, for
///     index locality) — no database round-trip.
/// </summary>
public sealed class SequentialGuidIdentification<TDoc> : IIdentification<TDoc, Guid>
    where TDoc : notnull
{
    private readonly Func<TDoc, Guid> _getter;
    private readonly Action<TDoc, Guid>? _setter;

    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    public SequentialGuidIdentification(MemberInfo idMember)
    {
        _getter = LambdaBuilder.Getter<TDoc, Guid>(idMember);
        _setter = LambdaBuilder.Setter<TDoc, Guid>(idMember);
    }

    public Guid Identity(TDoc document) => _getter(document);

    public Guid AssignIfMissing(TDoc document, ISequenceSource sequences)
    {
        var current = _getter(document);
        if (current != Guid.Empty)
        {
            return current;
        }

        // No setter on the id member (Guid Id { get; }) — the caller manages identity; return the
        // empty value rather than generating one.
        if (_setter is null)
        {
            return current;
        }

        var assigned = Guid.CreateVersion7();
        _setter(document, assigned);
        return assigned;
    }
}
