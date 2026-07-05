using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     <see cref="IIdentification{TDoc,TId}" /> for documents with externally-assigned
///     <see cref="string" /> keys (no auto-generation). The caller supplies the id; the strategy
///     reads it back and refuses to generate one.
/// </summary>
public sealed class StringIdentification<TDoc> : IIdentification<TDoc, string>
    where TDoc : notnull
{
    private readonly Func<TDoc, string> _getter;

    [RequiresUnreferencedCode("Builds an FEC-compiled accessor delegate over the id member via LambdaBuilder.")]
    public StringIdentification(MemberInfo idMember)
    {
        _getter = LambdaBuilder.Getter<TDoc, string>(idMember);
    }

    public string Identity(TDoc document) => _getter(document);

    public string AssignIfMissing(TDoc document, ISequenceSource sequences)
    {
        var current = _getter(document);
        if (current.IsNotEmpty())
        {
            return current;
        }

        throw new InvalidOperationException(
            $"{typeof(TDoc).Name} uses externally-assigned string keys but the document's id is null or empty.");
    }
}
