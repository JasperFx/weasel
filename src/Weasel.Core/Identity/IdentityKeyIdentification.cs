using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     <see cref="IIdentification{TDoc,TId}" /> for the identity-key strategy — a string id of the
///     form <c>"{alias}/{nextLong}"</c>, where <c>alias</c> is the document mapping's alias and the
///     suffix comes from the per-document Hi-Lo sequence.
/// </summary>
public sealed class IdentityKeyIdentification<TDoc> : IIdentification<TDoc, string>
    where TDoc : notnull
{
    private readonly Func<TDoc, string> _getter;
    private readonly Action<TDoc, string>? _setter;
    private readonly string _aliasPrefix;
    private readonly Type _sequenceKey;

    /// <param name="idMember">The string-typed id member on <typeparamref name="TDoc" />.</param>
    /// <param name="mappingAlias">The mapping's alias — used as the key prefix (<c>"{alias}/…"</c>).</param>
    /// <param name="sequenceKey">The type used to look up the Hi-Lo sequence.</param>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    public IdentityKeyIdentification(MemberInfo idMember, string mappingAlias, Type sequenceKey)
    {
        _getter = LambdaBuilder.Getter<TDoc, string>(idMember);
        _setter = LambdaBuilder.Setter<TDoc, string>(idMember);
        _aliasPrefix = mappingAlias + "/";
        _sequenceKey = sequenceKey;
    }

    public string Identity(TDoc document) => _getter(document);

    public string AssignIfMissing(TDoc document, ISequenceSource sequences)
    {
        var current = _getter(document);
        if (current.IsNotEmpty())
        {
            return current;
        }

        var nextLong = sequences.SequenceFor(_sequenceKey).NextLong();
        if (_setter is null) return current;
        var assigned = _aliasPrefix + nextLong;
        _setter(document, assigned);
        return assigned;
    }
}
