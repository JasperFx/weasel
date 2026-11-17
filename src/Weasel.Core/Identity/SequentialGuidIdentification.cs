using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core.Sequences;

namespace Weasel.Core.Identity;

/// <summary>
///     <see cref="IIdentification{TDoc,TId}" /> for documents whose id is a <see cref="Guid" />
///     generated sequentially by an <see cref="IGuidGenerator" /> — no database round-trip.
/// </summary>
/// <remarks>
///     The generator defaults to <see cref="SequentialGuid.PostgresOrder" />, so new ids sort after
///     earlier ones the way PostgreSQL compares a <c>uuid</c>. A store that writes the id to SQL Server
///     passes <see cref="SequentialGuid.SqlServerOrder" /> instead, because SQL Server compares a
///     <c>uniqueidentifier</c> in a different byte order.
/// </remarks>
public sealed class SequentialGuidIdentification<TDoc> : IIdentification<TDoc, Guid>
    where TDoc : notnull
{
    private readonly Func<TDoc, Guid> _getter;
    private readonly Action<TDoc, Guid>? _setter;
    private readonly IGuidGenerator _generator;

    /// <summary>
    ///     Generates ids in PostgreSQL <c>uuid</c> order (<see cref="SequentialGuid.PostgresOrder" />).
    /// </summary>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    public SequentialGuidIdentification(MemberInfo idMember) : this(idMember, SequentialGuid.PostgresOrder)
    {
    }

    /// <param name="idMember">The document's <see cref="Guid" /> id member.</param>
    /// <param name="generator">
    ///     Makes the new ids, e.g. <see cref="SequentialGuid.SqlServerOrder" /> for a SQL Server table.
    /// </param>
    [RequiresUnreferencedCode("Builds FEC-compiled accessor delegates over the id member via LambdaBuilder.")]
    public SequentialGuidIdentification(MemberInfo idMember, IGuidGenerator generator)
    {
        ArgumentNullException.ThrowIfNull(generator);
        _getter = LambdaBuilder.Getter<TDoc, Guid>(idMember);
        _setter = LambdaBuilder.Setter<TDoc, Guid>(idMember);
        _generator = generator;
    }

    /// <summary>
    ///     The generator this strategy assigns new ids from.
    /// </summary>
    public IGuidGenerator Generator => _generator;

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

        var assigned = _generator.NewGuid();
        _setter(document, assigned);
        return assigned;
    }
}
