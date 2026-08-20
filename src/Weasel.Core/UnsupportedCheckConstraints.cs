using System.Collections;

namespace Weasel.Core;

/// <summary>
///     The <see cref="TableBase{TColumn,TIndex,TForeignKey}.CheckConstraints" /> collection for a
///     provider that will not emit them: it refuses everything added to it and stays empty.
/// </summary>
/// <remarks>
///     <para>
///         Check constraints live on <c>TableBase</c>, so every provider's <c>Table</c> accepted
///         one — and only PostgreSQL and SQL Server ever wrote it into the DDL. On the other three
///         the constraint sat in the model, never reached <c>CREATE TABLE</c>, and was never
///         compared during delta detection either, so nothing reported the difference. A caller got
///         a table without the constraint they asked for and no way to notice (weasel#488).
///     </para>
///     <para>
///         Refusing is the rule weasel#449 settled for the index predicates that did the same
///         thing: a caller who sets a property gets it, or gets an exception, never a quietly
///         narrower object. The collection refuses rather than only
///         <see cref="ITable.AddCheckConstraint" /> throwing, because
///         <see cref="TableBase{TColumn,TIndex,TForeignKey}.CheckConstraints" /> is a public
///         <see cref="IList{T}" /> and adding to it directly is the more common spelling.
///     </para>
/// </remarks>
internal sealed class UnsupportedCheckConstraints: IList<TableCheckConstraint>
{
    private readonly string _provider;

    public UnsupportedCheckConstraints(string provider)
    {
        _provider = provider;
    }

    private NotSupportedException Refuse(TableCheckConstraint? constraint = null)
    {
        var named = constraint == null ? "a check constraint" : $"check constraint '{constraint.Name}'";

        return new NotSupportedException(
            $"Weasel does not emit check constraints on {_provider}, so {named} would be accepted and "
            + "then silently left out of the generated DDL. The engine supports them — Weasel does not "
            + "model them here yet; see weasel#488. Until it does, declare the constraint in SQL you run "
            + "yourself, or move the rule into a trigger.");
    }

    public void Add(TableCheckConstraint item) => throw Refuse(item);

    public void Insert(int index, TableCheckConstraint item) => throw Refuse(item);

    public TableCheckConstraint this[int index]
    {
        get => throw new ArgumentOutOfRangeException(nameof(index));
        set => throw Refuse(value);
    }

    // Everything below is the behaviour of an empty, immutable list. Nothing can have been added,
    // so nothing can be found, removed or enumerated.

    public int Count => 0;
    public bool IsReadOnly => true;

    public void Clear()
    {
    }

    public bool Contains(TableCheckConstraint item) => false;

    public void CopyTo(TableCheckConstraint[] array, int arrayIndex)
    {
    }

    public bool Remove(TableCheckConstraint item) => false;

    public void RemoveAt(int index) => throw new ArgumentOutOfRangeException(nameof(index));

    public int IndexOf(TableCheckConstraint item) => -1;

    public IEnumerator<TableCheckConstraint> GetEnumerator()
        => Enumerable.Empty<TableCheckConstraint>().GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
