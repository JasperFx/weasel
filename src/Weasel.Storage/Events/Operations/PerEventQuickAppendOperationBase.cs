#nullable enable
using System.Data.Common;
using System.Text;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Whether a <see cref="PerEventQuickAppendOperationBase"/> writes the stream row as a new stream or
/// bumps an existing one's version. Decided by the store's append planner after its version read.
/// </summary>
/// <remarks>
/// Named <c>StreamRowWrite</c> rather than <c>StreamWriteMode</c> on purpose: Polecat and Fisher
/// each have an internal <c>StreamWriteMode</c>, and a public Weasel.Storage type of the same simple
/// name would be CS0104 in every file of theirs that imports both namespaces.
/// </remarks>
public enum StreamRowWrite
{
    /// <summary>INSERT a new stream row.</summary>
    Insert,

    /// <summary>UPDATE the existing stream row's version.</summary>
    Update
}

/// <summary>
/// Dialect-neutral base for the batched Quick append on a store with no array-parameter server
/// function: one self-contained command per stream that writes the stream row, issues one
/// <c>INSERT</c> per event, and ends in exactly one result set carrying each event's assigned
/// <c>seq_id</c> in append order. The shape Polecat (SQL Server) and Fisher (SQLite) each carried a
/// copy of (weasel#743).
/// </summary>
/// <remarks>
/// <para>
/// <b>The optional metadata columns go through <see cref="IEventMetadataBinder"/>s</b>, in the same
/// order as the column list — not through a hand-written <c>if (options.EnableCorrelationId)</c>
/// block per column, which is how each store's copy grew out of step with its own SELECT. Build the
/// column list with <see cref="ComposeColumnList"/> from the same binder array, and the INSERT
/// cannot disagree with itself. A binder whose <see cref="IEventMetadataBinder.ValueSql"/> is a
/// server-side literal rather than <c>?</c> has that literal written in its slot and binds nothing.
/// </para>
/// <para>
/// <b>What a store supplies</b> is exactly what genuinely differs between them: the stream-row SQL
/// (<see cref="WriteInsertStream"/> / <see cref="WriteUpdateStreamVersion"/>, normally the
/// descriptor's closures), the head and tail of each event's INSERT, the core values, and how the
/// sequences come back (<see cref="WriteSequenceReadback"/>) — SQL Server accumulates them with
/// <c>OUTPUT inserted.seq_id INTO @table</c>, SQLite re-selects the rows it just wrote, because
/// <c>RETURNING</c> would produce one result set per INSERT and the batch executor's contract is one
/// per operation.
/// </para>
/// <para>
/// <see cref="PostprocessAsync"/> is the same in both and lives here: one row per event, the
/// sequence at ordinal 0, written onto <see cref="IEvent.Sequence"/> so a failed unit of work can
/// tombstone it.
/// </para>
/// </remarks>
public abstract class PerEventQuickAppendOperationBase: IStorageOperation, IExceptionTransform
{
    private readonly IEventMetadataBinder[] _metadataBinders;

    /// <param name="stream">The stream being appended to.</param>
    /// <param name="metadataBinders">
    /// The active optional metadata columns, in column-list order. Empty when none are enabled.
    /// </param>
    protected PerEventQuickAppendOperationBase(StreamAction stream, IEventMetadataBinder[] metadataBinders)
    {
        Stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _metadataBinders = metadataBinders ?? throw new ArgumentNullException(nameof(metadataBinders));
    }

    /// <summary>The stream being appended to.</summary>
    public StreamAction Stream { get; }

    /// <summary>The active optional metadata binders, in column-list order.</summary>
    public IReadOnlyList<IEventMetadataBinder> MetadataBinders => _metadataBinders;

    /// <summary>Set by the planner: insert a new stream row, or update the existing one's version.</summary>
    public StreamRowWrite Mode { get; set; } = StreamRowWrite.Insert;

    public Type DocumentType => typeof(IEvent);

    public OperationRole Role() => OperationRole.Events;

    /// <summary>
    /// <paramref name="coreColumns"/> followed by each binder's <see cref="IEventMetadataBinder.ColumnName"/>,
    /// comma-separated — the column list matching the values <see cref="ConfigureCommand"/> writes.
    /// </summary>
    public static string ComposeColumnList(string coreColumns, IReadOnlyList<IEventMetadataBinder> metadataBinders)
    {
        ArgumentNullException.ThrowIfNull(coreColumns);
        ArgumentNullException.ThrowIfNull(metadataBinders);

        var sb = new StringBuilder(coreColumns);
        foreach (var binder in metadataBinders)
        {
            sb.Append(", ").Append(binder.ColumnName);
        }

        return sb.ToString();
    }

    public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        WritePreamble(builder, session);

        if (Mode == StreamRowWrite.Insert)
        {
            WriteInsertStream(builder, session);
        }
        else
        {
            WriteUpdateStreamVersion(builder, session);
        }

        var events = Stream.Events;
        for (var i = 0; i < events.Count; i++)
        {
            var @event = events[i];

            WriteEventInsertPrefix(builder, @event, i);
            WriteCoreValues(builder, @event, session);

            for (var b = 0; b < _metadataBinders.Length; b++)
            {
                var binder = _metadataBinders[b];
                builder.Append(", ");

                if (binder.ValueSql == "?")
                {
                    // No separator: the ", " above already placed this binder's slot, and a binder
                    // binds exactly one value.
                    binder.Bind(builder.CreateGroupedParameterBuilder(null), Stream, @event, session);
                }
                else
                {
                    builder.Append(binder.ValueSql);
                }
            }

            WriteEventInsertSuffix(builder, @event, i);
            WriteAfterEvent(builder, @event, i, session);
        }

        WriteSequenceReadback(builder, session);
    }

    /// <summary>
    /// Anything the command needs before the stream row — a SQL Server table variable to collect
    /// sequences into, say. The default writes nothing.
    /// </summary>
    protected virtual void WritePreamble(ICommandBuilder builder, IStorageSession session)
    {
    }

    /// <summary>Write the stream-row INSERT, terminated.</summary>
    protected abstract void WriteInsertStream(ICommandBuilder builder, IStorageSession session);

    /// <summary>Write the stream-row version UPDATE, terminated.</summary>
    protected abstract void WriteUpdateStreamVersion(ICommandBuilder builder, IStorageSession session);

    /// <summary>
    /// Everything up to and including the opening parenthesis of the event's <c>VALUES</c> list —
    /// <c>insert into events (cols) [output ...] values (</c>. Build the column list with
    /// <see cref="ComposeColumnList"/>.
    /// </summary>
    /// <param name="ordinal">The event's zero-based position in the stream's pending events.</param>
    protected abstract void WriteEventInsertPrefix(ICommandBuilder builder, IEvent @event, int ordinal);

    /// <summary>
    /// The core column values, comma-separated, matching the core columns given to
    /// <see cref="ComposeColumnList"/>. Must write at least one value; the metadata binders follow
    /// it, each preceded by a comma.
    /// </summary>
    protected abstract void WriteCoreValues(ICommandBuilder builder, IEvent @event, IStorageSession session);

    /// <summary>
    /// Close the event's <c>VALUES</c> list and terminate the statement. Defaults to <c>);</c>.
    /// </summary>
    protected virtual void WriteEventInsertSuffix(ICommandBuilder builder, IEvent @event, int ordinal)
        => builder.Append(");");

    /// <summary>
    /// Statements that belong to one event after its INSERT — DCB tag writes, for one. The default
    /// writes nothing.
    /// </summary>
    protected virtual void WriteAfterEvent(ICommandBuilder builder, IEvent @event, int ordinal,
        IStorageSession session)
    {
    }

    /// <summary>
    /// The operation's single result set: one row per event, in append order, with the assigned
    /// <c>seq_id</c> at ordinal 0.
    /// </summary>
    protected abstract void WriteSequenceReadback(ICommandBuilder builder, IStorageSession session);

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        var events = Stream.Events;
        var i = 0;

        while (i < events.Count && await reader.ReadAsync(token).ConfigureAwait(false))
        {
            events[i].Sequence = await reader.GetFieldValueAsync<long>(0, token).ConfigureAwait(false);
            i++;
        }
    }

    /// <summary>
    /// Translate a failure. Only a stream-row <see cref="StreamRowWrite.Insert"/> is offered to
    /// <see cref="TransformInsertStreamException"/>, because that is the only write a duplicate
    /// stream id can collide on.
    /// </summary>
    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (Mode == StreamRowWrite.Insert && TransformInsertStreamException(original) is { } result)
        {
            transformed = result;
            return true;
        }

        transformed = null;
        return false;
    }

    /// <summary>
    /// Map the provider's primary-key violation on the stream INSERT to the store's
    /// stream-collision exception, or return null to leave the failure alone. The default leaves it.
    /// </summary>
    protected virtual Exception? TransformInsertStreamException(Exception original) => null;

    public override string ToString()
        => $"Quick append of {Stream.Events.Count} event(s) to stream {Stream.Key ?? Stream.Id.ToString()}";
}
