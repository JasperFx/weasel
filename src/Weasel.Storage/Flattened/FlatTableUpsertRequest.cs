using Weasel.Core;

namespace Weasel.Storage.Flattened;

/// <summary>
///     Everything a dialect needs to render the upsert for one event type's column mappings.
/// </summary>
/// <remarks>
///     The whole <see cref="ITable" /> is carried rather than just its identifier because a
///     function-emitting dialect has to declare an argument per input column, and an argument
///     declaration needs the column's declared type.
/// </remarks>
public sealed class FlatTableUpsertRequest
{
    public FlatTableUpsertRequest(ITable table, Type eventType, string primaryKeyColumn,
        IReadOnlyList<IColumnMap> columns)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        EventType = eventType ?? throw new ArgumentNullException(nameof(eventType));
        PrimaryKeyColumn = primaryKeyColumn ?? throw new ArgumentNullException(nameof(primaryKeyColumn));
        Columns = columns ?? throw new ArgumentNullException(nameof(columns));
    }

    /// <summary>The flat table being written.</summary>
    public ITable Table { get; }

    /// <summary>
    ///     The event type these mappings were declared for. Carried because a dialect that generates
    ///     one upsert function per event type has to name it after something, and the table alone is
    ///     shared by every event type the projection handles.
    /// </summary>
    public Type EventType { get; }

    /// <summary>Shorthand for <c>Table.Identifier</c>.</summary>
    public DbObjectName Identifier => Table.Identifier;

    /// <summary>
    ///     The single column the row is keyed on. A flat table's key is the stream (or an explicitly
    ///     named member of the event), which is one value; composite keys are out of scope because
    ///     the runtime binds exactly one key parameter.
    /// </summary>
    public string PrimaryKeyColumn { get; }

    /// <summary>The column mappings declared for this event type, in declaration order.</summary>
    public IReadOnlyList<IColumnMap> Columns { get; }

    /// <summary>
    ///     How many of the mappings read a value off the event, and therefore take a parameter.
    /// </summary>
    public int InputCount
    {
        get
        {
            var count = 0;
            for (var i = 0; i < Columns.Count; i++)
            {
                if (Columns[i].RequiresInput)
                {
                    count++;
                }
            }

            return count;
        }
    }
}

/// <summary>
///     What a dialect produces for an upsert: the statement the runtime executes, plus any schema
///     objects that statement depends on.
/// </summary>
/// <remarks>
///     <see cref="SchemaObjects" /> is empty for an inline form (<c>MERGE</c>,
///     <c>INSERT … ON CONFLICT</c>) and carries the generated function for a dialect that emits one.
///     That is the whole of what makes the function shape expressible without every caller having to
///     know which shape it is dealing with — the caller executes <see cref="Sql" /> and contributes
///     <see cref="SchemaObjects" /> to its migration.
/// </remarks>
public sealed class FlatTableUpsert
{
    private static readonly ISchemaObject[] None = [];

    public FlatTableUpsert(string sql, IReadOnlyList<ISchemaObject> schemaObjects)
    {
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        SchemaObjects = schemaObjects ?? throw new ArgumentNullException(nameof(schemaObjects));
    }

    /// <summary>The statement the projection executes per matching event.</summary>
    public string Sql { get; }

    /// <summary>Schema objects the statement depends on; empty for an inline upsert.</summary>
    public IReadOnlyList<ISchemaObject> SchemaObjects { get; }

    /// <summary>An inline upsert that depends on nothing but the table itself.</summary>
    public static FlatTableUpsert Statement(string sql) => new(sql, None);

    /// <summary>An upsert whose statement calls generated schema objects.</summary>
    public static FlatTableUpsert CallingInto(string sql, params ISchemaObject[] schemaObjects) =>
        new(sql, schemaObjects);
}

/// <summary>
///     The rendered pieces of an inline upsert, composed once by
///     <see cref="FlatTableSqlDialectBase" /> so that a <c>MERGE</c> dialect and an
///     <c>ON CONFLICT</c> dialect differ only in how they are strung together.
/// </summary>
public sealed class FlatTableUpsertClauses
{
    public FlatTableUpsertClauses(
        string quotedPrimaryKey,
        string primaryKeyParameter,
        IReadOnlyList<string> insertColumns,
        IReadOnlyList<string> insertValues,
        IReadOnlyList<string> updateAssignments)
    {
        QuotedPrimaryKey = quotedPrimaryKey;
        PrimaryKeyParameter = primaryKeyParameter;
        InsertColumns = insertColumns;
        InsertValues = insertValues;
        UpdateAssignments = updateAssignments;
    }

    /// <summary>The key column, quoted.</summary>
    public string QuotedPrimaryKey { get; }

    /// <summary>The placeholder the key value binds to — parameter 0.</summary>
    public string PrimaryKeyParameter { get; }

    /// <summary>The insert column list, key first, each quoted.</summary>
    public IReadOnlyList<string> InsertColumns { get; }

    /// <summary>The insert value expressions, aligned with <see cref="InsertColumns" />.</summary>
    public IReadOnlyList<string> InsertValues { get; }

    /// <summary>
    ///     The update-branch assignments. The key is deliberately absent — it is what the row was
    ///     matched on, so assigning it would be a no-op at best.
    /// </summary>
    public IReadOnlyList<string> UpdateAssignments { get; }

    /// <summary>The insert column list as comma-separated SQL.</summary>
    public string InsertColumnList => string.Join(", ", InsertColumns);

    /// <summary>The insert value list as comma-separated SQL.</summary>
    public string InsertValueList => string.Join(", ", InsertValues);

    /// <summary>The update assignments as comma-separated SQL.</summary>
    public string UpdateAssignmentList => string.Join(", ", UpdateAssignments);
}
