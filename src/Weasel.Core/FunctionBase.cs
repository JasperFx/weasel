using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Shared base for provider-specific <c>Function</c> classes (PostgreSQL, SQL Server).
///     Owns the cross-provider state (body, optional pre-built drop statements, removal flag)
///     and the standard <see cref="ISchemaObject" /> boilerplate that both providers were
///     reimplementing nearly verbatim: <see cref="AllNames" />,
///     <see cref="WriteDropStatement" /> (delegating to <see cref="DropStatements" />),
///     <see cref="Body" />, and a base <see cref="DropStatements" /> rule.
/// </summary>
public abstract class FunctionBase: SchemaObjectBase
{
    private readonly string[]? _dropStatements;

    protected FunctionBase(DbObjectName identifier, string? body) : base(identifier)
    {
        RawBody = body;
    }

    protected FunctionBase(DbObjectName identifier, string body, string[] dropStatements) : base(identifier)
    {
        RawBody = body;
        _dropStatements = dropStatements;
    }

    /// <summary>
    ///     The raw body string the function was constructed with. Subclasses route this
    ///     through their provider-specific <see cref="ISchemaObject.WriteCreateStatement" />
    ///     (PostgreSQL writes it directly; SQL Server wraps it in <c>EXEC sp_executesql</c>).
    /// </summary>
    protected string? RawBody { get; }

    /// <summary>
    ///     True when the function has been marked for removal via the provider's
    ///     <c>ForRemoval(...)</c> factory. A removed function emits no
    ///     <c>WriteCreateStatement</c> output and its <see cref="DropStatements" /> is empty
    ///     (the delta is expected to call <c>WriteDropStatement</c> on the actual function).
    /// </summary>
    public bool IsRemoved { get; protected set; }

    /// <summary>
    ///     Returns the body of the function as the create-statement text would emit it.
    ///     Both PostgreSQL and SQL Server implementations build this by routing through
    ///     <see cref="ISchemaObject.WriteCreateStatement" />.
    /// </summary>
    public string Body(Migrator? rules = null)
    {
        rules ??= GetDefaultMigrator();
        var writer = new StringWriter();
        WriteCreateStatement(rules, writer);
        return writer.ToString();
    }

    /// <summary>
    ///     Subclasses supply a provider-appropriate default <see cref="Migrator" /> for the
    ///     parameterless <see cref="Body" /> call. Mainly used by tests and by the
    ///     <c>FunctionDeltaBase</c> diff path.
    /// </summary>
    protected abstract Migrator GetDefaultMigrator();

    /// <summary>
    ///     Returns the DROP statements that will be emitted by
    ///     <see cref="WriteDropStatement" />. Honors a pre-built array when the function
    ///     was constructed with one (e.g. fetched from the database with the drop SQL
    ///     supplied by the catalog query); returns an empty array when the function is
    ///     marked removed; otherwise delegates to <see cref="ComputeDefaultDropStatements" />
    ///     for the provider-specific shape (e.g. PostgreSQL needs the function signature,
    ///     SQL Server can use the identifier directly).
    /// </summary>
    public virtual string[] DropStatements()
    {
        if (_dropStatements?.Length > 0)
        {
            return _dropStatements;
        }

        if (IsRemoved)
        {
            return Array.Empty<string>();
        }

        return ComputeDefaultDropStatements();
    }

    /// <summary>
    ///     Build the DROP statement(s) for a function that wasn't given an explicit
    ///     drop list at construction time. Subclasses override with provider-specific
    ///     SQL (PostgreSQL parses out the function signature for the OID-disambiguated
    ///     drop; SQL Server uses the qualified identifier directly).
    /// </summary>
    protected abstract string[] ComputeDefaultDropStatements();

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        foreach (var dropStatement in DropStatements())
        {
            writer.WriteLine(dropStatement);
        }
    }

    /// <summary>
    ///     Function-specific delta production: emits a <see cref="FunctionDeltaBase" />-typed
    ///     delta (the provider's concrete <c>FunctionDelta</c>) by first reading the actual
    ///     function from the result set. Default behaviour treats absence as Create and
    ///     compares bodies for Update; subclasses can override
    ///     <see cref="ReadExistingFromReaderAsync" /> for the provider-specific catalog query
    ///     shape.
    /// </summary>
    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(
        DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        return CreateFunctionDelta(existing);
    }

    /// <summary>
    ///     Read the actual function from the catalog query result and return it (or null
    ///     if it does not exist). Subclass implementation depends entirely on the shape of
    ///     the <see cref="ISchemaObject.ConfigureQueryCommand" /> output (PostgreSQL emits
    ///     two result sets; SQL Server emits one).
    /// </summary>
    protected abstract Task<FunctionBase?> ReadExistingFromReaderAsync(
        DbDataReader reader, CancellationToken ct);

    /// <summary>
    ///     Subclasses wrap the actual function in their provider-specific
    ///     <c>FunctionDelta</c> type (which inherits from <see cref="SchemaObjectDelta{T}" />
    ///     of the concrete function type).
    /// </summary>
    protected abstract ISchemaObjectDelta CreateFunctionDelta(FunctionBase? actual);
}
