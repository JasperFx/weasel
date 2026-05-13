namespace Weasel.Core;

/// <summary>
///     Shared base for provider-specific <c>View</c> classes (PostgreSQL, SQLite).
///     Owns the cross-provider state (the view's SELECT body) and the standard
///     boilerplate both providers reimplement: <see cref="MoveToSchema" /> (with a virtual
///     <see cref="WithSchema" /> hook for the provider-specific <c>DbObjectName</c>
///     wrapping), <see cref="ToBasicCreateViewSql" />, and the "DROP first, then CREATE"
///     template for <see cref="ISchemaObject.WriteCreateStatement" />.
/// </summary>
public abstract class ViewBase: SchemaObjectBase
{
    protected ViewBase(DbObjectName identifier, string viewSql) : base(identifier)
    {
        ViewSql = viewSql ?? throw new ArgumentNullException(nameof(viewSql));
    }

    /// <summary>
    ///     The SELECT statement (without <c>CREATE VIEW … AS</c> prefix) that defines this
    ///     view's contents. Exposed publicly so delta classes can normalize/compare it.
    /// </summary>
    public string ViewSql { get; }

    /// <summary>
    ///     Move this view to a different schema. Subclasses provide
    ///     <see cref="WithSchema" /> to wrap the new identifier in their provider-specific
    ///     <see cref="DbObjectName" /> subclass (PostgreSQL's <c>PostgresqlObjectName</c>,
    ///     SQLite's <c>SqliteObjectName</c>).
    /// </summary>
    public void MoveToSchema(string schemaName)
    {
        Identifier = WithSchema(schemaName);
    }

    /// <summary>
    ///     Construct a new identifier for this view in the named schema, wrapped in the
    ///     provider-specific <see cref="DbObjectName" /> subclass.
    /// </summary>
    protected abstract DbObjectName WithSchema(string schemaName);

    /// <summary>
    ///     Generate the CREATE VIEW SQL with the provider's default formatting rules
    ///     ("concise"). Useful for diagnostics.
    /// </summary>
    public string ToBasicCreateViewSql()
    {
        var writer = new StringWriter();
        var migrator = GetDefaultMigratorForBasicSql();
        WriteCreateStatement(migrator, writer);
        return writer.ToString();
    }

    /// <summary>
    ///     Subclasses supply a provider-appropriate <see cref="Migrator" /> for the
    ///     no-argument <see cref="ToBasicCreateViewSql" /> helper.
    /// </summary>
    protected abstract Migrator GetDefaultMigratorForBasicSql();
}
