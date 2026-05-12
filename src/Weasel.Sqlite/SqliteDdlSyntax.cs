using Weasel.Core;

namespace Weasel.Sqlite;

/// <summary>
///     SQLite implementation of <see cref="IDdlSyntaxStrategy" />.
///     <para>
///     Wired through <c>Tables.Table.WriteDropStatement</c> and the CREATE-TABLE
///     header in <c>WriteCreateStatement</c> as part of #270 step 8 (prototype).
///     The SQLite side is the more interesting half of the validation: it has
///     <see cref="InlineForeignKeyConstraints" /> = true (no ALTER TABLE for FKs)
///     and uses a different auto-increment spelling, exercising the parts of the
///     strategy that genuinely have to be different from PostgreSQL.
///     </para>
/// </summary>
public sealed class SqliteDdlSyntax: IDdlSyntaxStrategy
{
    /// <summary>Process-wide singleton — strategy is stateless.</summary>
    public static readonly SqliteDdlSyntax Instance = new();

    private SqliteDdlSyntax() { }

    /// <inheritdoc />
    public string QuoteIdentifier(string name) => SchemaUtils.QuoteName(name);

    /// <inheritdoc />
    /// <remarks>
    ///     SQLite has no CASCADE on DROP TABLE — dependent objects must be
    ///     dropped manually if they exist (in practice they don't, because
    ///     SQLite views can't reference tables in attached databases and FKs
    ///     are intra-table).
    /// </remarks>
    public void WriteDropTable(TextWriter writer, DbObjectName identifier)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {identifier};");
    }

    /// <inheritdoc />
    public void WriteCreateTableHeader(TextWriter writer, DbObjectName identifier, CreationStyle style)
    {
        if (style == CreationStyle.DropThenCreate)
        {
            writer.WriteLine($"CREATE TABLE {identifier} (");
        }
        else
        {
            writer.WriteLine($"CREATE TABLE IF NOT EXISTS {identifier} (");
        }
    }

    /// <inheritdoc />
    /// <remarks>
    ///     SQLite requires inline <c>CONSTRAINT … FOREIGN KEY (…) REFERENCES …</c>
    ///     clauses inside the CREATE TABLE body. It has no <c>ALTER TABLE ADD
    ///     CONSTRAINT</c> for foreign keys (and limited ALTER TABLE generally).
    /// </remarks>
    public bool InlineForeignKeyConstraints => true;

    /// <inheritdoc />
    /// <remarks>
    ///     SQLite's <c>AUTOINCREMENT</c> applies only to <c>INTEGER PRIMARY KEY</c>
    ///     columns and forces the rowid allocator to never reuse values. For
    ///     simple auto-increment without that guarantee, <c>INTEGER PRIMARY KEY</c>
    ///     alone is sufficient — but the strategy returns the explicit keyword
    ///     since that's what users opt in to via the fluent
    ///     <c>.AutoIncrement()</c> call.
    /// </remarks>
    public string AutoIncrementToken => "AUTOINCREMENT";

    /// <inheritdoc />
    public string StatementTerminator => ";";
}
