using System.Globalization;
using Weasel.Postgresql;
using Weasel.Postgresql.Functions;
using Weasel.Sqlite;
using Weasel.Storage.Flattened;

namespace Weasel.Core.Tests.Flattened;

/// <summary>
///     One reference <see cref="IFlatTableSqlDialect" /> per upsert form the Critter Stack stores
///     actually emit, reproducing the SQL each store's own flat-table code produces today.
/// </summary>
/// <remarks>
///     <para>
///         These live in the test project on purpose. <c>Weasel.Storage</c> references no provider
///         assembly, and each store already owns the rest of its flat-table plumbing — so shipping
///         the dialects here is what proves the seam can express all three shapes without
///         prejudging where a store chooses to put its own implementation.
///     </para>
///     <para>
///         The SQL Server one is Polecat's <c>MERGE</c>, the SQLite one Fisher's
///         <c>INSERT … ON CONFLICT DO UPDATE</c>, and the PostgreSQL one Marten's generated upsert
///         <em>function</em> — the shape that does not fit an inline statement at all, and therefore
///         the one that decides whether the abstraction is wide enough.
///     </para>
/// </remarks>
internal static class ReferenceFlatTableDialects
{
    public static readonly SqlServerFlatTableDialect SqlServer = new();
    public static readonly SqliteFlatTableDialect Sqlite = new();
    public static readonly PostgresqlFlatTableDialect Postgresql = new();
}

/// <summary>Polecat's shape: a <c>MERGE</c> with the pre-update row reached through <c>target</c>.</summary>
internal sealed class SqlServerFlatTableDialect: InlineFlatTableSqlDialect
{
    /// <remarks>
    ///     Weasel.SqlServer ships no <see cref="IDdlSyntaxStrategy" /> yet, so the bracket rule is
    ///     spelled out here. Doubling an embedded <c>]</c> is not optional — it would otherwise close
    ///     the bracket early (polecat#390 / weasel#416).
    /// </remarks>
    public override string QuoteIdentifier(string name) => string.Concat("[", name.Replace("]", "]]"), "]");

    public override string ExistingRowReference(DbObjectName table, string columnName)
        => $"target.{QuoteIdentifier(columnName)}";

    protected override FlatTableUpsert WriteUpsert(FlatTableUpsertRequest request, FlatTableUpsertClauses clauses)
    {
        // UPDLOCK/HOLDLOCK: the daemon applies slices concurrently, so two slices touching the same
        // row race the probe and both insert on the primary key without it (polecat#500).
        var sql = $"""
                   MERGE {TableReference(request.Identifier)} WITH (UPDLOCK, HOLDLOCK) AS target
                   USING (SELECT {clauses.PrimaryKeyParameter} AS {clauses.QuotedPrimaryKey}) AS source ON target.{clauses.QuotedPrimaryKey} = source.{clauses.QuotedPrimaryKey}
                   WHEN MATCHED THEN UPDATE SET {clauses.UpdateAssignmentList}
                   WHEN NOT MATCHED THEN INSERT ({clauses.InsertColumnList}) VALUES ({clauses.InsertValueList});
                   """;

        return FlatTableUpsert.Statement(sql);
    }

    public override string BuildDelete(DbObjectName table, string primaryKeyColumn)
        => $"DELETE FROM {TableReference(table)} WHERE {QuoteIdentifier(primaryKeyColumn)} = {ParameterName(0)};";
}

/// <summary>
///     Fisher's shape: one <c>INSERT … ON CONFLICT DO UPDATE</c>, where an unqualified column on the
///     right of an assignment <em>is</em> the pre-update row.
/// </summary>
internal sealed class SqliteFlatTableDialect: InlineFlatTableSqlDialect
{
    public SqliteFlatTableDialect(): base(SqliteDdlSyntax.Instance)
    {
    }

    /// <remarks>SQLite has no schemas; the logical schema is folded into the table name instead.</remarks>
    public override string TableReference(DbObjectName table) => QuoteIdentifier(table.Name);

    /// <remarks>
    ///     Bare, not <c>excluded."c"</c> — that would be the value the insert branch would have
    ///     written, which turns every increment into a self-assignment.
    /// </remarks>
    public override string ExistingRowReference(DbObjectName table, string columnName)
        => QuoteIdentifier(columnName);

    protected override FlatTableUpsert WriteUpsert(FlatTableUpsertRequest request, FlatTableUpsertClauses clauses)
    {
        var sql = $"""
                   insert into {TableReference(request.Identifier)} ({clauses.InsertColumnList})
                   values ({clauses.InsertValueList})
                   on conflict ({clauses.QuotedPrimaryKey}) do update set {clauses.UpdateAssignmentList};
                   """;

        return FlatTableUpsert.Statement(sql);
    }
}

/// <summary>
///     Marten's shape: a generated <c>plpgsql</c> upsert function per event type, called by the
///     runtime statement.
/// </summary>
/// <remarks>
///     The one form that is not a single inline statement, so it derives from the bare
///     <see cref="FlatTableSqlDialectBase" /> rather than from
///     <see cref="InlineFlatTableSqlDialect" /> and returns the function alongside the call. Its
///     parameters are the function's own named arguments rather than positional placeholders, which
///     is why the inline clause composition does not fit.
/// </remarks>
internal sealed class PostgresqlFlatTableDialect: FlatTableSqlDialectBase
{
    public PostgresqlFlatTableDialect(): base(PostgresqlDdlSyntax.Instance)
    {
    }

    /// <remarks>
    ///     Inside <c>ON CONFLICT DO UPDATE</c>, PostgreSQL names the pre-update row by the table's own
    ///     name.
    /// </remarks>
    public override string ExistingRowReference(DbObjectName table, string columnName)
        => $"{QuoteIdentifier(table.Name)}.{QuoteIdentifier(columnName)}";

    /// <remarks>Weasel's command builders translate <c>?</c> into the provider's placeholder.</remarks>
    public override string ParameterName(int index) => "?";

    public override FlatTableUpsert BuildUpsert(FlatTableUpsertRequest request)
    {
        var context = new FlatTableColumnContext(this, request.Identifier);

        var arguments = new List<string> { ArgumentDeclaration(request, request.PrimaryKeyColumn) };
        var insertColumns = new List<string> { QuoteIdentifier(request.PrimaryKeyColumn) };
        var insertValues = new List<string> { ArgumentName(request.PrimaryKeyColumn) };
        var updates = new List<string>();

        foreach (var map in request.Columns)
        {
            var argument = string.Empty;

            if (map.RequiresInput)
            {
                argument = ArgumentName(map.ColumnName);
                arguments.Add(ArgumentDeclaration(request, map.ColumnName));
            }

            insertColumns.Add(QuoteIdentifier(map.ColumnName));
            insertValues.Add(map.InsertExpression(context, argument));
            updates.Add(map.UpdateExpression(context, argument));
        }

        var identifier = FunctionName(request);

        var body = $"""
                    CREATE OR REPLACE FUNCTION {identifier.QualifiedName}({string.Join(", ", arguments)}) RETURNS void LANGUAGE plpgsql
                    AS $function$
                    BEGIN
                    INSERT INTO {TableReference(request.Identifier)} ({string.Join(", ", insertColumns)}) VALUES ({string.Join(", ", insertValues)})
                      ON CONFLICT ON CONSTRAINT {request.Table.PrimaryKeyName}
                      DO UPDATE SET {string.Join(", ", updates)};
                    END;
                    $function$;
                    """;

        var call =
            $"select {identifier.QualifiedName}({string.Join(", ", Enumerable.Repeat("?", arguments.Count))})";

        return FlatTableUpsert.CallingInto(call, new Function(identifier, body));
    }

    private static DbObjectName FunctionName(FlatTableUpsertRequest request)
        => new PostgresqlObjectName(request.Identifier.Schema,
            $"mt_upsert_{request.Identifier.Name.ToLower(CultureInfo.InvariantCulture)}_"
            + $"{request.EventType.Name.ToLower(CultureInfo.InvariantCulture)}");

    private static string ArgumentName(string columnName) => "p_" + columnName;

    private string ArgumentDeclaration(FlatTableUpsertRequest request, string columnName)
    {
        var column = request.Table.Columns.FirstOrDefault(x =>
            string.Equals(x.Name, columnName, StringComparison.OrdinalIgnoreCase));

        return $"{ArgumentName(columnName)} {column?.Type ?? "text"}";
    }
}
