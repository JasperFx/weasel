using System.Data.Common;
using System.Globalization;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Tables;

public partial class Table
{
    /// <summary>
    ///     The columns, in position order, with everything the model can state about each: the type as
    ///     <c>RDB$FIELDS</c> stores it, nullability and collation coalesced from the column and its domain
    ///     -- a domain's <c>NOT NULL</c> and a column's <c>COLLATE</c> are only recorded on the domain --
    ///     the default source cast to text, whether it is an identity, and a computed column's expression.
    ///     The engine version rides along on every row, for the one type whose storage differs by version.
    /// </summary>
    private const string ColumnSql = """
        SELECT
            TRIM(rf.RDB$FIELD_NAME),
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            f.RDB$CHARACTER_LENGTH,
            COALESCE(rf.RDB$NULL_FLAG, f.RDB$NULL_FLAG, 0),
            CAST(COALESCE(rf.RDB$DEFAULT_SOURCE, f.RDB$DEFAULT_SOURCE) AS VARCHAR(8191)),
            TRIM(rf.RDB$GENERATOR_NAME),
            TRIM(cs.RDB$CHARACTER_SET_NAME),
            COALESCE(rf.RDB$COLLATION_ID, f.RDB$COLLATION_ID, 0),
            TRIM(co.RDB$COLLATION_NAME),
            rdb$get_context('SYSTEM', 'ENGINE_VERSION'),
            CAST(f.RDB$COMPUTED_SOURCE AS VARCHAR(8191))
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = rf.RDB$RELATION_NAME AND r.RDB$VIEW_BLR IS NULL
        JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
        LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
        LEFT JOIN RDB$COLLATIONS co ON co.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
            AND co.RDB$COLLATION_ID = COALESCE(rf.RDB$COLLATION_ID, f.RDB$COLLATION_ID, 0)
        WHERE rf.RDB$RELATION_NAME = @table
        ORDER BY rf.RDB$FIELD_POSITION
        """;

    /// <summary>
    ///     The primary key and every unique constraint, one row per column, the key first. The model has
    ///     no unique constraints -- a unique index is how it says "unique" -- but Firebird will not change
    ///     the type of a column one covers, so the delta has to know which columns those are.
    /// </summary>
    private const string KeySql = """
        SELECT TRIM(rc.RDB$CONSTRAINT_NAME), TRIM(s.RDB$FIELD_NAME), TRIM(rc.RDB$CONSTRAINT_TYPE)
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
        WHERE rc.RDB$RELATION_NAME = @table AND rc.RDB$CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY rc.RDB$CONSTRAINT_TYPE, rc.RDB$CONSTRAINT_NAME, s.RDB$FIELD_POSITION
        """;

    /// <summary>
    ///     Foreign keys, one row per column pair, through <c>RDB$REF_CONSTRAINTS</c> to the key they
    ///     reference, and that key's segment at the same position.
    /// </summary>
    private const string ForeignKeySql = """
        SELECT
            TRIM(rc.RDB$CONSTRAINT_NAME),
            TRIM(pk.RDB$RELATION_NAME),
            TRIM(s.RDB$FIELD_NAME),
            TRIM(ps.RDB$FIELD_NAME),
            TRIM(ref.RDB$DELETE_RULE),
            TRIM(ref.RDB$UPDATE_RULE)
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$REF_CONSTRAINTS ref ON ref.RDB$CONSTRAINT_NAME = rc.RDB$CONSTRAINT_NAME
        JOIN RDB$RELATION_CONSTRAINTS pk ON pk.RDB$CONSTRAINT_NAME = ref.RDB$CONST_NAME_UQ
        JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
        JOIN RDB$INDEX_SEGMENTS ps ON ps.RDB$INDEX_NAME = pk.RDB$INDEX_NAME AND ps.RDB$FIELD_POSITION = s.RDB$FIELD_POSITION
        WHERE rc.RDB$RELATION_NAME = @table AND rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
        ORDER BY rc.RDB$CONSTRAINT_NAME, s.RDB$FIELD_POSITION
        """;

    /// <summary>
    ///     The indexes that back no constraint, one row per segment -- an expression index has none, so
    ///     the join is outer. A primary key, unique constraint or foreign key creates an index of its
    ///     own, and comparing those as ordinary indexes would report every one of them as an extra to
    ///     drop (weasel#445).
    /// </summary>
    private const string IndexSql = """
        SELECT
            TRIM(i.RDB$INDEX_NAME),
            COALESCE(i.RDB$UNIQUE_FLAG, 0),
            COALESCE(i.RDB$INDEX_TYPE, 0),
            COALESCE(i.RDB$INDEX_INACTIVE, 0),
            CAST(i.RDB$EXPRESSION_SOURCE AS VARCHAR(8191)),
            {0},
            TRIM(s.RDB$FIELD_NAME)
        FROM RDB$INDICES i
        LEFT JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = i.RDB$INDEX_NAME
        WHERE i.RDB$RELATION_NAME = @table
          AND COALESCE(i.RDB$SYSTEM_FLAG, 0) = 0
          AND NOT EXISTS (SELECT 1 FROM RDB$RELATION_CONSTRAINTS rc WHERE rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME)
        ORDER BY i.RDB$INDEX_NAME, s.RDB$FIELD_POSITION
        """;

    /// <summary>
    ///     The version of the server this table was read from, when it was read from one.
    /// </summary>
    internal FirebirdServerVersion? ServerVersion { get; private set; }

    /// <summary>
    ///     Firebird executes one statement per command, so the four queries go through
    ///     <see cref="FirebirdDbCommandBuilder" />, which splits them.
    /// </summary>
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     Register the four introspection queries -- columns, keys, foreign keys, indexes -- as
    ///     four statements separated by <see cref="CommandBuilderBase{TCommand,TParameter,TParameterType}.StartNewCommand" />.
    ///     They stay unterminated: each is a command of its own.
    /// </summary>
    /// <remarks>
    ///     A partial index's condition is in <c>RDB$INDICES.RDB$CONDITION_SOURCE</c>, which only Firebird 5
    ///     has, so the index query names it only when the builder was made against an open connection to
    ///     a Firebird 5 server. Against anything else it reads as no condition.
    ///     <para>
    ///         A name longer than the server's catalog holds is refused as over the limit, before it is
    ///         bound: the query would otherwise fail with "string truncation".
    ///     </para>
    /// </remarks>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var version = builder is FirebirdDbCommandBuilder firebird
            ? firebird.ServerVersion
            : FirebirdServerVersion.Of(builder.Command.Connection);
        FirebirdMigrator.AssertCatalogCanHold(CatalogName, version, "the name of a table");

        var table = builder.AddParameter(CatalogName).ParameterName;
        string bind(string sql) => sql.Replace("@table", "@" + table);

        builder.Append(bind(ColumnSql));
        builder.StartNewCommand();

        builder.Append(bind(KeySql));
        builder.StartNewCommand();

        builder.Append(bind(ForeignKeySql));
        builder.StartNewCommand();

        var condition = version is { SupportsPartialIndexes: true }
            ? "CAST(i.RDB$CONDITION_SOURCE AS VARCHAR(8191))"
            : "CAST(NULL AS VARCHAR(8191))";

        builder.Append(bind(string.Format(CultureInfo.InvariantCulture, IndexSql, condition)));
    }

    /// <summary>
    ///     Read the four result sets <see cref="ConfigureQueryCommand" /> registers, in order.
    /// </summary>
    internal async Task<Table?> ReadExistingFromReaderAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = new Table(Identifier) { PreserveIdentifierCase = PreserveIdentifierCase };

        await readColumnsAsync(reader, existing, ct).ConfigureAwait(false);

        if (!existing.Columns.Any())
        {
            // The table does not exist. The remaining result sets are still there and still have to
            // be walked, or the next schema object in the batch reads this table's rows.
            for (var i = 0; i < 3; i++)
            {
                await reader.NextResultAsync(ct).ConfigureAwait(false);
            }

            return null;
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readKeysAsync(reader, existing, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readForeignKeysAsync(reader, existing, ct).ConfigureAwait(false);

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        await readIndexesAsync(reader, existing, ct).ConfigureAwait(false);

        return existing;
    }

    /// <summary>
    ///     Read this table from the database, through the same queries and readers the migration path
    ///     uses.
    /// </summary>
    public async Task<Table?> FetchExistingAsync(FbConnection conn, CancellationToken ct = default)
    {
        var builder = new FirebirdDbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        return await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
    }

    private static async Task readColumnsAsync(DbDataReader reader, Table existing, CancellationToken ct)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);

            var type = FirebirdColumnType.FromCatalog(
                int32(reader, 1)!.Value,
                int32(reader, 2),
                int32(reader, 3),
                int32(reader, 4),
                int32(reader, 5),
                text(reader, 9),
                int32(reader, 10) is > 0 ? text(reader, 11) : null);

            var identity = text(reader, 8) != null;
            var column = new TableColumn(name, type.ToString())
            {
                AllowNulls = int32(reader, 6) != 1,
                DefaultExpression = TableColumn.ReadDefault(text(reader, 7)),
                IsAutoNumber = identity,
                ComputedExpression = ExpressionText.WithoutOuterParentheses(text(reader, 13))
            };

            existing.AddColumn(column);

            existing.ServerVersion ??= FirebirdServerVersion.TryParse(text(reader, 12));
        }
    }

    private static async Task readKeysAsync(DbDataReader reader, Table existing, CancellationToken ct)
    {
        string? name = null;
        var columns = new List<string>();

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (reader.GetString(2) == "UNIQUE")
            {
                existing.UniqueConstraintColumns.Add(reader.GetString(1));
                continue;
            }

            name = reader.GetString(0);
            columns.Add(reader.GetString(1));
        }

        foreach (var columnName in columns)
        {
            var column = existing.ColumnFor(columnName);
            if (column != null)
            {
                column.IsPrimaryKey = true;
            }
        }

        // The catalog orders by segment position, so this is the DECLARED key order, which for a
        // composite key need not match the order the columns appear in the table. Comparison stays
        // order-insensitive unless the model pins an order.
        existing.SetPrimaryKeyOrder(columns);

        if (name != null)
        {
            existing.PrimaryKeyName = name;
        }
    }

    private static async Task readForeignKeysAsync(DbDataReader reader, Table existing, CancellationToken ct)
    {
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var fk = existing.FindOrCreateForeignKey(reader.GetString(0));
            fk.LinkedTable = new FirebirdObjectName(reader.GetString(1));
            fk.LinkColumns(reader.GetString(2), reader.GetString(3));
            fk.ReadReferentialActions(text(reader, 4), text(reader, 5));
        }
    }

    private static async Task readIndexesAsync(DbDataReader reader, Table existing, CancellationToken ct)
    {
        IndexDefinition? current = null;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);

            if (current == null || !current.Name.Equals(name, StringComparison.Ordinal))
            {
                current = new IndexDefinition(name)
                {
                    IsUnique = int32(reader, 1) == 1,
                    SortOrder = int32(reader, 2) == 1 ? SortOrder.Desc : SortOrder.Asc,
                    IsInactive = int32(reader, 3) == 1,
                    Expression = IndexDefinition.ReadExpression(text(reader, 4)),
                    Predicate = IndexDefinition.ReadPredicate(text(reader, 5))
                };

                existing.Indexes.Add(current);
            }

            var column = text(reader, 6);
            if (column != null)
            {
                current.AddColumn(column);
            }
        }
    }

    private static int? int32(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static string? text(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
}
