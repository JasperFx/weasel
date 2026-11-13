using BenchmarkDotNet.Attributes;
using Weasel.Core;
using Weasel.Postgresql;
using PgTable = Weasel.Postgresql.Tables.Table;
using PgTableDelta = Weasel.Postgresql.Tables.TableDelta;
using PgIndex = Weasel.Postgresql.Tables.IndexDefinition;
using SsTable = Weasel.SqlServer.Tables.Table;
using SsTableDelta = Weasel.SqlServer.Tables.TableDelta;
using SsIndex = Weasel.SqlServer.Tables.IndexDefinition;

namespace Weasel.Benchmarks;

/// <summary>
///     The string-formatting paths weasel#805 asked about, in-process and with no connection:
///     building command text, rendering an object name, and writing DDL and a schema patch.
/// </summary>
/// <remarks>
///     <para>
///         The two command rows are the per-command path: ten parameters appended into a statement and
///         the command compiled. Parameter naming on it was already made allocation-free by weasel#558
///         (<see cref="ParameterNames" />), and PostgreSQL's <c>$N</c> markers go through
///         <c>StringBuilder.Append(int)</c>, which formats into the builder's own buffer.
///     </para>
///     <para>
///         <c>ToString()</c> on a provider object name re-renders the quoted form on every call
///         (<c>QuotedQualifiedName</c> is an interpolation, not a field), which is the one place a
///         number-free interpolation runs repeatedly. The DDL and patch rows run once per migration,
///         not per command, and are here to put a size on what span formatting could save there.
///     </para>
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class FormattingBenchmarks
{
    private readonly PostgresqlMigrator _pgMigrator = new();
    private readonly SqlServer.SqlServerMigrator _ssMigrator = new();

    private PostgresqlObjectName _pgName = null!;
    private SqlServer.SqlServerObjectName _ssName = null!;

    private PgTable _pgTable = null!;
    private PgTableDelta _pgDelta = null!;
    private SsTable _ssTable = null!;
    private SsTableDelta _ssDelta = null!;

    private object[] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pgName = new PostgresqlObjectName("public", "mt_doc_user", SchemaUtils.IdentifierUsage.General);
        _ssName = new SqlServer.SqlServerObjectName("dbo", "mt_doc_user");

        _pgTable = pgTable(true);
        _pgDelta = new PgTableDelta(_pgTable, pgTable(false));
        _ssTable = ssTable(true);
        _ssDelta = new SsTableDelta(_ssTable, ssTable(false));

        _values = [1, 2L, "three", Guid.NewGuid(), DateTimeOffset.UtcNow, true, 7.5m, 8, "nine", 10L];
    }

    private static PgTable pgTable(bool complete)
    {
        var table = new PgTable("public.people");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull();
        table.AddColumn<string>("last_name").NotNull();
        table.AddColumn("email", "varchar(250)");
        table.AddColumn<int>("age").DefaultValue(18);
        table.AddColumn<DateTimeOffset>("created_at").DefaultValueByExpression("now()");
        table.AddColumn("balance", "numeric(12,2)");
        table.AddColumn<bool>("is_active").DefaultValueByExpression("true");

        if (complete)
        {
            table.AddColumn<Guid>("org_id").ForeignKeyTo("public.orgs", "id");
            table.AddColumn("data", "jsonb");
            table.Indexes.Add(new PgIndex("idx_people_email") { Columns = ["email"], IsUnique = true });
            table.Indexes.Add(new PgIndex("idx_people_last_name") { Columns = ["last_name", "first_name"] });
        }

        return table;
    }

    private static SsTable ssTable(bool complete)
    {
        var table = new SsTable("dbo.people");
        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name").NotNull();
        table.AddColumn<string>("last_name").NotNull();
        table.AddColumn("email", "varchar(250)");
        table.AddColumn<int>("age").DefaultValue(18);
        table.AddColumn<DateTimeOffset>("created_at").DefaultValueByExpression("sysdatetimeoffset()");
        table.AddColumn("balance", "decimal(12,2)");
        table.AddColumn<bool>("is_active").DefaultValueByExpression("1");

        if (complete)
        {
            table.AddColumn<Guid>("org_id").ForeignKeyTo("dbo.orgs", "id");
            table.AddColumn("data", "nvarchar(max)");
            table.Indexes.Add(new SsIndex("idx_people_email") { Columns = ["email"], IsUnique = true });
            table.Indexes.Add(new SsIndex("idx_people_last_name") { Columns = ["last_name", "first_name"] });
        }

        return table;
    }

    [Benchmark(Description = "Postgresql CommandBuilder: 10 params + Compile")]
    public string PostgresqlCommand()
    {
        var builder = new Postgresql.CommandBuilder();
        builder.Append("select id, data from public.mt_doc_user where ");
        for (var i = 0; i < _values.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(" and ");
            }

            builder.Append("c");
            builder.Append(" = ");
            builder.AppendParameter(_values[i]);
        }

        return builder.Compile().CommandText;
    }

    [Benchmark(Description = "SqlServer CommandBuilder: 10 params + Compile")]
    public string SqlServerCommand()
    {
        var builder = new SqlServer.CommandBuilder();
        builder.Append("select id, data from dbo.mt_doc_user where ");
        for (var i = 0; i < _values.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(" and ");
            }

            builder.Append("c");
            builder.Append(" = ");
            builder.AppendParameter(_values[i]);
        }

        return builder.Compile().CommandText;
    }

    [Benchmark(Description = "PostgresqlObjectName.ToString()")]
    public string PostgresqlName() => _pgName.ToString();

    [Benchmark(Description = "SqlServerObjectName.ToString()")]
    public string SqlServerName() => _ssName.ToString();

    [Benchmark(Description = "Postgresql Table.WriteCreateStatement (10 cols, 2 idx, 1 fk)")]
    public string PostgresqlCreateTable()
    {
        var writer = new StringWriter();
        _pgTable.WriteCreateStatement(_pgMigrator, writer);
        return writer.ToString();
    }

    [Benchmark(Description = "SqlServer Table.WriteCreateStatement (10 cols, 2 idx, 1 fk)")]
    public string SqlServerCreateTable()
    {
        var writer = new StringWriter();
        _ssTable.WriteCreateStatement(_ssMigrator, writer);
        return writer.ToString();
    }

    [Benchmark(Description = "Postgresql TableDelta.WriteUpdate (+2 cols, +2 idx, +1 fk)")]
    public string PostgresqlPatch()
    {
        var writer = new StringWriter();
        _pgDelta.WriteUpdate(_pgMigrator, writer);
        return writer.ToString();
    }

    [Benchmark(Description = "SqlServer TableDelta.WriteUpdate (+2 cols, +2 idx, +1 fk)")]
    public string SqlServerPatch()
    {
        var writer = new StringWriter();
        _ssDelta.WriteUpdate(_ssMigrator, writer);
        return writer.ToString();
    }
}
