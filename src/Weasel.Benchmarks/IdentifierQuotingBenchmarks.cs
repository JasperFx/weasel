using BenchmarkDotNet.Attributes;
using Weasel.Postgresql;
using Weasel.SqlServer;
using PgTable = Weasel.Postgresql.Tables.Table;
using SsTable = Weasel.SqlServer.Tables.Table;
using SsTableDelta = Weasel.SqlServer.Tables.TableDelta;
using SsIndex = Weasel.SqlServer.Tables.IndexDefinition;

namespace Weasel.Benchmarks;

/// <summary>
///     The reserved-word check every quoted SQL Server identifier pays (weasel#825), and the DDL paths
///     it dominated. PostgreSQL's object name and table are here only as the yardstick the issue was
///     measured against.
/// </summary>
/// <remarks>
///     The three <c>IsReservedWord</c> rows are the three paths through it: an ASCII keyword (a hit),
///     an ASCII ordinary name (a miss — the common case, and the one a linear scan paid in full), and
///     a non-ASCII name, which deliberately still takes the culture-aware scan.
/// </remarks>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class IdentifierQuotingBenchmarks
{
    private readonly PostgresqlMigrator _pgMigrator = new();
    private readonly SqlServerMigrator _ssMigrator = new();

    private PostgresqlObjectName _pgName = null!;
    private SqlServerObjectName _ssName = null!;

    private PgTable _pgTable = null!;
    private SsTable _ssTable = null!;
    private SsTableDelta _ssDelta = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pgName = new PostgresqlObjectName("public", "mt_doc_user",
            Postgresql.SchemaUtils.IdentifierUsage.General);
        _ssName = new SqlServerObjectName("dbo", "mt_doc_user");

        _pgTable = new PgTable("public.people");
        _pgTable.AddColumn<Guid>("id").AsPrimaryKey();
        _pgTable.AddColumn<string>("first_name").NotNull();
        _pgTable.AddColumn<string>("last_name").NotNull();
        _pgTable.AddColumn("email", "varchar(250)");
        _pgTable.AddColumn<int>("age").DefaultValue(18);
        _pgTable.AddColumn<DateTimeOffset>("created_at").DefaultValueByExpression("now()");
        _pgTable.AddColumn("balance", "numeric(12,2)");
        _pgTable.AddColumn<bool>("is_active").DefaultValueByExpression("true");
        _pgTable.AddColumn<Guid>("org_id").ForeignKeyTo("public.orgs", "id");
        _pgTable.AddColumn("data", "jsonb");
        _pgTable.Indexes.Add(new Postgresql.Tables.IndexDefinition("idx_people_email")
        {
            Columns = ["email"], IsUnique = true
        });
        _pgTable.Indexes.Add(new Postgresql.Tables.IndexDefinition("idx_people_last_name")
        {
            Columns = ["last_name", "first_name"]
        });

        _ssTable = ssTable(true);
        _ssDelta = new SsTableDelta(_ssTable, ssTable(false));
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

    [Benchmark(Description = "SqlServer IsReservedWord: ASCII keyword (\"select\")")]
    public bool ReservedHit() => SqlServerIdentifierRules.Instance.IsReservedWord("select");

    [Benchmark(Description = "SqlServer IsReservedWord: ASCII name (\"mt_doc_user\")")]
    public bool ReservedMiss() => SqlServerIdentifierRules.Instance.IsReservedWord("mt_doc_user");

    [Benchmark(Description = "SqlServer IsReservedWord: non-ASCII name (\"Grüße\")")]
    public bool ReservedNonAscii() => SqlServerIdentifierRules.Instance.IsReservedWord("Grüße");

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

    [Benchmark(Description = "SqlServer TableDelta.WriteUpdate (+2 cols, +2 idx, +1 fk)")]
    public string SqlServerPatch()
    {
        var writer = new StringWriter();
        _ssDelta.WriteUpdate(_ssMigrator, writer);
        return writer.ToString();
    }
}
