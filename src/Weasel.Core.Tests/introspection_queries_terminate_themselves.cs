using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Every schema object's introspection query has to terminate itself.
/// </summary>
/// <remarks>
///     <para>
///     <see cref="SchemaMigration" /> concatenates the objects of a migration into a single command,
///     separating them with <see cref="CommandBuilderBase{TCommand,TParameter,TDbType}.StartNewCommand" />
///     — which is a no-op on every provider but Oracle. So an object whose query does not end in a
///     terminator runs straight into whatever follows it, and the migration fails with a syntax
///     error at the next statement's first keyword.
///     </para>
///     <para>
///     The failure only appears in combination. Each object is fine on its own, which is exactly why
///     per-object tests never caught it: PostgreSQL's stored procedure, user-defined type and
///     trigger shipped unterminated (weasel#515), and SQLite's trigger with them (weasel#518). This
///     test exists so the next schema object type added cannot repeat it.
///     </para>
///     <para>
///     <b>Oracle is deliberately excluded, and asserted separately below.</b> ODP.NET will not
///     execute several statements from one command, so <c>OracleDbCommandBuilder</c> overrides
///     <c>StartNewCommand</c> to split the batch and hand back one command per statement. A
///     terminator there is a syntax error, not a fix — so a well-meaning sweep across all providers
///     would break Oracle. That is the whole reason the exclusion is a test rather than a comment.
///     </para>
/// </remarks>
public class introspection_queries_terminate_themselves
{
    /// <summary>
    ///     One instance of every schema object type, per provider whose statements are concatenated.
    /// </summary>
    /// <remarks>
    ///     Listed rather than reflected into existence: the constructors have nothing in common, and
    ///     guessing arguments would fail in ways that look like product bugs.
    ///     <see cref="every_schema_object_type_is_covered" /> is what keeps the list honest.
    /// </remarks>
    private static IEnumerable<(string Provider, ISchemaObject Object)> batchingObjects()
    {
        yield return ("PostgreSQL", new Postgresql.Tables.Table("test.thing"));
        yield return ("PostgreSQL", new Postgresql.Sequence("test.thing_seq"));
        yield return ("PostgreSQL", new Postgresql.Views.View("test.v_thing", "select 1"));
        yield return ("PostgreSQL",
            new Postgresql.Functions.Function(new DbObjectName("test", "fn_thing"), "create function test.fn_thing() returns int as $$ select 1 $$ language sql;"));
        yield return ("PostgreSQL",
            new Postgresql.Procedures.StoredProcedure(new DbObjectName("test", "p_thing"), "create procedure test.p_thing() language plpgsql as $$ begin end; $$;"));
        yield return ("PostgreSQL", Postgresql.Types.UserDefinedType.Enum("test.mood", "happy", "sad"));
        yield return ("PostgreSQL",
            new Postgresql.Triggers.Trigger(new DbObjectName("test", "trg_thing"), new DbObjectName("test", "thing"),
                "before insert on test.thing for each row execute function test.stamp()"));
        yield return ("PostgreSQL", new Postgresql.Views.MaterializedView("test.mv_thing", "select 1"));
        yield return ("PostgreSQL", new Postgresql.Extension("postgis"));
        yield return ("PostgreSQL", new Postgresql.SchemaExistenceCheck([new Postgresql.Tables.Table("test.thing")]));
        yield return ("PostgreSQL", new Postgresql.Tables.DatabasePoolTable("test"));
        yield return ("PostgreSQL", new Postgresql.Tables.TenantAssignmentTable("test"));
        yield return ("PostgreSQL",
            new Postgresql.Functions.UpsertFunction(new DbObjectName("test", "upsert_thing"),
                upsertTarget(), "id"));

        yield return ("SQL Server", new SqlServer.Tables.Table("test.thing"));
        yield return ("SQL Server", new SqlServer.Tables.TableType(new DbObjectName("test", "thing_type")));
        yield return ("SQL Server", new SqlServer.Sequence("test.thing_seq"));
        yield return ("SQL Server", new SqlServer.Views.View("test.v_thing", "select 1 as x"));
        yield return ("SQL Server",
            new SqlServer.Functions.Function(new DbObjectName("test", "fn_thing"), "create function test.fn_thing() returns int as begin return 1 end"));
        yield return ("SQL Server",
            new SqlServer.Procedures.StoredProcedure(new DbObjectName("test", "p_thing"), "create procedure test.p_thing as select 1"));
        yield return ("SQL Server", new SqlServer.Synonyms.Synonym("test.s_thing", "test.thing"));
        yield return ("SQL Server",
            new SqlServer.Triggers.Trigger(new DbObjectName("test", "trg_thing"), new DbObjectName("test", "thing"),
                "after insert as select 1"));

        yield return ("SQLite", new Sqlite.Tables.Table("thing"));
        yield return ("SQLite", new Sqlite.Views.View("v_thing", "select 1"));
        yield return ("SQLite",
            new Sqlite.Triggers.Trigger("trg_thing", "thing", "after insert on thing begin select 1; end"));

        yield return ("MySQL", new MySql.Tables.Table("test.thing"));
        yield return ("MySQL", new MySql.Sequence("test.thing_seq"));
        yield return ("MySQL", new MySql.Views.View("test.v_thing", "select 1"));
        yield return ("MySQL", new MySql.Functions.Function("test.fn_thing", "create function test.fn_thing() returns int return 1;"));
        yield return ("MySQL",
            new MySql.Procedures.StoredProcedure("test.p_thing", "create procedure test.p_thing() begin select 1; end"));
        yield return ("MySQL",
            new MySql.Triggers.Trigger("trg_thing", "test.thing", "after insert on test.thing for each row begin end"));
    }

    /// <summary>
    ///     <see cref="Postgresql.Functions.UpsertFunction" /> derives its signature from a table, so
    ///     it needs a real one with a primary key rather than an empty placeholder.
    /// </summary>
    private static Postgresql.Tables.Table upsertTarget()
    {
        var table = new Postgresql.Tables.Table("test.thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    /// <summary>
    ///     The Oracle objects, which must NOT terminate. See the class remarks.
    /// </summary>
    private static IEnumerable<ISchemaObject> oracleObjects()
    {
        yield return new Oracle.Tables.Table("TEST.THING");
        yield return new Oracle.Sequence("TEST.THING_SEQ");
        yield return new Oracle.Views.View("TEST.V_THING", "select 1 from dual");
        yield return new Oracle.Views.MaterializedView("TEST.MV_THING", "select 1 from dual");
        yield return new Oracle.Synonyms.Synonym("TEST.S_THING", "TEST.THING");
        yield return new Oracle.Functions.Function(new DbObjectName("TEST", "FN_THING"),
            "create function TEST.FN_THING return number is begin return 1; end;");
        yield return new Oracle.Procedures.StoredProcedure(new DbObjectName("TEST", "P_THING"),
            "create procedure TEST.P_THING is begin null; end;");
        yield return new Oracle.Packages.Package(new DbObjectName("TEST", "PKG_THING"),
            "create package TEST.PKG_THING is end;", "create package body TEST.PKG_THING is end;");
        yield return new Oracle.Triggers.Trigger(new DbObjectName("TEST", "TRG_THING"),
            new DbObjectName("TEST", "THING"), "before insert on TEST.THING begin null; end;");
    }

    private static DbCommand commandFor(string provider) => provider switch
    {
        "PostgreSQL" => new NpgsqlCommand(),
        "SQL Server" => new SqlCommand(),
        "SQLite" => new SqliteCommand(),
        "MySQL" => new MySqlCommand(),
        _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "No command type registered")
    };

    private static string sqlFor(ISchemaObject schemaObject, DbCommand command)
    {
        var builder = new DbCommandBuilder(command);
        schemaObject.ConfigureQueryCommand(builder);
        return builder.ToString();
    }

    [Fact]
    public void every_batching_provider_object_terminates_its_query()
    {
        var offenders = batchingObjects()
            .Where(x => !sqlFor(x.Object, commandFor(x.Provider)).TrimEnd().EndsWith(';'))
            .Select(x => $"{x.Provider}: {x.Object.GetType().FullName}")
            .ToArray();

        offenders.ShouldBeEmpty(
            $"These introspection queries do not end in ';', so they will run into the next object's query when a migration contains more than one:{Environment.NewLine}"
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    ///     Oracle's queries must stay unterminated. A ';' here is not a missing fix — it breaks the
    ///     split that lets ODP.NET execute the batch at all.
    /// </summary>
    [Fact]
    public void oracle_objects_do_not_terminate_their_queries()
    {
        var offenders = oracleObjects()
            .Where(x => sqlFor(x, new global::Oracle.ManagedDataAccess.Client.OracleCommand()).TrimEnd().EndsWith(';'))
            .Select(x => x.GetType().FullName!)
            .ToArray();

        offenders.ShouldBeEmpty(
            $"Oracle executes one statement per command, so these must NOT end in ';':{Environment.NewLine}"
            + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    ///     The guard on the guard: a schema object type that nothing above instantiates is a type
    ///     whose query is never checked, and adding one is exactly when this mistake gets made.
    /// </summary>
    [Fact]
    public void every_schema_object_type_is_covered()
    {
        var covered = batchingObjects().Select(x => x.Object.GetType())
            .Concat(oracleObjects().Select(x => x.GetType()))
            .ToHashSet();

        var assemblies = new[]
        {
            typeof(Postgresql.Tables.Table).Assembly,
            typeof(SqlServer.Tables.Table).Assembly,
            typeof(Sqlite.Tables.Table).Assembly,
            typeof(MySql.Tables.Table).Assembly,
            typeof(Oracle.Tables.Table).Assembly
        };

        var missing = assemblies
            .SelectMany(a => a.GetExportedTypes())
            .Where(t => t is { IsAbstract: false, IsGenericTypeDefinition: false }
                        && typeof(ISchemaObject).IsAssignableFrom(t)
                        && !covered.Contains(t))
            .Select(t => t.FullName!)
            .OrderBy(x => x)
            .ToArray();

        missing.ShouldBeEmpty(
            $"These schema object types are not covered by this test, so nothing checks whether their introspection query terminates. Add an instance to batchingObjects() -- or to oracleObjects() if the type is Oracle's:{Environment.NewLine}"
            + string.Join(Environment.NewLine, missing));
    }
}
