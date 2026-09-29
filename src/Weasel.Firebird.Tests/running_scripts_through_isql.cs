using System.Diagnostics;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A3: every script Weasel writes -- a patch, its drop file, a whole database -- has to run in isql
///     unchanged: PSQL wrapped in <c>SET TERM</c>, and a <c>COMMIT</c> after every statement, without
///     which isql runs a guarded block and the plain DDL after it in one transaction and silently loses
///     the guarded object at the commit.
/// </summary>
/// <remarks>
///     The isql tests shell out to <c>docker exec</c> and run only when
///     <c>weasel_firebird_isql_container</c> names the container of the server under test -- for the
///     compose file, <c>weasel-fb-core-firebird5-1</c> or its siblings. Without it they are skipped, and
///     say so.
/// </remarks>
public class running_scripts_through_isql: IntegrationContext
{
    private static readonly string? Container = Environment.GetEnvironmentVariable("weasel_firebird_isql_container");

    private static ISchemaObject[] model()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();
        states.AddColumn("name", "VARCHAR(40)").DefaultValueByString("it's; ^ fine");

        var people = new Table("people");
        people.AddColumn<long>("id").AsPrimaryKey().AutoIncrement();
        people.AddColumn<string>("order date").AddIndex();
        people.AddColumn<int>("state_id").ForeignKeyTo(states, "id", onDelete: CascadeAction.Cascade);

        return [people, states];
    }

    private static async Task<(int ExitCode, string Output)> runAsync(params string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        return (process.ExitCode, (await output) + (await error));
    }

    /// <summary>
    ///     Run a script with <c>isql -q -b -i</c> inside the server's container, where <c>-b</c> stops at
    ///     the first failure and makes it the exit code.
    /// </summary>
    private async Task isqlAsync(string script)
    {
        if (Container == null)
        {
            Assert.Skip("Set weasel_firebird_isql_container to the Firebird container's name to run scripts through isql");
        }

        var local = Path.Combine(Path.GetTempPath(), $"weasel-firebird-{Guid.NewGuid():N}.sql");
        var remote = $"/tmp/{Path.GetFileName(local)}";
        await File.WriteAllTextAsync(local, script, new UTF8Encoding(false));

        try
        {
            var copy = await runAsync("cp", local, $"{Container}:{remote}");
            copy.ExitCode.ShouldBe(0, copy.Output);

            var builder = new FbConnectionStringBuilder(ConnectionString);
            var run = await runAsync("exec", Container!, "/opt/firebird/bin/isql", "-q", "-b", "-m", "-ch", "UTF8",
                "-user", builder.UserID, "-password", builder.Password, "-i", remote, $"localhost:{builder.Database}");

            run.ExitCode.ShouldBe(0, $"isql failed on:{Environment.NewLine}{script}{Environment.NewLine}{run.Output}");
            run.Output.ShouldNotContain("Statement failed");
        }
        finally
        {
            File.Delete(local);
            await runAsync("exec", Container!, "rm", "-f", remote);
        }
    }

    private async Task<(string Update, string Drop)> patchFilesAsync(params ISchemaObject[] objects)
    {
        var migration = await DetermineAsync(objects);
        var file = Path.Combine(Path.GetTempPath(), $"weasel-firebird-{Guid.NewGuid():N}.sql");

        try
        {
            await new FirebirdMigrator().WriteMigrationFileAsync(file, migration);
            return (await File.ReadAllTextAsync(file), await File.ReadAllTextAsync(SchemaMigration.ToDropFileName(file)));
        }
        finally
        {
            File.Delete(file);
            File.Delete(SchemaMigration.ToDropFileName(file));
        }
    }

    [Fact]
    public async Task a_patch_runs_clean_through_isql_twice_and_its_drop_file_undoes_it()
    {
        var (update, drop) = await patchFilesAsync(model());

        await isqlAsync(update);
        (await DetermineAsync(model())).Difference.ShouldBe(SchemaPatchDifference.None);

        // weasel#620: every CREATE and ADD is guarded, so the same patch runs again.
        await isqlAsync(update);

        await isqlAsync(drop);
        (await ((Table)model()[0]).ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await ((Table)model()[1]).ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task an_update_patch_runs_clean_through_isql()
    {
        await ApplyAsync(model());

        var changed = model();
        var people = (Table)changed[0];
        people.AddColumn<int>("age").NotNull().DefaultValue(0);
        people.ModifyColumn("age").AddIndex(x => x.SortOrder = SortOrder.Desc);
        people.ForeignKeys.Clear();

        var (update, _) = await patchFilesAsync(changed);
        await isqlAsync(update);

        (await DetermineAsync(changed)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A whole-database script writes each object in the order it was registered, with no migration
    ///     to hold a foreign key back until its table exists -- on every provider -- so the referenced
    ///     table goes first.
    /// </summary>
    private DatabaseWithTables database()
    {
        var db = new DatabaseWithTables("scripted", ConnectionString);
        foreach (var table in model().Cast<ITable>().Reverse())
        {
            db.AddTable(table);
        }

        return db;
    }

    /// <summary>
    ///     <c>DatabaseBase.ToDatabaseScript</c> used to write past the writer the migrator hands its step,
    ///     straight into the outer one, so the Firebird migrator never saw the statements it has to put
    ///     a commit after.
    /// </summary>
    [Fact]
    public void the_whole_database_script_commits_after_every_statement()
    {
        var statements = FirebirdScript.Split(database().ToDatabaseScript());

        statements.Count.ShouldBe(8);
        statements.Where((_, i) => i % 2 == 1).ShouldAllBe(x => x == "COMMIT");
        statements.Where((_, i) => i % 2 == 0).ShouldAllBe(x => x.StartsWith("EXECUTE BLOCK"));
    }

    [Fact]
    public async Task the_whole_database_script_runs_clean_through_isql()
    {
        var db = database();

        await isqlAsync(db.ToDatabaseScript());

        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
