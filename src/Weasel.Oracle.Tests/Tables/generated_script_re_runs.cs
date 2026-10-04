using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     weasel#681. A generated creation script has to be re-runnable: one that only works against a
///     virgin schema is a trap, and running it again after a partial failure is the first thing an
///     operator tries.
/// </summary>
/// <remarks>
///     Oracle's <c>CREATE TABLE</c> is already wrapped in an <c>all_tables</c> existence check, so a
///     second run skipped it and reached the trailing <c>ALTER TABLE … ADD CONSTRAINT</c>, which had
///     no guard of its own and failed with <c>ORA-02275</c>. A model with no foreign keys re-ran
///     fine, which is why this went unnoticed.
///     <para>
///     The script is split on <c>/</c> lines the way sqlplus reads it, because ODP.NET will not
///     execute several statements from one command — the same reason
///     <c>OracleDbCommandBuilder</c> splits a batch.
///     </para>
/// </remarks>
[Collection("integration")]
public class generated_script_re_runs: IntegrationContext
{
    public generated_script_re_runs(): base("WEASEL")
    {
    }

    private static Table table(string name, string? references = null)
    {
        var table = new Table(new OracleObjectName("WEASEL", name));
        table.AddColumn<int>("id").AsPrimaryKey();

        if (references != null)
        {
            table.AddColumn<int>("p_id").AllowNulls();
            table.ForeignKeys.Add(new ForeignKey($"fk_{name}")
            {
                LinkedTable = new OracleObjectName("WEASEL", references),
                ColumnNames = ["p_id"],
                LinkedNames = ["id"]
            });
        }

        return table;
    }

    /// <summary>
    ///     Split on a line containing only <c>/</c>, which is how sqlplus ends a PL/SQL block.
    /// </summary>
    private static IEnumerable<string> statements(string script)
        => script.Split('\n')
            .Aggregate(new List<List<string>> { new() }, (acc, line) =>
            {
                if (line.Trim() == "/")
                {
                    acc.Add(new List<string>());
                }
                else
                {
                    acc[^1].Add(line);
                }

                return acc;
            })
            .Select(lines => string.Join('\n', lines).Trim())
            .Where(x => x.Length > 0);

    private async Task runAsync(string script)
    {
        foreach (var statement in statements(script))
        {
            await using var cmd = theConnection.CreateCommand(statement);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<int> constraintCountAsync(string name)
    {
        await using var cmd = theConnection.CreateCommand(
            $"select count(*) from all_constraints where owner = 'WEASEL' and constraint_name = '{name.ToUpperInvariant()}'");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task the_script_runs_and_re_runs()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("WEASEL", ConnectionSource.ConnectionString);
        db.AddTable(table("or_parent"));
        db.AddTable(table("or_child", "or_parent"));

        var script = db.ToDatabaseScript();

        await runAsync(script);
        (await constraintCountAsync("fk_or_child")).ShouldBe(1);

        // The assertion with teeth: without the guard this throws ORA-02275
        await runAsync(script);

        (await constraintCountAsync("fk_or_child")).ShouldBe(1);
    }
}
