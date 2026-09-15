using System.IO;
using System.Threading.Tasks;
using Shouldly;
using Weasel.Postgresql.Tables;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.Indexes;

/// <summary>
/// An index added to a table that already exists is built against rows, which a plain CREATE INDEX does
/// under ACCESS EXCLUSIVE. Whether that matters is not a property of the index but of the moment, and the
/// delta is where the moment is known.
/// </summary>
[Collection("adding_indexes")]
public class adding_an_index_to_an_existing_table(): IndexDeltasDetectionContext("adding_indexes")
{
    [Fact]
    public async Task is_built_concurrently_when_the_migrator_asks_for_it()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("user_name").AddIndex();
        var delta = await theTable.FindDeltaAsync(theConnection);

        UpdateSql(delta, new PostgresqlMigrator { BuildIndexesConcurrentlyOnAlter = true })
            .ShouldContain("CREATE INDEX CONCURRENTLY");
    }

    [Fact]
    public async Task is_built_blocking_by_default()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("user_name").AddIndex();
        var delta = await theTable.FindDeltaAsync(theConnection);

        var sql = UpdateSql(delta, new PostgresqlMigrator());

        sql.ShouldContain("CREATE INDEX");
        sql.ShouldNotContain("CONCURRENTLY");
    }

    /// <summary>
    /// Creating the table is the other half: nothing is scanned there, so the index belongs inline in the
    /// CREATE TABLE script and the option must not reach it.
    /// </summary>
    [Fact]
    public void creating_the_table_is_left_blocking()
    {
        theTable.ModifyColumn("user_name").AddIndex();

        var writer = new StringWriter();
        theTable.WriteCreateStatement(new PostgresqlMigrator { BuildIndexesConcurrentlyOnAlter = true }, writer);

        writer.ToString().ShouldNotContain("CONCURRENTLY");
    }

    private static string UpdateSql(TableDelta delta, PostgresqlMigrator migrator)
    {
        var writer = new StringWriter();
        delta.WriteUpdate(migrator, writer);
        return writer.ToString();
    }
}
