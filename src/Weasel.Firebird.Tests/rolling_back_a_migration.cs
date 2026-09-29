using System.Data.Common;
using System.Globalization;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Tests;

/// <summary>
///     A rollback is rendered DDL like any other, so it has to be split into statements before Firebird
///     will run it: the default of one batch sends the whole script as a single command, which fails on
///     its second statement (the gap Oracle still has).
/// </summary>
public class rolling_back_a_migration: IntegrationContext
{
    private static Table people()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        return table;
    }

    [Fact]
    public void rollback_sql_is_split_into_one_statement_per_batch()
    {
        var writer = new StringWriter();
        people().WriteDropStatement(new FirebirdMigrator(), writer);

        new FirebirdMigrator().SplitIntoBatches(writer.ToString()).Count.ShouldBe(2);
    }

    [Fact]
    public async Task rolling_back_a_create_drops_the_table()
    {
        var migration = await ApplyAsync(people());

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task rolling_back_an_update_restores_the_previous_shape()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var original = people();
        original.AddColumn<int>("legacy");
        await ApplyAsync(states, original);

        var changed = people();
        changed.AddColumn<int>("age").AddIndex();
        changed.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        var migration = await ApplyAsync(changed);
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await original.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task rolling_back_several_tables_runs_every_statement()
    {
        var states = new Table("states");
        states.AddColumn<int>("id").AsPrimaryKey();

        var migration = await ApplyAsync(states, people());

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await states.ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
    }

    /// <summary>
    ///     Firebird will not drop a table a view still uses, so a rollback undoes the view first: the
    ///     deltas are undone last to first.
    /// </summary>
    [Fact]
    public async Task rolling_back_a_table_and_a_view_over_it_drops_both()
    {
        var names = new RawView("v_names", "SELECT name FROM people");
        var migration = await ApplyAsync(people(), names);

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'V_NAMES'"))
            .ShouldBe(0);
    }

    /// <summary>
    ///     Nor will it drop a column a view uses: the view over the added column goes before the column.
    /// </summary>
    [Fact]
    public async Task rolling_back_a_new_column_and_a_view_over_it_restores_the_table()
    {
        await ApplyAsync(people());

        var changed = people();
        changed.AddColumn<int>("age");
        var migration = await ApplyAsync(changed, new RawView("v_ages", "SELECT age FROM people"));

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());

        (await people().FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'V_AGES'"))
            .ShouldBe(0);
    }

    /// <summary>
    ///     A migration file's drop script is written by the same code, in the same order, and runs.
    /// </summary>
    [Fact]
    public async Task a_migration_files_drop_script_drops_the_view_before_the_table()
    {
        var migrator = new FirebirdMigrator();
        var names = new RawView("v_names", "SELECT name FROM people");
        var migration = await SchemaMigration.DetermineAsync(theConnection, migrator, CancellationToken.None,
            people(), names);
        await migrator.ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);

        var file = Path.Combine(Path.GetTempPath(), $"weasel-firebird-{Guid.NewGuid():N}.sql");
        await migrator.WriteMigrationFileAsync(file, migration);
        var dropFile = SchemaMigration.ToDropFileName(file);

        try
        {
            var drop = await File.ReadAllTextAsync(dropFile);
            drop.IndexOf("DROP VIEW v_names", StringComparison.Ordinal)
                .ShouldBeLessThan(drop.IndexOf("DROP TABLE people", StringComparison.Ordinal));

            await migrator.ExecuteScriptAsync(theConnection, drop);

            (await people().ExistsInDatabaseAsync(theConnection)).ShouldBeFalse();
        }
        finally
        {
            File.Delete(file);
            File.Delete(dropFile);
        }
    }

    /// <summary>
    ///     A view as raw DDL, standing in for a typed one: all a rollback needs is its create, its drop
    ///     and whether it is there.
    /// </summary>
    private sealed class RawView(string name, string select): ISchemaObject
    {
        public DbObjectName Identifier { get; } = new FirebirdObjectName(name);

        public void WriteCreateStatement(Migrator migrator, TextWriter writer)
            => FirebirdScript.WriteStatement(writer, $"CREATE VIEW {name} AS {select}");

        public void WriteDropStatement(Migrator rules, TextWriter writer)
            => FirebirdScript.WriteStatement(writer, $"DROP VIEW {name}");

        public void ConfigureQueryCommand(DbCommandBuilder builder)
        {
            var parameter = builder.AddParameter(name.ToUpperInvariant()).ParameterName;
            builder.Append(
                $"SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = @{parameter} AND RDB$VIEW_BLR IS NOT NULL");
        }

        public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
        {
            await reader.ReadAsync(ct);
            var exists = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture) > 0;
            return new SchemaObjectDelta(this, exists ? SchemaPatchDifference.None : SchemaPatchDifference.Create);
        }

        public IEnumerable<DbObjectName> AllNames() => [Identifier];
    }
}
