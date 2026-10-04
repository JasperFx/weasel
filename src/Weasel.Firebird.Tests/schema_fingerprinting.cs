using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Firebird.Tests;

/// <summary>
///     The fingerprint short-circuit keeps its stamps in a table named, by default,
///     <c>{DefaultSchemaName}.weasel_schema_fingerprints</c>. Firebird before 6 has no schemas, and
///     <c>PUBLIC.weasel_schema_fingerprints</c> is a syntax error there, so the Firebird migrator names
///     the table alone through <c>Migrator.FingerprintTableName</c>.
/// </summary>
public class schema_fingerprinting: IntegrationContext
{
    private DatabaseWithTables database()
    {
        var db = new DatabaseWithTables("fingerprinted", ConnectionString);
        db.Migrator.UseSchemaFingerprinting = true;

        var table = db.AddTable(new FirebirdObjectName("people"));
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("name", typeof(string));

        return db;
    }

    [Fact]
    public async Task an_apply_stamps_the_fingerprint_and_the_next_one_trusts_it()
    {
        (await database().ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.Create);

        (await ScalarAsync<int>("SELECT COUNT(*) FROM weasel_schema_fingerprints")).ShouldBe(1);

        // Drift outside Weasel is not seen while the stamp matches: that is the trade the option makes.
        await ExecuteAsync("ALTER TABLE people DROP name");
        (await database().ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_changed_configuration_applies_again_and_stamps_again()
    {
        await database().ApplyAllConfiguredChangesToDatabaseAsync();

        var changed = database();
        changed.Tables.Single().AddColumn("age", typeof(int));

        (await changed.ApplyAllConfiguredChangesToDatabaseAsync()).ShouldBe(SchemaPatchDifference.Update);
        (await ScalarAsync<int>("SELECT COUNT(*) FROM weasel_schema_fingerprints")).ShouldBe(2);
    }

    [Fact]
    public void the_fingerprint_table_is_named_without_a_schema()
    {
        new NamingMigrator().Name("weasel_schema_fingerprints").ShouldBe("weasel_schema_fingerprints");
    }

    private sealed class NamingMigrator: FirebirdMigrator
    {
        public string Name(string table) => FingerprintTableName(table);
    }
}
