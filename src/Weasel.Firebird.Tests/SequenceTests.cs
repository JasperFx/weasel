using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

public class SequenceTests
{
    private static string[] createStatements(Sequence sequence)
    {
        var writer = new StringWriter();
        sequence.WriteCreateStatement(new FirebirdMigrator(), writer);
        return FirebirdScript.Split(writer.ToString()).ToArray();
    }

    [Fact]
    public void a_plain_sequence_is_one_guarded_create()
    {
        var statement = createStatements(new Sequence("order_numbers")).Single();

        statement.ShouldContain("RDB$GENERATORS WHERE RDB$GENERATOR_NAME = 'ORDER_NUMBERS'");
        statement.ShouldContain("EXECUTE STATEMENT 'CREATE SEQUENCE order_numbers'");
        statement.ShouldNotContain("CREATE OR ALTER");
    }

    /// <summary>
    ///     Firebird 3 hands out one increment past START WITH, Firebird 4 and later START WITH itself, so
    ///     the create asks the engine which it is.
    /// </summary>
    [Fact]
    public void a_start_value_is_written_for_the_version_that_runs_it()
    {
        var statement = createStatements(new Sequence(new FirebirdObjectName("order_numbers"), 100) { IncrementBy = 10 })
            .Single();

        statement.ShouldContain("ENGINE_VERSION') STARTING WITH '3.'");
        statement.ShouldContain("'CREATE SEQUENCE order_numbers START WITH 90 INCREMENT BY 10'");
        statement.ShouldContain("'CREATE SEQUENCE order_numbers START WITH 100 INCREMENT BY 10'");
    }

    [Fact]
    public void an_increment_alone_is_written_for_the_version_too()
    {
        createStatements(new Sequence("hilo") { IncrementBy = 10 }).Single()
            .ShouldContain("START WITH -9 INCREMENT BY 10");
    }

    [Fact]
    public void an_increment_of_zero_is_refused()
    {
        Should.Throw<InvalidOperationException>(() => createStatements(new Sequence("s") { IncrementBy = 0 }));
    }

    [Fact]
    public void a_sequence_in_another_schema_is_refused()
    {
        Should.Throw<NotSupportedException>(() => createStatements(new Sequence("sales.order_numbers")));
    }

    [Fact]
    public void the_drop_runs_only_while_the_sequence_exists()
    {
        var writer = new StringWriter();
        new Sequence("order_numbers").WriteDropStatement(new FirebirdMigrator(), writer);

        var statement = FirebirdScript.Split(writer.ToString()).Single();
        statement.ShouldContain("IF (EXISTS(SELECT 1 FROM RDB$GENERATORS");
        statement.ShouldContain("'DROP SEQUENCE order_numbers'");
    }

    [Fact]
    public void the_introspection_query_is_one_unterminated_statement()
    {
        var builder = new FirebirdDbCommandBuilder();
        new Sequence("order_numbers").ConfigureQueryCommand(builder);

        builder.ToString().ShouldNotEndWith(";");
        builder.CompileCommands().Single().Parameters[0].Value.ShouldBe("ORDER_NUMBERS");
    }

    [Fact]
    public void a_reserved_or_spaced_name_is_delimited()
    {
        createStatements(new Sequence("order numbers")).Single().ShouldContain("'CREATE SEQUENCE \"ORDER NUMBERS\"'");
    }

    [Fact]
    public void the_identifier_is_a_firebird_name()
    {
#pragma warning disable CS0618
        new Sequence(new DbObjectName("PUBLIC", "s")).Identifier.ShouldBeOfType<FirebirdObjectName>();
#pragma warning restore CS0618
    }

    [Fact]
    public void the_migrator_creates_firebird_sequences()
    {
        new FirebirdMigrator().CreateSequence(new FirebirdObjectName("s")).ShouldBeOfType<Sequence>();
    }

    [Theory]
    [InlineData(null, 1L, SchemaPatchDifference.None)]
    [InlineData(null, 5L, SchemaPatchDifference.None)]
    [InlineData(5L, 5L, SchemaPatchDifference.None)]
    [InlineData(10L, 5L, SchemaPatchDifference.Update)]
    public void an_increment_is_compared_only_when_the_model_states_one(long? model, long actual,
        SchemaPatchDifference expected)
    {
        new SequenceDelta(new Sequence("s") { IncrementBy = model }, actual).Difference.ShouldBe(expected);
    }

    [Fact]
    public void a_missing_sequence_is_a_create_and_rolls_back_to_a_drop()
    {
        var delta = new SequenceDelta(new Sequence("s"), null);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);

        var rollback = new StringWriter();
        delta.WriteRollback(new FirebirdMigrator(), rollback);
        rollback.ToString().ShouldContain("DROP SEQUENCE s");
    }

    [Fact]
    public void a_changed_increment_is_altered_in_place_and_rolls_back_to_the_old_one()
    {
        var delta = new SequenceDelta(new Sequence("s") { IncrementBy = 10 }, 5);

        var update = new StringWriter();
        delta.WriteUpdate(new FirebirdMigrator(), update);
        FirebirdScript.Split(update.ToString()).ShouldBe(["ALTER SEQUENCE s INCREMENT BY 10"]);

        var rollback = new StringWriter();
        delta.WriteRollback(new FirebirdMigrator(), rollback);
        FirebirdScript.Split(rollback.ToString()).ShouldBe(["ALTER SEQUENCE s INCREMENT BY 5"]);
    }
}

public class sequences_in_the_database: IntegrationContext
{
    private Task<long> nextValueAsync(string sequence) => ScalarAsync<long>($"SELECT NEXT VALUE FOR {sequence} FROM RDB$DATABASE");

    [Fact]
    public async Task a_created_sequence_exists_and_reads_back_as_no_change()
    {
        var sequence = new Sequence("order_numbers");

        (await sequence.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);

        await CreateSchemaObjectInDatabase(sequence);

        (await sequence.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await nextValueAsync("order_numbers")).ShouldBe(1L);
    }

    /// <summary>
    ///     The first value handed out is StartWith on Firebird 3, 4 and 5 alike, and the next one is an
    ///     increment on.
    /// </summary>
    [Theory]
    [InlineData(100L, 1L)]
    [InlineData(100L, 10L)]
    [InlineData(1L, 10L)]
    [InlineData(-5L, 1L)]
    public async Task the_first_value_is_the_start_value_on_every_version(long startWith, long incrementBy)
    {
        var sequence = new Sequence(new FirebirdObjectName("order_numbers"), startWith) { IncrementBy = incrementBy };

        await CreateSchemaObjectInDatabase(sequence);

        (await nextValueAsync("order_numbers")).ShouldBe(startWith, $"on Firebird {ServerVersion}");
        (await nextValueAsync("order_numbers")).ShouldBe(startWith + incrementBy);
    }

    [Fact]
    public async Task an_increment_without_a_start_value_starts_at_one()
    {
        await CreateSchemaObjectInDatabase(new Sequence("hilo") { IncrementBy = 10 });

        (await nextValueAsync("hilo")).ShouldBe(1L);
        (await nextValueAsync("hilo")).ShouldBe(11L);
    }

    [Fact]
    public async Task creating_twice_is_harmless_and_does_not_restart_it()
    {
        var sequence = new Sequence("order_numbers");
        await CreateSchemaObjectInDatabase(sequence);
        await nextValueAsync("order_numbers");

        await CreateSchemaObjectInDatabase(sequence);

        (await nextValueAsync("order_numbers")).ShouldBe(2L);
    }

    [Fact]
    public async Task a_changed_increment_is_altered_in_place_keeping_the_value_reached()
    {
        await CreateSchemaObjectInDatabase(new Sequence("hilo"));
        (await nextValueAsync("hilo")).ShouldBe(1L);

        var changed = new Sequence("hilo") { IncrementBy = 10 };
        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Update);

        var migration = await ApplyAsync(changed);

        (await changed.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await nextValueAsync("hilo")).ShouldBe(11L, "altered, not dropped and recreated");

        await migration.RollbackAllAsync(theConnection, new FirebirdMigrator());
        (await new Sequence("hilo") { IncrementBy = 1 }.FindDeltaAsync(theConnection)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task drop_removes_it_and_is_harmless_when_it_is_gone()
    {
        var sequence = new Sequence("order_numbers");
        await CreateSchemaObjectInDatabase(sequence);

        await sequence.DropAsync(theConnection);
        await sequence.DropAsync(theConnection);

        (await sequence.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task migrates_beside_tables_through_the_migration_path()
    {
        var table = new Table("orders");
        table.AddColumn<long>("id").AsPrimaryKey();

        var sequence = new Sequence("order_numbers");

        await ApplyAsync(table, sequence);

        var migration = await DetermineAsync(table, sequence);
        migration.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     An identity column's generator is the server's (system flag 6) and not a sequence of the model:
    ///     a sequence of the same shape beside it is independent.
    /// </summary>
    [Fact]
    public async Task an_identity_columns_generator_is_not_mistaken_for_a_sequence()
    {
        var table = new Table("events");
        table.AddColumn<long>("id").AsPrimaryKey().AutoIncrement();
        await ApplyAsync(table);

        var generator = (await ListAsync("SELECT RDB$GENERATOR_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = 'EVENTS'")).Single();

        (await new Sequence(generator).FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task the_teardown_drops_it()
    {
        await CreateSchemaObjectInDatabase(new Sequence("order_numbers"));

        await theConnection.DropSchemaAsync();

        (await ScalarAsync<int>("SELECT COUNT(*) FROM RDB$GENERATORS WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0")).ShouldBe(0);
    }

    [Fact]
    public async Task a_patch_with_a_sequence_is_runnable_twice()
    {
        var sequence = new Sequence(new FirebirdObjectName("order_numbers"), 50) { IncrementBy = 5 };
        var migration = await DetermineAsync(sequence);

        var writer = new StringWriter();
        new FirebirdMigrator().WriteScript(writer, (m, w) => migration.WriteAllUpdates(w, m, AutoCreate.CreateOrUpdate));

        await ExecuteAsync(writer.ToString());
        await ExecuteAsync(writer.ToString());

        (await nextValueAsync("order_numbers")).ShouldBe(50L);
    }
}
