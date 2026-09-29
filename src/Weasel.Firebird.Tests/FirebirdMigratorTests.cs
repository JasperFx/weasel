using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using JasperFx;
using NSubstitute;
using Shouldly;
using Weasel.Core;
using Weasel.Firebird.Tables;
using Xunit;

namespace Weasel.Firebird.Tests;

public class FirebirdMigratorTests
{
    private readonly FirebirdMigrator theMigrator = new();

    [Fact]
    public void the_default_schema_is_public()
    {
        theMigrator.DefaultSchemaName.ShouldBe("PUBLIC");
    }

    [Fact]
    public void matches_only_a_firebird_connection()
    {
        theMigrator.MatchesConnection(new FbConnection()).ShouldBeTrue();
        theMigrator.MatchesConnection(Substitute.For<DbConnection>()).ShouldBeFalse();
    }

    [Fact]
    public void json_is_stored_as_text_blobs()
    {
        theMigrator.DefaultJsonColumnType.ShouldBe("BLOB SUB_TYPE TEXT");
    }

    [Fact]
    public void creates_firebird_tables()
    {
        theMigrator.CreateTable(new FirebirdObjectName("people")).ShouldBeOfType<Table>();
    }

    [Fact]
    public void builds_introspection_through_the_splitting_builder()
    {
        theMigrator.CreateCommandBuilder(new FbConnection()).ShouldBeOfType<FirebirdDbCommandBuilder>();
    }

    [Fact]
    public void the_default_schema_needs_no_ddl()
    {
        var writer = new StringWriter();

        theMigrator.WriteSchemaCreationSql(["PUBLIC", "public"], writer);
        theMigrator.WriteSchemaDropSql(["PUBLIC"], writer);

        writer.ToString().ShouldBeEmpty();
    }

    [Fact]
    public void another_schema_is_refused_rather_than_created()
    {
        Should.Throw<NotSupportedException>(() => theMigrator.WriteSchemaCreationSql(["sales"], new StringWriter()));
        Should.Throw<NotSupportedException>(() => theMigrator.WriteSchemaDropSql(["sales"], new StringWriter()));
    }

    [Fact]
    public void a_script_line_is_an_isql_input()
    {
        theMigrator.ToExecuteScriptLine("patch's.sql").ShouldBe("INPUT 'patch''s.sql';");
    }

    /// <summary>
    ///     A3: isql runs a script in one transaction until it meets a COMMIT, and a guarded block beside
    ///     plain DDL then loses the guarded object at that commit.
    /// </summary>
    [Fact]
    public void a_script_commits_after_every_statement()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AddIndex();

        var writer = new StringWriter();
        theMigrator.WriteScript(writer, (m, w) => table.WriteCreateStatement(m, w));

        var statements = FirebirdScript.Split(writer.ToString());
        statements.Count.ShouldBe(4);
        statements[1].ShouldBe("COMMIT");
        statements[3].ShouldBe("COMMIT");
    }

    [Theory]
    [InlineData("orders")]
    [InlineData("order date")]
    [InlineData("QRTZ_FIRED_TRIGGERS")]
    [InlineData("IDX_QRTZ_FT_INST_JOB_REQ_RCVRY")]
    public void accepts_a_name_that_fits(string name)
    {
        theMigrator.AssertValidIdentifier(name);
        theMigrator.AssertValidLocalIdentifier(name);
    }

    [Theory]
    [InlineData("a\"b", "double quote")]
    [InlineData("a^b", "'^'")]
    [InlineData("a;b", "semicolon")]
    [InlineData("a'b", "single quote")]
    [InlineData(" a", "whitespace")]
    [InlineData("", "empty")]
    public void refuses_a_name_that_cannot_be_written_safely(string name, string reason)
    {
        Should.Throw<InvalidOperationException>(() => theMigrator.AssertValidIdentifier(name))
            .Message.ShouldContain(reason);
    }

    /// <summary>
    ///     A6: Firebird 3's limit is 31 bytes for every kind of object, and the server refuses a longer
    ///     name rather than truncating it.
    /// </summary>
    [Fact]
    public void refuses_a_name_over_31_bytes_by_default()
    {
        theMigrator.AssertValidIdentifier(new string('a', 31));

        var ex = Should.Throw<InvalidOperationException>(() => theMigrator.AssertValidIdentifier(new string('a', 32)));
        ex.Message.ShouldContain("32 bytes");
        ex.Message.ShouldContain("MaxIdentifierLength");
    }

    [Fact]
    public void counts_bytes_not_characters_at_31()
    {
        // 15 two-byte characters and one ASCII: 16 characters, 31 bytes -- and one more is over.
        theMigrator.AssertValidIdentifier(new string('Ä', 15) + "A");
        Should.Throw<InvalidOperationException>(() => theMigrator.AssertValidIdentifier(new string('Ä', 16)));
    }

    [Fact]
    public void a_column_name_is_held_to_the_limit_too()
    {
        Should.Throw<InvalidOperationException>(() => theMigrator.AssertValidLocalIdentifier(new string('a', 32)));
    }

    [Fact]
    public void counts_characters_at_firebird_4s_63()
    {
        var migrator = new FirebirdMigrator { MaxIdentifierLength = 63 };

        migrator.AssertValidIdentifier(new string('Ä', 63));
        Should.Throw<InvalidOperationException>(() => migrator.AssertValidIdentifier(new string('a', 64)))
            .Message.ShouldContain("64 characters");
    }

    [Theory]
    [InlineData("RDB$DB_KEY", true)]
    [InlineData("rdb$record_version", true)]
    [InlineData("id", false)]
    [InlineData("RDB$SOMETHING", false)]
    public void knows_the_pseudo_columns_every_table_has(string column, bool system)
    {
        theMigrator.IsSystemColumn(column).ShouldBe(system);
    }

    [Fact]
    public void the_ddl_transaction_waits_for_locks()
    {
        var options = new FirebirdMigrator { LockTimeout = TimeSpan.FromSeconds(3) }.DdlTransactionOptions();

        options.TransactionBehavior.HasFlag(FbTransactionBehavior.Wait).ShouldBeTrue();
        options.TransactionBehavior.HasFlag(FbTransactionBehavior.NoWait).ShouldBeFalse();
        options.WaitTimeout.ShouldBe(TimeSpan.FromSeconds(3));
    }

    /// <summary>
    ///     Everything is rendered before anything runs, so a refusal comes before the first statement.
    /// </summary>
    [Fact]
    public void a_migration_is_rendered_whole_and_a_refusal_comes_first()
    {
        var good = new Table("good");
        good.AddColumn<int>("id").AsPrimaryKey();

        var mixed = new Table("mixed");
        mixed.AddColumn<int>("a");
        mixed.AddColumn<int>("b");
        var index = new IndexDefinition("idx_mixed") { Columns = ["a", "b"] };
        index.DescendingColumns.Add("b");
        mixed.Indexes.Add(index);

        var migration = new SchemaMigration([new TableDelta(good, null), new TableDelta(mixed, null)]);

        Should.Throw<InvalidOperationException>(() => theMigrator.RenderStatements(migration))
            .Message.ShouldContain("mixes directions");
    }

    [Fact]
    public void rendering_splits_every_delta_into_statements()
    {
        var table = new Table("people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AddIndex();

        theMigrator.RenderStatements(new SchemaMigration(new TableDelta(table, null))).Count.ShouldBe(2);
    }

    [Fact]
    public void creates_a_database_named_for_the_file()
    {
        var database = theMigrator.CreateDatabase(
            new FbConnection("DataSource=localhost;Database=/var/lib/firebird/data/orders.fdb;User=SYSDBA;Password=pw"));

        database.ShouldBeOfType<DatabaseWithTables>().Identifier.ShouldBe("orders");
    }

    [Fact]
    public void creating_a_database_needs_a_firebird_connection()
    {
        Should.Throw<ArgumentException>(() =>
            theMigrator.CreateDatabase(Substitute.For<DbConnection>()));
    }

    [Fact]
    public void no_tables_to_delete_is_no_sql()
    {
        theMigrator.GenerateDeleteAllSql([]).ShouldBeEmpty();
    }

    /// <summary>
    ///     A8: one command, because DatabaseCleaner runs the result as one.
    /// </summary>
    [Fact]
    public void deleting_everything_is_one_execute_block_children_first()
    {
        var sql = theMigrator.GenerateDeleteAllSql([new FirebirdObjectName("children"), new FirebirdObjectName("parents")]);

        sql.ShouldStartWith("EXECUTE BLOCK AS");
        sql.ShouldEndWith("END");
        FirebirdScript.FindOutsideLiterals(sql, "^").ShouldBe(-1);
        sql.IndexOf("'CHILDREN'", StringComparison.Ordinal)
            .ShouldBeLessThan(sql.IndexOf("'PARENTS'", StringComparison.Ordinal));
        sql.ShouldContain("RESTART WITH");
        sql.ShouldContain("STARTING WITH '3.', 0, 1");
    }

    [Fact]
    public void deleting_everything_can_leave_identities_alone()
    {
        theMigrator.GenerateDeleteAllSql([new FirebirdObjectName("children")], resetIdentity: false)
            .ShouldNotContain("RESTART WITH");
    }

    [Fact]
    public void deleting_from_another_schema_is_refused()
    {
        Should.Throw<NotSupportedException>(() =>
            theMigrator.GenerateDeleteAllSql([new FirebirdObjectName("sales", "orders")]));
    }

    [Fact]
    public async Task nothing_to_apply_runs_nothing()
    {
        var migration = new SchemaMigration([]);

        await theMigrator.ApplyAllAsync(new FbConnection(), migration, AutoCreate.CreateOrUpdate);
    }
}
