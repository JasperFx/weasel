using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Functions;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Weasel.Postgresql.Views;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class KeywordIdentifierTests
{
    private static string QuoteName(string name,
        SchemaUtils.IdentifierUsage usage = SchemaUtils.IdentifierUsage.General)
    {
        return SchemaUtils.QuoteName(name, usage);
    }

    private static string QuoteQualifiedName(string schema, string name,
        SchemaUtils.IdentifierUsage usage = SchemaUtils.IdentifierUsage.General)
    {
        return $"{QuoteName(schema, usage)}.{QuoteName(name, usage)}";
    }

    [Fact]
    public void function_time_identifier_is_quoted()
    {
        var function = new Function(new DbObjectName("time", "time"), "select 1;");
        // "time" is a Category C keyword, quoting depends on Function context
        var expected = QuoteQualifiedName("time", "time", SchemaUtils.IdentifierUsage.Function);
        function.Identifier.QualifiedName.ShouldBe(expected);
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    [InlineData("authorization")]
    [InlineData("MyTable")]
    public void table_ddl_quotes_keyword_schema_and_name(string keyword)
    {
        var table = new Table(new DbObjectName(keyword, keyword));
        table.AddColumn<int>("id");

        var ddl = table.ToBasicCreateTableSql();

        ddl.ShouldContain($"CREATE TABLE IF NOT EXISTS {QuoteQualifiedName(keyword, keyword)}");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    [InlineData("authorization")]
    [InlineData("MyTable")]
    public void view_ddl_quotes_keyword_schema_and_name(string keyword)
    {
        var view = new View(new DbObjectName(keyword, keyword), "SELECT 1 AS id");

        var ddl = view.ToBasicCreateViewSql();

        var expected = QuoteQualifiedName(keyword, keyword);
        ddl.ShouldContain($"DROP VIEW IF EXISTS {expected};");
        ddl.ShouldContain($"CREATE VIEW {expected} AS SELECT 1 AS id;");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    [InlineData("authorization")]
    [InlineData("MyTable")]
    public void sequence_ddl_quotes_keyword_schema_and_name(string keyword)
    {
        var sequence = new Sequence(new DbObjectName(keyword, keyword));
        var writer = new StringWriter();

        sequence.WriteCreateStatement(new PostgresqlMigrator(), writer);

        writer.ToString().ShouldContain($"CREATE SEQUENCE {QuoteQualifiedName(keyword, keyword)};");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    [InlineData("authorization")]
    [InlineData("MyTable")]
    public void function_identifier_quotes_keyword_schema_and_name(string keyword)
    {
        var function = new Function(new DbObjectName(keyword, keyword), "select 1;");

        var expected = QuoteQualifiedName(keyword, keyword, SchemaUtils.IdentifierUsage.Function);
        function.Identifier.QualifiedName.ShouldBe(expected);
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    [InlineData("authorization")]
    [InlineData("MyTable")]
    public void index_ddl_quotes_keyword_schema_and_name(string keyword)
    {
        var table = new Table(new DbObjectName(keyword, keyword));
        table.AddColumn<int>("id");

        var index = new IndexDefinition(keyword)
        {
            Columns = new[] { "id" }
        };

        var ddl = index.ToDDL(table);

        var expectedIndexName = SchemaUtils.QuoteName(keyword);
        var expectedTableName = QuoteQualifiedName(keyword, keyword);
        ddl.ShouldContain($"CREATE INDEX {expectedIndexName} ON {expectedTableName}");
    }

    [Theory]
    [InlineData("order", "one", "two")]
    [InlineData("between", "one", "two")]
    [InlineData("authorization", "one", "two")]
    public void hash_partition_ddl_quotes_keyword_table_names(string keyword, params string[] suffixes)
    {
        var table = new Table(new DbObjectName(keyword, keyword));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("role")
            .PartitionByHash(suffixes);

        var sql = table.ToCreateSql(new PostgresqlMigrator());

        var expectedParent = QuoteQualifiedName(keyword, keyword);
        var expectedPartition = QuoteQualifiedName(keyword, keyword + "_" + suffixes[0]);
        sql.ShouldContain($"create table {expectedPartition} partition of {expectedParent}");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    public void list_partition_ddl_quotes_keyword_table_names(string keyword)
    {
        var table = new Table(new DbObjectName(keyword, keyword));
        table.AddColumn<int>("id").AsPrimaryKey();
        var partitioning = table.PartitionByList("id");
        partitioning.AddPartition("a", "'val1'");

        var sql = table.ToCreateSql(new PostgresqlMigrator());

        var expectedParent = QuoteQualifiedName(keyword, keyword);
        var expectedPartition = QuoteQualifiedName(keyword, keyword + "_a");
        sql.ShouldContain($"CREATE TABLE {expectedPartition} partition of {expectedParent}");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    public void range_partition_ddl_quotes_keyword_table_names(string keyword)
    {
        var table = new Table(new DbObjectName(keyword, keyword));
        table.AddColumn<int>("id").AsPrimaryKey();
        var partitioning = table.PartitionByRange("id");
        partitioning.AddRange("a", "1", "100");

        var sql = table.ToCreateSql(new PostgresqlMigrator());

        var expectedParent = QuoteQualifiedName(keyword, keyword);
        var expectedPartition = QuoteQualifiedName(keyword, keyword + "_a");
        sql.ShouldContain($"CREATE TABLE {expectedPartition} PARTITION OF {expectedParent}");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    public void default_partition_ddl_quotes_keyword_table_names(string keyword)
    {
        var writer = new StringWriter();
        var identifier = new DbObjectName(keyword, keyword);

        writer.WriteDefaultPartition(identifier);

        var sql = writer.ToString();
        var expectedParent = QuoteQualifiedName(keyword, keyword);
        var expectedPartition = QuoteQualifiedName(keyword, keyword + "_default");
        sql.ShouldContain($"CREATE TABLE {expectedPartition} PARTITION OF {expectedParent} DEFAULT;");
    }

    [Theory]
    [InlineData("order")]
    [InlineData("between")]
    public void partition_rebuild_temp_table_uses_correct_quoting(string keyword)
    {
        var tempName = PostgresqlObjectName.From(
            new DbObjectName(keyword, keyword + "_temp"));

        var expected = QuoteQualifiedName(keyword, keyword + "_temp");
        tempName.ToString().ShouldBe(expected);
    }
}
