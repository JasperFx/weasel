using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tests.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

/// <summary>
/// Reproduces https://github.com/JasperFx/weasel/issues/224
/// When a table has a mixed-case name (requiring quoting in PostgreSQL)
/// and a unique index, the second migration run should detect the existing
/// index and not try to recreate it.
/// </summary>
[Collection("mixed_case")]
public class mixed_case_table_with_index : IntegrationContext
{
    private readonly Table _table;

    public mixed_case_table_with_index() : base("mixed_case")
    {
        _table = new Table(new DbObjectName("mixed_case", "TestRecords"));
        _table.AddColumn<Guid>("id").AsPrimaryKey();
        _table.AddColumn<Guid>("fake_id");
        _table.AddColumn<int>("year");
        _table.AddColumn<int>("month");

        _table.Indexes.Add(new IndexDefinition("ix_test_records_unique")
        {
            Columns = ["fake_id", "year", "month"],
            IsUnique = true
        });
    }

    public override Task InitializeAsync() => ResetSchema();

    [Fact]
    public async Task should_detect_no_delta_after_creating_table_with_unique_index()
    {
        // First pass: create the table and index
        await CreateSchemaObjectInDatabase(_table);

        // Second pass: should detect no changes needed
        var delta = await _table.FindDeltaAsync(theConnection);

        delta.Indexes.Missing.ShouldBeEmpty(
            "The unique index should be found in the database, not classified as missing");
        delta.Indexes.Different.ShouldBeEmpty(
            "The unique index should match the expected definition");
        delta.HasChanges().ShouldBeFalse(
            "No schema changes should be detected on second run");
    }

    [Fact]
    public async Task should_apply_migration_twice_without_error()
    {
        // First pass: create the table and index
        await CreateSchemaObjectInDatabase(_table);

        // Second pass: should succeed without "relation already exists" error
        await CreateSchemaObjectInDatabase(_table);
    }
}
