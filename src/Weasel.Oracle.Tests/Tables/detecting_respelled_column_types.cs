using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

/// <summary>
///     Oracle does not store a column type the way it was declared. The ANSI names are rewritten --
///     <c>NUMERIC(13,4)</c> and <c>DECIMAL(13,4)</c> come back as <c>NUMBER</c>, <c>INTEGER</c> as
///     <c>NUMBER</c> with scale 0, <c>REAL</c> as <c>FLOAT</c>, <c>VARCHAR</c> as <c>VARCHAR2</c>, an
///     <c>INTERVAL</c> with its precisions filled in -- and a character column declared in characters reports its
///     size in bytes in <c>DATA_LENGTH</c>. A model that said any of those compared unequal to the
///     column Oracle created from it, so the table drifted forever: every migration issued the same
///     <c>ALTER TABLE … MODIFY</c> and every assert reported a difference.
/// </summary>
public class detecting_respelled_column_types: IntegrationContext
{
    private const string SchemaName = "respelled";

    public detecting_respelled_column_types(): base(SchemaName)
    {
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static Table TableWith(string columnType)
    {
        var table = new Table($"{SchemaName}.respelled");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("value", columnType);
        return table;
    }

    [Theory]
    [InlineData("NUMERIC(13,4)")]
    [InlineData("NUMERIC(10)")]
    [InlineData("NUMERIC")]
    [InlineData("DECIMAL(13,4)")]
    [InlineData("DECIMAL")]
    [InlineData("DEC(10,2)")]
    [InlineData("INTEGER")]
    [InlineData("INT")]
    [InlineData("SMALLINT")]
    [InlineData("REAL")]
    [InlineData("VARCHAR(50)")]
    [InlineData("CHARACTER(10)")]
    [InlineData("CHARACTER VARYING(50)")]
    [InlineData("VARCHAR2(100 CHAR)")]
    [InlineData("VARCHAR2(100 BYTE)")]
    [InlineData("CHAR(10 CHAR)")]
    [InlineData("NVARCHAR2(100)")]
    [InlineData("NCHAR(10)")]
    [InlineData("INTERVAL DAY TO SECOND")]
    [InlineData("INTERVAL DAY(3) TO SECOND(2)")]
    [InlineData("INTERVAL YEAR TO MONTH")]
    [InlineData("TIMESTAMP(3)")]
    [InlineData("TIMESTAMP WITH LOCAL TIME ZONE")]
    public async Task a_column_created_from_the_model_matches_the_model(string columnType)
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(columnType));

        var delta = await TableWith(columnType).FindDeltaAsync(theConnection, Ct);

        delta.Columns.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_migration_path_settles_on_a_timespan_column()
    {
        // INTERVAL DAY TO SECOND is what a TimeSpan maps to, so this was not an exotic spelling:
        // every TimeSpan column migrated on every run.
        await ResetSchema();

        static Table Durations()
        {
            var table = new Table($"{SchemaName}.durations");
            table.AddColumn<int>("id").AsPrimaryKey();
            table.AddColumn<TimeSpan>("timeout");
            table.AddColumn<decimal>("amount");
            return table;
        }

        (await Durations().MigrateAsync(theConnection)).ShouldBeTrue();
        (await Durations().MigrateAsync(theConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_widened_column_declared_through_a_multi_word_synonym_is_detected_and_settles()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith("CHARACTER VARYING(100)"));

        var widened = TableWith("CHARACTER VARYING(200)");
        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await widened.ApplyChangesAsync(theConnection, Ct);

        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Theory]
    [InlineData("BINARY_DOUBLE", "DOUBLE PRECISION", "1.5")]
    [InlineData("TIMESTAMP", "TIMESTAMP(3) WITH TIME ZONE", "SYSTIMESTAMP")]
    public async Task a_populated_column_that_matched_its_model_before_is_left_alone(
        string existingType, string declaredType, string value)
    {
        // Both pairs compared equal before the stored-name comparison. Reporting them now would issue
        // an ALTER ... MODIFY that Oracle refuses on a populated column (ORA-01439).
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(existingType));
        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.respelled (id, value) VALUES (1, {value})")
            .ExecuteNonQueryAsync(Ct);

        var model = TableWith(declaredType);
        (await model.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);

        await model.ApplyChangesAsync(theConnection, Ct);
    }

    [Theory]
    [InlineData("VARCHAR2(100 CHAR)", "VARCHAR2(400)", "'x'")]
    [InlineData("CHAR(10 CHAR)", "CHAR(40)", "'abc'")]
    [InlineData("NCHAR(10)", "NCHAR(20)", "N'abc'")]
    [InlineData("NVARCHAR2(2000)", "NVARCHAR2(4000)", "N'x'")]
    [InlineData("NCHAR(1000)", "NCHAR(2000)", "N'abc'")]
    public async Task a_column_sized_in_characters_matches_its_size_in_characters_or_in_bytes(
        string existingType, string byteSizedModel, string value)
    {
        // Before the reader took CHAR_LENGTH it reported DATA_LENGTH, the size in bytes, and a model
        // stating that matched. It must still: the MODIFY to the byte size re-pads every stored CHAR
        // value, and NVARCHAR2(4000) / NCHAR(2000) are ORA-00910 on every migration. The size in
        // characters is the drift this fixes.
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(existingType));
        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.respelled (id, value) VALUES (1, {value})")
            .ExecuteNonQueryAsync(Ct);

        async Task<object?> storedLength() => await theConnection
            .CreateCommand($"SELECT LENGTH(value) FROM {SchemaName}.respelled WHERE id = 1")
            .ExecuteScalarAsync(Ct);

        var before = await storedLength();

        (await TableWith(byteSizedModel).FindDeltaAsync(theConnection, Ct)).Difference
            .ShouldBe(SchemaPatchDifference.None);
        (await TableWith(byteSizedModel).MigrateAsync(theConnection, Ct)).ShouldBeFalse();

        (await TableWith(existingType).FindDeltaAsync(theConnection, Ct)).Difference
            .ShouldBe(SchemaPatchDifference.None);
        (await TableWith(existingType).MigrateAsync(theConnection, Ct)).ShouldBeFalse();

        (await storedLength()).ShouldBe(before);
    }

    [Theory]
    [InlineData("NVARCHAR2(100)", "NATIONAL CHARACTER VARYING(200)", "N'x'", "NVARCHAR2(200)")]
    [InlineData("NVARCHAR2(100)", "NCHAR VARYING(200)", "N'x'", "NVARCHAR2(200)")]
    [InlineData("VARCHAR2(100 CHAR)", "CHARACTER VARYING(400 CHAR)", "'x'", "VARCHAR2(400 CHAR)")]
    [InlineData("VARCHAR2(100 CHAR)", "VARCHAR(400 CHAR)", "'x'", "VARCHAR2(400 CHAR)")]
    [InlineData("NCHAR(10)", "NATIONAL CHAR(20)", "N'abc'", "NCHAR(20)")]
    [InlineData("NCHAR(10)", "NATIONAL CHARACTER(20)", "N'abc'", "NCHAR(20)")]
    public async Task a_widening_declared_through_a_synonym_is_detected_even_at_the_byte_size(
        string existingType, string widenedModel, string value, string widenedType)
    {
        // The new length is the column's size in bytes, which a stored spelling is excused for. A
        // synonym never matched on master, so master's MODIFY widened the column; it still has to.
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith(existingType));
        await theConnection.CreateCommand($"INSERT INTO {SchemaName}.respelled (id, value) VALUES (1, {value})")
            .ExecuteNonQueryAsync(Ct);

        var model = TableWith(widenedModel);
        (await model.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await model.ApplyChangesAsync(theConnection, Ct);

        (await model.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
        (await model.FetchExistingAsync(theConnection, Ct))!.ColumnFor("value")!.Type.ShouldBe(widenedType);
    }

    [Fact]
    public async Task a_real_type_change_is_still_detected()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith("NUMERIC(13,4)"));

        var delta = await TableWith("VARCHAR2(20)").FindDeltaAsync(theConnection, Ct);

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task a_widened_character_column_declared_in_characters_is_detected_and_settles()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith("VARCHAR2(100 CHAR)"));

        var widened = TableWith("VARCHAR2(200 CHAR)");
        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await widened.ApplyChangesAsync(theConnection, Ct);

        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_widened_national_character_column_is_detected_and_settles()
    {
        await ResetSchema();

        await CreateSchemaObjectInDatabase(TableWith("NVARCHAR2(100)"));

        var widened = TableWith("NVARCHAR2(150)");
        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.Update);

        await widened.ApplyChangesAsync(theConnection, Ct);

        (await widened.FindDeltaAsync(theConnection, Ct)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task an_existing_character_column_reads_back_in_the_semantics_it_was_declared_in()
    {
        await ResetSchema();

        var table = new Table($"{SchemaName}.semantics");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn("in_chars", "VARCHAR2(100 CHAR)");
        table.AddColumn("in_bytes", "VARCHAR2(100)");
        table.AddColumn("national", "NVARCHAR2(100)");
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection, Ct);

        existing.ShouldNotBeNull();
        existing.ColumnFor("in_chars")!.Type.ShouldBe("VARCHAR2(100 CHAR)");
        existing.ColumnFor("in_bytes")!.Type.ShouldBe("VARCHAR2(100)");
        existing.ColumnFor("national")!.Type.ShouldBe("NVARCHAR2(100)");
    }
}
