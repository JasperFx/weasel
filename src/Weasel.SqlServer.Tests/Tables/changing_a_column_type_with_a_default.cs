using System.Text.RegularExpressions;
using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     A column whose type changes while a default is involved. <c>AlterColumnTypeSql</c> used to write
///     the whole column declaration into <c>ALTER COLUMN</c>, which takes only a type and nullability:
/// </summary>
/// <remarks>
///     <code>
///     Incorrect syntax near the keyword 'DEFAULT'.
///     The object 'DF__scores__score__5596676D' is dependent on column 'score'.
///     </code>
///     <para>
///         The first when the model declares a default, the second when the database already has one
///         bound to the column.
///     </para>
/// </remarks>
public class changing_a_column_type_with_a_default: IntegrationContext
{
    private const string Schema = "type_default";

    public changing_a_column_type_with_a_default(): base(Schema)
    {
    }

    private static Table scores(string type, int? defaultValue)
    {
        var table = new Table(new SqlServerObjectName(Schema, "scores"));
        table.AddColumn<int>("id").AsPrimaryKey();
        var score = table.AddColumn("score", type);
        if (defaultValue.HasValue) score.DefaultValue(defaultValue.Value);

        return table;
    }

    private async Task migrate(Table table)
    {
        var migration = await SchemaMigration.DetermineAsync(theConnection, table);
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);
    }

    private async Task<string?> defaultDefinitionAsync()
    {
        var result = await theConnection.CreateCommand(
                "select dc.definition from sys.default_constraints dc "
                + "inner join sys.columns c on c.object_id = dc.parent_object_id and c.column_id = dc.parent_column_id "
                + $"where dc.parent_object_id = object_id('{Schema}.scores') and c.name = 'score'")
            .ExecuteScalarAsync();

        return result as string;
    }

    [Fact]
    public async Task the_type_changes_when_the_model_declares_a_default()
    {
        await ResetSchema();
        await migrate(scores("int", defaultValue: null));

        var after = scores("bigint", defaultValue: 0);
        await migrate(after);

        (await defaultDefinitionAsync()).ShouldBe("((0))");
        (await SchemaMigration.DetermineAsync(theConnection, after)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_type_changes_when_the_column_already_has_a_default()
    {
        await ResetSchema();
        await migrate(scores("int", defaultValue: 0));

        var after = scores("bigint", defaultValue: 0);
        await migrate(after);

        (await defaultDefinitionAsync()).ShouldBe("((0))");
        (await SchemaMigration.DetermineAsync(theConnection, after)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task the_default_goes_when_the_model_no_longer_declares_one()
    {
        await ResetSchema();
        await migrate(scores("int", defaultValue: 0));

        var after = scores("bigint", defaultValue: null);
        await migrate(after);

        (await defaultDefinitionAsync()).ShouldBeNull();
        (await SchemaMigration.DetermineAsync(theConnection, after)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     A default the model adds during a type change: the rendered update has to survive a second
    ///     run, which finds the default the first run bound and so has to drop it before the
    ///     <c>ALTER COLUMN</c> and the re-add ("Column already has a DEFAULT bound to it").
    /// </summary>
    [Theory]
    [InlineData(null, 7)]
    [InlineData(5, 7)]
    [InlineData(5, null)]
    public async Task the_rendered_update_runs_twice(int? before, int? after)
    {
        await ResetSchema();
        await migrate(scores("int", before));

        var expected = scores("bigint", after);
        var migration = await SchemaMigration.DetermineAsync(theConnection, expected);
        var writer = new StringWriter();
        migration.WriteAllUpdates(writer, new SqlServerMigrator(), AutoCreate.CreateOrUpdate);
        var script = writer.ToString();

        await executeInBatches(script);
        await executeInBatches(script);

        (await SchemaMigration.DetermineAsync(theConnection, expected)).Difference.ShouldBe(SchemaPatchDifference.None);
    }

    private async Task executeInBatches(string script)
    {
        foreach (var batch in Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await theConnection.CreateCommand(batch).ExecuteNonQueryAsync();
        }
    }
}
