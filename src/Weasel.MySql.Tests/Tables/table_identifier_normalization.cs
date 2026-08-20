using Shouldly;
using Weasel.Core;
using Weasel.MySql.Tables;
using Xunit;

namespace Weasel.MySql.Tests.Tables;

/// <summary>
///     A Table built from a plain <see cref="DbObjectName" /> used to keep it, so its
///     qualified name rendered unquoted while everything read back out of the catalog
///     came back as a backtick-quoted <see cref="MySqlObjectName" />. DbObjectName
///     equality compares those qualified names, so the two never matched and a foreign
///     key built that way reported drift forever. See wolverine#3983.
/// </summary>
public class table_identifier_normalization: IntegrationContext
{
    [Fact]
    public void identifier_is_normalized_from_a_plain_db_object_name()
    {
        var table = new Table(new DbObjectName("weasel_testing", "normalize_1"));

        table.Identifier.ShouldBeOfType<MySqlObjectName>();
        table.Identifier.QualifiedName.ShouldBe("`weasel_testing`.`normalize_1`");
    }

    [Fact]
    public void a_plain_db_object_name_equals_the_parsed_form()
    {
        new Table(new DbObjectName("weasel_testing", "normalize_2")).Identifier
            .ShouldBe(new Table("weasel_testing.normalize_2").Identifier);
    }

    [Fact]
    public void foreign_key_linked_table_is_normalized()
    {
        var table = new Table(new DbObjectName("weasel_testing", "normalize_3"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("parent_id")
            .ForeignKeyTo(new DbObjectName("weasel_testing", "normalize_parent"), "id");

        table.ForeignKeys.Single().LinkedTable.ShouldBeOfType<MySqlObjectName>();
    }

    [Fact]
    public async Task a_foreign_key_declared_with_a_plain_db_object_name_reports_no_drift()
    {
        await DropTableAsync("`weasel_testing`.`normalize_child`");
        await DropTableAsync("`weasel_testing`.`normalize_parent`");

        var parent = new Table(new DbObjectName("weasel_testing", "normalize_parent"));
        parent.AddColumn<int>("id").AsPrimaryKey();
        await parent.CreateAsync(theConnection);

        var child = new Table(new DbObjectName("weasel_testing", "normalize_child"));
        child.AddColumn<int>("id").AsPrimaryKey();
        child.AddColumn<int>("parent_id")
            .ForeignKeyTo(new DbObjectName("weasel_testing", "normalize_parent"), "id",
                onDelete: CascadeAction.Cascade);

        await child.CreateAsync(theConnection);

        var delta = await child.FindDeltaAsync(theConnection) as TableDelta;

        delta!.ForeignKeys!.Different.ShouldBeEmpty();
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
