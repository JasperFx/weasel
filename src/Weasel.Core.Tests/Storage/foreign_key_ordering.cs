using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class foreign_key_ordering
{
    // child -> parent -> grandparent; "loose" references nothing; "orphan" references a table not in the set.
    private static readonly Dictionary<string, string[]> theReferences = new()
    {
        ["child"] = ["parent"],
        ["parent"] = ["grandparent"],
        ["grandparent"] = [],
        ["loose"] = [],
        ["orphan"] = ["elsewhere"]
    };

    private static IEnumerable<string> referencesOf(string table) => theReferences[table];

    [Fact]
    public void referenced_first_is_creation_order()
    {
        ForeignKeyOrdering.ReferencedFirst(["child", "loose", "parent", "grandparent", "orphan"], referencesOf)
            .ShouldBe(["grandparent", "parent", "child", "loose", "orphan"]);
    }

    [Fact]
    public void referencing_first_is_deletion_order()
    {
        ForeignKeyOrdering.ReferencingFirst(["grandparent", "loose", "parent", "child", "orphan"], referencesOf)
            .ShouldBe(["child", "parent", "grandparent", "loose", "orphan"]);
    }

    [Fact]
    public void unrelated_items_keep_their_input_order()
    {
        ForeignKeyOrdering.ReferencedFirst(["orphan", "loose"], referencesOf).ShouldBe(["orphan", "loose"]);
        ForeignKeyOrdering.ReferencingFirst(["orphan", "loose"], referencesOf).ShouldBe(["orphan", "loose"]);
    }

    [Fact]
    public void a_diamond_puts_the_shared_target_on_the_right_side_of_both()
    {
        var references = new Dictionary<string, string[]>
        {
            ["a"] = ["b", "c"], ["b"] = ["d"], ["c"] = ["d"], ["d"] = []
        };

        var created = ForeignKeyOrdering.ReferencedFirst(["a", "b", "c", "d"], x => references[x]);
        created.First().ShouldBe("d");
        created.Last().ShouldBe("a");

        var deleted = ForeignKeyOrdering.ReferencingFirst(["d", "c", "b", "a"], x => references[x]);
        deleted.First().ShouldBe("a");
        deleted.Last().ShouldBe("d");
    }

    [Fact]
    public void a_cycle_terminates_and_keeps_every_item()
    {
        var references = new Dictionary<string, string[]> { ["a"] = ["b"], ["b"] = ["a"], ["c"] = ["c"] };

        ForeignKeyOrdering.ReferencedFirst(["a", "b", "c"], x => references[x]).ShouldBe(["b", "a", "c"]);
        ForeignKeyOrdering.ReferencingFirst(["a", "b", "c"], x => references[x]).ShouldBe(["b", "a", "c"]);
    }

    [Fact]
    public void identity_follows_the_comparer_and_duplicates_collapse()
    {
        ForeignKeyOrdering.ReferencedFirst(["CHILD", "Parent", "parent"],
                x => x.Equals("child", StringComparison.OrdinalIgnoreCase) ? ["PARENT"] : [],
                StringComparer.OrdinalIgnoreCase)
            .ShouldBe(["Parent", "CHILD"]);
    }

    [Fact]
    public void document_types_order_by_their_references()
    {
        var references = new Dictionary<Type, Type[]>
        {
            [typeof(int)] = [typeof(string)], [typeof(string)] = []
        };

        ForeignKeyOrdering.ReferencedFirst([typeof(int), typeof(string)], x => references[x])
            .ShouldBe([typeof(string), typeof(int)]);
    }
}
