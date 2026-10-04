using Weasel.Core.Migrations;
using Weasel.Postgresql;
using Weasel.SqlServer;
using Weasel.Postgresql.Tables;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     weasel#677. A creation script runs top to bottom, so it has to create a referenced table
///     before the table referencing it. The migration path has deferred foreign keys to tables
///     created later since <see cref="SchemaMigration" /> gained
///     <c>ISchemaObjectDeltaWithDeferrableForeignKeys</c>; the script path wrote objects in yield
///     order and had no equivalent.
/// </summary>
/// <remarks>
///     These are over the ordering itself. That the resulting script actually runs is a separate
///     question, and a more important one — every assertion short of executing it passes over a
///     script whose first statement fails, which is exactly how this survived in Polecat
///     (JasperFx/polecat#684). The executing tests live with the providers:
///     <c>generated_script_runs_in_dependency_order</c> in the PostgreSQL, SQL Server and SQLite
///     suites.
/// </remarks>
public class schema_object_dependency_order
{
    private static Table table(string name, params string[] references)
    {
        var result = new Table(new PostgresqlObjectName("dep", name));
        result.AddColumn<Guid>("id").AsPrimaryKey();

        foreach (var reference in references)
        {
            result.AddColumn<Guid>($"{reference}_id")
                .ForeignKeyTo(new PostgresqlObjectName("dep", reference), "id");
        }

        return result;
    }

    private static string[] namesOf(IEnumerable<ISchemaObject> objects)
        => objects.Select(x => x.Identifier.Name).ToArray();

    [Fact]
    public void puts_a_referenced_table_before_the_table_referencing_it()
    {
        // Declared in exactly the wrong order: child first
        var objects = new ISchemaObject[] { table("child", "parent"), table("parent") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["parent", "child"]);
    }

    [Fact]
    public void leaves_an_order_that_is_already_valid_alone()
    {
        var objects = new ISchemaObject[] { table("parent"), table("child", "parent") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["parent", "child"]);
    }

    [Fact]
    public void resolves_a_chain()
    {
        var objects = new ISchemaObject[]
        {
            table("grandchild", "child"), table("child", "parent"), table("parent")
        };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["parent", "child", "grandchild"]);
    }

    /// <summary>
    ///     Unrelated objects keep their original relative order, so a script's shape only changes
    ///     where it had to.
    /// </summary>
    [Fact]
    public void is_stable_for_objects_with_no_relationship()
    {
        var objects = new ISchemaObject[] { table("c"), table("a"), table("b") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["c", "a", "b"]);
    }

    /// <summary>
    ///     A self-referencing key is satisfied by the table's own creation, so it is not an ordering
    ///     constraint — and must not be mistaken for a cycle.
    /// </summary>
    [Fact]
    public void a_self_reference_is_not_an_edge()
    {
        var objects = new ISchemaObject[] { table("other"), table("tree", "tree") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["other", "tree"]);
    }

    /// <summary>
    ///     Two tables referencing each other have no valid creation order, so there is nothing to
    ///     sort them into. It must not throw and must not drop either one — see the remarks on
    ///     <see cref="SchemaObjectOrdering" /> for why this is left rather than refused.
    /// </summary>
    [Fact]
    public void a_mutual_reference_is_left_in_encounter_order()
    {
        var objects = new ISchemaObject[] { table("a", "b"), table("b", "a"), table("c") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["b", "a", "c"]);
    }

    [Fact]
    public void ignores_a_reference_to_something_this_script_does_not_create()
    {
        var objects = new ISchemaObject[] { table("child", "elsewhere"), table("parent") };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["child", "parent"]);
    }

    /// <summary>
    ///     Names are matched on <see cref="DbObjectName.QualifiedName" /> with an
    ///     <see cref="StringComparer.OrdinalIgnoreCase" /> comparer, the same way
    ///     <see cref="SchemaMigration" /> matches them when it defers a foreign key. Keeping the two
    ///     paths on one rule is the point; it also means a name whose <i>spelling</i> differs is a
    ///     different object, which on PostgreSQL is not a technicality — a table created as
    ///     <c>"PARENT"</c> really is a different table from <c>parent</c>, and
    ///     <c>ToQualifiedName</c> quotes the first and not the second, so they do not match. Nothing
    ///     is lost by not matching them: a reference the script does not create is simply left in
    ///     place, which is the right answer for a table it is not creating.
    /// </summary>
    [Fact]
    public void matches_a_name_whose_qualified_spelling_differs_only_in_case()
    {
        // SQL Server folds neither but compares case-insensitively, so these two really are one table
        var child = new SqlServer.Tables.Table(new SqlServerObjectName("dep", "child"));
        child.AddColumn<Guid>("id").AsPrimaryKey();
        child.AddColumn<Guid>("parent_id")
            .ForeignKeyTo(new SqlServerObjectName("dep", "PARENT"), "id");

        var parent = new SqlServer.Tables.Table(new SqlServerObjectName("dep", "parent"));
        parent.AddColumn<Guid>("id").AsPrimaryKey();

        namesOf(SchemaObjectOrdering.InDependencyOrder(new ISchemaObject[] { child, parent }))
            .ShouldBe(["parent", "child"]);
    }

    /// <summary>
    ///     A dependency that is not a foreign key — a trigger writing to another table, a view
    ///     selecting from one — declares itself. Polecat hit this moving its full-text index onto
    ///     schema objects (JasperFx/polecat#685) and had to hand-order the pair.
    /// </summary>
    [Fact]
    public void honours_a_declared_non_foreign_key_dependency()
    {
        var objects = new ISchemaObject[]
        {
            new DependentObject("trigger", new PostgresqlObjectName("dep", "tokens")),
            table("tokens")
        };

        namesOf(SchemaObjectOrdering.InDependencyOrder(objects))
            .ShouldBe(["tokens", "trigger"]);
    }

    [Fact]
    public void an_empty_or_single_set_is_returned_as_is()
    {
        SchemaObjectOrdering.InDependencyOrder(Array.Empty<ISchemaObject>()).ShouldBeEmpty();

        var only = table("one");
        SchemaObjectOrdering.InDependencyOrder(new ISchemaObject[] { only }).Single().ShouldBeSameAs(only);
    }

    // ---- features ----

    private static string[] namesOf(IEnumerable<IFeatureSchema> features)
        => features.Select(x => x.Identifier).ToArray();

    private static IFeatureSchema feature(string identifier, params ISchemaObject[] objects)
        => new StubFeature(identifier, objects);

    /// <summary>
    ///     The reason the per-object sort is not enough on its own: a table's referenced table can
    ///     belong to another feature, and <c>WriteScriptsByTypeAsync</c> puts every feature in its
    ///     own file, so nothing inside a feature can reach across.
    /// </summary>
    [Fact]
    public void orders_features_by_where_their_objects_point()
    {
        var features = new[]
        {
            feature("children", table("child", "parent")),
            feature("parents", table("parent"))
        };

        namesOf(SchemaObjectOrdering.InDependencyOrder(features))
            .ShouldBe(["parents", "children"]);
    }

    /// <summary>
    ///     <see cref="IFeatureSchema.DependentTypes" /> is documented as controlling "the proper
    ///     ordering of object creation or scripting" but was only ever read by the lazy per-feature
    ///     storage path. Honour it here too, so a dependency that is not expressible as an object
    ///     reference still orders.
    /// </summary>
    [Fact]
    public void honours_declared_dependent_types()
    {
        var features = new IFeatureSchema[]
        {
            new StubFeature("second", [table("b")], typeof(string), [typeof(int)]),
            new StubFeature("first", [table("a")], typeof(int))
        };

        namesOf(SchemaObjectOrdering.InDependencyOrder(features)).ShouldBe(["first", "second"]);
    }

    [Fact]
    public void leaves_mutually_dependent_features_in_encounter_order()
    {
        var features = new[]
        {
            feature("a", table("a1", "b1")),
            feature("b", table("b1", "a1"))
        };

        namesOf(SchemaObjectOrdering.InDependencyOrder(features)).ShouldBe(["b", "a"]);
    }

    private class StubFeature: IFeatureSchema
    {
        private readonly Type[] _dependentTypes;

        public StubFeature(string identifier, ISchemaObject[] objects, Type? storageType = null,
            Type[]? dependentTypes = null)
        {
            Identifier = identifier;
            Objects = objects;
            StorageType = storageType ?? typeof(StubFeature);
            _dependentTypes = dependentTypes ?? [];
        }

        public ISchemaObject[] Objects { get; }
        public string Identifier { get; }
        public Migrator Migrator { get; } = new PostgresqlMigrator();
        public Type StorageType { get; }

        public void WritePermissions(Migrator rules, TextWriter writer)
        {
        }

        public IEnumerable<Type> DependentTypes() => _dependentTypes;
    }

    private class DependentObject: ISchemaObject, ISchemaObjectWithDependencies
    {
        private readonly DbObjectName[] _dependsOn;

        public DependentObject(string name, params DbObjectName[] dependsOn)
        {
            Identifier = new PostgresqlObjectName("dep", name);
            _dependsOn = dependsOn;
        }

        public IEnumerable<DbObjectName> DependsOn => _dependsOn;
        public DbObjectName Identifier { get; }

        public void WriteCreateStatement(Migrator migrator, TextWriter writer) => writer.WriteLine("-- stub");
        public void WriteDropStatement(Migrator rules, TextWriter writer) => writer.WriteLine("-- stub");
        public void ConfigureQueryCommand(DbCommandBuilder builder) => throw new NotSupportedException();

        public Task<ISchemaObjectDelta> CreateDeltaAsync(System.Data.Common.DbDataReader reader,
            CancellationToken ct = default) => throw new NotSupportedException();

        public IEnumerable<DbObjectName> AllNames() => [Identifier];
    }
}
