using System.Reflection;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     The object type support matrix in <c>docs/core/object-types.md</c>, asserted against the
///     assemblies rather than maintained by hand. A support matrix is worth nothing once it drifts,
///     and it drifts the moment somebody adds an object type and forgets the doc (weasel#454).
/// </summary>
/// <remarks>
///     <para>
///         Deliberately shallow: it checks that a type by the expected name exists in the expected
///         provider and implements <see cref="ISchemaObject" />, not that it works. Whether it works
///         is what every other test in the tree is for. What this catches is the doc going stale in
///         either direction — a ✓ for something nobody built, or a ✗ for something that shipped.
///     </para>
///     <para>
///         When you add an object type to a provider: add it to <see cref="Supported" />, remove it
///         from <see cref="NotSupported" />, and update the table in the doc. The build fails until
///         all three agree.
///     </para>
/// </remarks>
public class object_type_support_matrix
{
    private static Assembly AssemblyFor(string provider)
        => provider switch
        {
            "Postgresql" => typeof(Postgresql.Tables.Table).Assembly,
            "SqlServer" => typeof(SqlServer.Tables.Table).Assembly,
            "Oracle" => typeof(Oracle.Tables.Table).Assembly,
            "MySql" => typeof(MySql.Tables.Table).Assembly,
            "Sqlite" => typeof(Sqlite.Tables.Table).Assembly,
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null)
        };

    /// <summary>
    ///     Every ✓ in the matrix, as (provider, object type, the type's full name).
    /// </summary>
    private static readonly (string Provider, string ObjectType, string TypeName)[] SupportedRows =
    [
        ("Postgresql", "Table", "Weasel.Postgresql.Tables.Table"),
        ("Postgresql", "Sequence", "Weasel.Postgresql.Sequence"),
        ("Postgresql", "View", "Weasel.Postgresql.Views.View"),
        ("Postgresql", "MaterializedView", "Weasel.Postgresql.Views.MaterializedView"),
        ("Postgresql", "Function", "Weasel.Postgresql.Functions.Function"),
        ("Postgresql", "Extension", "Weasel.Postgresql.Extension"),
        ("Postgresql", "Trigger", "Weasel.Postgresql.Triggers.Trigger"),

        ("SqlServer", "Table", "Weasel.SqlServer.Tables.Table"),
        ("SqlServer", "Sequence", "Weasel.SqlServer.Sequence"),
        ("SqlServer", "View", "Weasel.SqlServer.Views.View"),
        ("SqlServer", "Function", "Weasel.SqlServer.Functions.Function"),
        ("SqlServer", "StoredProcedure", "Weasel.SqlServer.Procedures.StoredProcedure"),
        ("SqlServer", "TableType", "Weasel.SqlServer.Tables.TableType"),
        ("SqlServer", "Trigger", "Weasel.SqlServer.Triggers.Trigger"),

        ("Oracle", "Table", "Weasel.Oracle.Tables.Table"),
        ("Oracle", "Sequence", "Weasel.Oracle.Sequence"),
        ("Oracle", "View", "Weasel.Oracle.Views.View"),
        ("Oracle", "Trigger", "Weasel.Oracle.Triggers.Trigger"),

        ("MySql", "Table", "Weasel.MySql.Tables.Table"),
        ("MySql", "Sequence", "Weasel.MySql.Sequence"),
        ("MySql", "View", "Weasel.MySql.Views.View"),
        ("MySql", "Trigger", "Weasel.MySql.Triggers.Trigger"),

        ("Sqlite", "Table", "Weasel.Sqlite.Tables.Table"),
        ("Sqlite", "View", "Weasel.Sqlite.Views.View"),
        ("Sqlite", "Trigger", "Weasel.Sqlite.Triggers.Trigger")
    ];

    public static TheoryData<string, string, string> Supported
    {
        get
        {
            var data = new TheoryData<string, string, string>();
            foreach (var row in SupportedRows) data.Add(row.Provider, row.ObjectType, row.TypeName);
            return data;
        }
    }

    /// <summary>
    ///     Every ✗ in the matrix — the engine has it, Weasel does not model it yet. Each carries the
    ///     issue that will change the answer, so the failure message says what to do.
    /// </summary>
    public static TheoryData<string, string, string> NotSupported =>
        new()
        {
            { "Postgresql", "Weasel.Postgresql.Procedures.StoredProcedure", "#451" },
            { "MySql", "Weasel.MySql.Procedures.StoredProcedure", "#451" },
            { "Oracle", "Weasel.Oracle.Procedures.StoredProcedure", "#451" },
            { "Oracle", "Weasel.Oracle.Functions.Function", "#450" },
            { "Oracle", "Weasel.Oracle.Views.MaterializedView", "#453" },
            { "MySql", "Weasel.MySql.Functions.Function", "#450" }
        };

    [Theory]
    [MemberData(nameof(Supported))]
    public void a_supported_object_type_really_is_a_schema_object(
        string provider, string objectType, string typeName)
    {
        var type = AssemblyFor(provider).GetType(typeName);

        type.ShouldNotBeNull(
            $"docs/core/object-types.md claims {provider} supports {objectType}, but {typeName} does not exist");

        typeof(ISchemaObject).IsAssignableFrom(type).ShouldBeTrue(
            $"{typeName} exists but is not an ISchemaObject, so {provider} cannot migrate a {objectType}");
    }

    [Theory]
    [MemberData(nameof(NotSupported))]
    public void an_unsupported_object_type_really_is_absent(string provider, string typeName, string issue)
    {
        AssemblyFor(provider).GetType(typeName).ShouldBeNull(
            $"{typeName} exists now, so docs/core/object-types.md and {issue} are both out of date");
    }

    /// <summary>
    ///     The catch-all: anything in a provider assembly that implements <see cref="ISchemaObject" />
    ///     and is not in <see cref="Supported" /> is an object type somebody added without touching
    ///     the matrix.
    /// </summary>
    /// <remarks>
    ///     <see cref="Infrastructure" /> holds the implementations that are not user-facing object
    ///     types — a schema-existence probe, the internal tables Weasel creates for its own
    ///     bookkeeping, the abstract bases. Those are exempt by name rather than by heuristic, so
    ///     adding one is a deliberate act.
    /// </remarks>
    private static readonly string[] Infrastructure =
    [
        "Weasel.Postgresql.SchemaExistenceCheck",
        "Weasel.Postgresql.Tables.TenantAssignmentTable",
        "Weasel.Postgresql.Tables.DatabasePoolTable",
        "Weasel.Postgresql.Functions.UpsertFunction"
    ];

    [Theory]
    [InlineData("Postgresql")]
    [InlineData("SqlServer")]
    [InlineData("Oracle")]
    [InlineData("MySql")]
    [InlineData("Sqlite")]
    public void no_object_type_is_missing_from_the_matrix(string provider)
    {
        var documented = SupportedRows.Select(x => x.TypeName).ToHashSet();

        var actual = AssemblyFor(provider).GetExportedTypes()
            .Where(x => typeof(ISchemaObject).IsAssignableFrom(x))
            .Where(x => x is { IsAbstract: false, IsInterface: false })
            .Select(x => x.FullName!)
            .Where(x => !documented.Contains(x))
            .Where(x => !Infrastructure.Contains(x))
            .ToArray();

        actual.ShouldBeEmpty(
            $"{provider} has schema object types that docs/core/object-types.md does not mention: "
            + string.Join(", ", actual));
    }
}
