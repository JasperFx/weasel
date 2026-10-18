using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     An override has to carry the same <see cref="DynamicallyAccessedMembersAttribute" /> as the
///     method it overrides, on its return value and on every parameter.
/// </summary>
/// <remarks>
///     <para>
///     <see cref="MultiCommandDataReader" /> overrode <see cref="DbDataReader.GetFieldType" /> without
///     repeating the base method's <c>[return: DynamicallyAccessedMembers(PublicFields | PublicProperties)]</c>.
///     A trimmed or natively published application that keeps the type then reports
///     <c>IL2093</c>, or <c>IL2104: Assembly 'Weasel.Core' produced trim warnings</c> under the
///     default single-warning mode. Every application that executes a compiled batch through
///     <c>CommandBuilderExtensions.ExecuteReaderAsync</c> keeps it, whichever database it talks
///     to, because the choice of a multi-command reader is only made at run time.
///     </para>
///     <para>
///     The trim analyzer does see this, but only as a warning in Weasel's own build, so it went
///     unnoticed. This sweep makes it a failing test.
///     </para>
/// </remarks>
public class overrides_keep_their_trim_annotations
{
    private static readonly Assembly[] WeaselAssemblies =
    [
        typeof(MultiCommandDataReader).Assembly,
        typeof(Storage.ChangeTracker<>).Assembly,
        typeof(Postgresql.Tables.Table).Assembly,
        typeof(SqlServer.Tables.Table).Assembly,
        typeof(Sqlite.Tables.Table).Assembly,
        typeof(MySql.Tables.Table).Assembly,
        typeof(Oracle.Tables.Table).Assembly,
        typeof(Firebird.Tables.Table).Assembly
    ];

    [Fact]
    public void every_override_keeps_the_annotations_of_the_method_it_overrides()
    {
        var mismatches = Overrides()
            .SelectMany(o => Mismatches(o.Override, o.Base))
            .OrderBy(x => x)
            .ToArray();

        mismatches.ShouldBeEmpty(
            "each of these drops or changes a [DynamicallyAccessedMembers] the overridden method declares, which a trimmed or native publish reports as IL2092/IL2093; copy the base method's attribute onto the override");
    }

    /// <summary>
    ///     The guard on the guard: the sweep has to reach internal types, or it would have passed with
    ///     the very defect it was written for still in place.
    /// </summary>
    [Fact]
    public void the_sweep_reaches_the_internal_multi_command_reader()
    {
        var getFieldType = typeof(MultiCommandDataReader).GetMethod(nameof(DbDataReader.GetFieldType), [typeof(int)])!;

        Overrides().Select(o => o.Override).ShouldContain(getFieldType);
        MemberTypesOf(getFieldType.GetBaseDefinition().ReturnParameter).ShouldBe(
            DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties,
            "DbDataReader.GetFieldType declares this, so it is the annotation the override must repeat");
    }

    private static IEnumerable<(MethodInfo Override, MethodInfo Base)> Overrides() =>
        WeaselAssemblies
            .SelectMany(a => a.GetTypes())
            .SelectMany(t => t.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            .Select(m => (Override: m, Base: m.GetBaseDefinition()))
            .Where(x => x.Base.DeclaringType != x.Override.DeclaringType);

    private static IEnumerable<string> Mismatches(MethodInfo overriding, MethodInfo overridden)
    {
        var slots = new List<(string Slot, ParameterInfo Override, ParameterInfo Base)>
        {
            ("return value", overriding.ReturnParameter, overridden.ReturnParameter)
        };

        var overridingParameters = overriding.GetParameters();
        var overriddenParameters = overridden.GetParameters();
        for (var i = 0; i < overridingParameters.Length; i++)
        {
            slots.Add(($"parameter '{overridingParameters[i].Name}'", overridingParameters[i], overriddenParameters[i]));
        }

        foreach (var (slot, overrideParameter, baseParameter) in slots)
        {
            var expected = MemberTypesOf(baseParameter);
            var actual = MemberTypesOf(overrideParameter);
            if (actual != expected)
            {
                yield return $"{Describe(overriding)}, {slot}: {actual}, but {overridden.DeclaringType!.FullName}.{overridden.Name} declares {expected}";
            }
        }
    }

    private static DynamicallyAccessedMemberTypes MemberTypesOf(ParameterInfo parameter) =>
        parameter.GetCustomAttribute<DynamicallyAccessedMembersAttribute>(inherit: false)?.MemberTypes
        ?? DynamicallyAccessedMemberTypes.None;

    private static string Describe(MethodInfo method) =>
        $"{method.DeclaringType!.FullName}.{method.Name}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))})";
}
