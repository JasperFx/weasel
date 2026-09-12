using System;
using System.Linq;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.SqlGeneration;
using Xunit;

namespace Weasel.Postgresql.Tests.SqlGeneration;

/// <summary>
///     weasel#591: under <see cref="EnumStorage.AsString" /> these two fragments rendered every
///     value with <c>ToString()</c>, which is the enum member's <em>declared</em> name. A serializer
///     that renamed the member — <c>[JsonStringEnumMemberName]</c> on System.Text.Json,
///     <c>[EnumMember]</c> on Newtonsoft — stored something else, so the filter compared against a
///     name that is not in the data and matched nothing, reporting it as "no rows" rather than as an
///     error. The optional renderer lets the calling store supply the name the serializer actually
///     wrote; omitting it keeps the historical behaviour exactly.
/// </summary>
public class EnumIsOneOfWhereFragmentTests
{
    public enum Zorgvorm
    {
        Huisarts,
        Apotheek
    }

    // Stands in for a serializer that renamed one member.
    private static string renamed(object value)
        => value switch
        {
            Zorgvorm.Apotheek => "apotheek-houdend",
            _ => value.ToString()!
        };

    private static object parameterValueFor(EnumIsOneOfWhereFragment fragment, out string sql)
    {
        var builder = new CommandBuilder();
        fragment.Apply(builder);
        var command = builder.Compile();
        sql = command.CommandText;
        return command.Parameters[0].Value!;
    }

    private static object parameterValueFor(EnumIsNotOneOfWhereFragment fragment)
    {
        var builder = new CommandBuilder();
        fragment.Apply(builder);
        return builder.Compile().Parameters[0].Value!;
    }

    [Fact]
    public void as_string_without_a_renderer_keeps_the_declared_name()
    {
        var fragment = new EnumIsOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek, Zorgvorm.Huisarts }, EnumStorage.AsString, "d.data ->> 'Zorgvorm'");

        ((string[])parameterValueFor(fragment, out _))
            .ShouldBe(new[] { "Apotheek", "Huisarts" });
    }

    [Fact]
    public void as_string_asks_the_renderer_for_the_stored_name()
    {
        var fragment = new EnumIsOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek, Zorgvorm.Huisarts }, EnumStorage.AsString, "d.data ->> 'Zorgvorm'",
            renamed);

        ((string[])parameterValueFor(fragment, out _))
            .ShouldBe(new[] { "apotheek-houdend", "Huisarts" });
    }

    [Fact]
    public void as_integer_never_consults_the_renderer()
    {
        var fragment = new EnumIsOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek }, EnumStorage.AsInteger, "d.data ->> 'Zorgvorm'",
            _ => throw new InvalidOperationException("the renderer is for the string storage only"));

        ((int?[])parameterValueFor(fragment, out _))
            .ShouldBe(new int?[] { 1 });
    }

    [Fact]
    public void a_null_entry_is_still_the_is_null_branch_and_never_reaches_the_renderer()
    {
        var fragment = new EnumIsOneOfWhereFragment(
            new Zorgvorm?[] { Zorgvorm.Apotheek, null }, EnumStorage.AsString, "d.data ->> 'Zorgvorm'",
            value => value is null
                ? throw new InvalidOperationException("a null entry must not be rendered")
                : renamed(value));

        var values = (string[])parameterValueFor(fragment, out var sql);

        values.ShouldBe(new[] { "apotheek-houdend" });
        sql.ShouldContain("is null");
    }

    [Fact]
    public void is_not_one_of_without_a_renderer_keeps_the_declared_name()
    {
        var fragment = new EnumIsNotOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek }, EnumStorage.AsString, "d.data ->> 'Zorgvorm'");

        ((string[])parameterValueFor(fragment)).ShouldBe(new[] { "Apotheek" });
    }

    [Fact]
    public void is_not_one_of_asks_the_renderer_for_the_stored_name()
    {
        var fragment = new EnumIsNotOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek }, EnumStorage.AsString, "d.data ->> 'Zorgvorm'", renamed);

        ((string[])parameterValueFor(fragment)).ShouldBe(new[] { "apotheek-houdend" });
    }

    [Fact]
    public void is_not_one_of_as_integer_never_consults_the_renderer()
    {
        var fragment = new EnumIsNotOneOfWhereFragment(
            new[] { Zorgvorm.Apotheek }, EnumStorage.AsInteger, "d.data ->> 'Zorgvorm'",
            _ => throw new InvalidOperationException("the renderer is for the string storage only"));

        ((int[])parameterValueFor(fragment)).ShouldBe(new[] { 1 });
    }
}
