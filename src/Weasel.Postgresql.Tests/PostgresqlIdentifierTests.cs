using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

public class PostgresqlIdentifierTests
{
    [Fact]
    public void short_name_is_unchanged()
    {
        var name = "fk_mt_events_stream_id";
        PostgresqlIdentifier.Shorten(name).ShouldBe(name);
    }

    [Fact]
    public void exactly_at_limit_is_unchanged()
    {
        var name = new string('a', 63);
        PostgresqlIdentifier.Shorten(name).ShouldBe(name);
    }

    [Fact]
    public void one_over_limit_is_shortened()
    {
        var name = new string('a', 64);
        var result = PostgresqlIdentifier.Shorten(name);
        result.Length.ShouldBeLessThanOrEqualTo(63);
        result.ShouldNotBe(name);
    }

    [Fact]
    public void shortened_name_respects_max_length()
    {
        var name = "fkey_mt_event_tag_bootstrap_token_resource_name_seq_id_is_archived";
        name.Length.ShouldBeGreaterThan(63);

        var result = PostgresqlIdentifier.Shorten(name);
        result.Length.ShouldBeLessThanOrEqualTo(63);
    }

    [Fact]
    public void shortening_is_deterministic()
    {
        var name = "fkey_mt_event_tag_bootstrap_token_resource_name_seq_id_is_archived";
        var result1 = PostgresqlIdentifier.Shorten(name);
        var result2 = PostgresqlIdentifier.Shorten(name);
        result1.ShouldBe(result2);
    }

    [Fact]
    public void different_long_names_produce_different_results()
    {
        var name1 = "fkey_mt_event_tag_bootstrap_token_resource_name_seq_id_is_archived";
        var name2 = "fkey_mt_event_tag_another_very_long_type_name_here_seq_id_is_archived";

        var result1 = PostgresqlIdentifier.Shorten(name1);
        var result2 = PostgresqlIdentifier.Shorten(name2);

        result1.ShouldNotBe(result2);
    }

    [Fact]
    public void shortened_name_ends_with_hash_suffix()
    {
        var name = new string('x', 100);
        var result = PostgresqlIdentifier.Shorten(name);

        // Should end with _XXXX (underscore + 4 hex chars)
        result[^5].ShouldBe('_');
        result[^4..].ShouldMatch("[0-9a-f]{4}");
    }

    [Fact]
    public void custom_max_length_is_respected()
    {
        var name = "some_identifier_that_is_moderately_long";
        var result = PostgresqlIdentifier.Shorten(name, maxLength: 20);
        result.Length.ShouldBeLessThanOrEqualTo(20);
    }

    [Fact]
    public void realistic_event_tag_fk_names()
    {
        // These are the exact patterns from Marten's EventTagTable
        var cases = new[]
        {
            "fkey_mt_event_tag_bootstrap_token_resource_name_seq_id_is_archived",
            "fkey_mt_event_tag_very_long_discriminated_union_type_name_seq_id_is_archived",
            "pk_mt_event_tag_bootstrap_token_resource_name",
            "fk_mt_natural_key_verylongaggregatetypename_stream_tenant_is_archived",
        };

        foreach (var original in cases)
        {
            var shortened = PostgresqlIdentifier.Shorten(original);
            shortened.Length.ShouldBeLessThanOrEqualTo(63,
                $"'{original}' ({original.Length} chars) shortened to '{shortened}' ({shortened.Length} chars)");
        }
    }

    [Fact]
    public void empty_and_null_handled_gracefully()
    {
        PostgresqlIdentifier.Shorten("").ShouldBe("");
        PostgresqlIdentifier.Shorten("a").ShouldBe("a");
    }
}
