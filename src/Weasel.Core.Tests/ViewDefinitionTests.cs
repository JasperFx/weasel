using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

public class ViewDefinitionTests
{
    private const string SqlServerDelimiters = "[\"";

    /// <summary>
    ///     The three cases that fail against the previous implementation, which took everything
    ///     after the first <c>" AS "</c>. Each one puts an <c>AS</c> somewhere a view header may
    ///     legally carry it, ahead of the view's own.
    /// </summary>
    public class the_defect
    {
        [Fact]
        public void a_column_alias_is_not_mistaken_for_the_views_own_as()
        {
            ViewDefinition.ExtractBody(
                    "CREATE VIEW dbo.v_person\nAS\nSELECT id, first_name AS given_name FROM dbo.person",
                    SqlServerDelimiters)
                .ShouldBe("SELECT id, first_name AS given_name FROM dbo.person");
        }

        [Fact]
        public void an_as_inside_a_delimited_identifier_is_not_the_separator()
        {
            ViewDefinition.ExtractBody("CREATE VIEW [dbo].[my as view] AS SELECT 1 AS one", SqlServerDelimiters)
                .ShouldBe("SELECT 1 AS one");
        }

        [Fact]
        public void an_as_inside_a_leading_comment_is_not_the_separator()
        {
            ViewDefinition.ExtractBody("/* selects x AS y */\nCREATE VIEW dbo.v AS SELECT 1", SqlServerDelimiters)
                .ShouldBe("SELECT 1");
        }
    }

    /// <summary>
    ///     Behaviour that was already correct and is pinned so the rewrite cannot quietly lose it.
    ///     These pass against the previous implementation too.
    /// </summary>
    public class guards
    {
        [Fact]
        public void the_body_starts_after_the_views_own_as()
        {
            ViewDefinition.ExtractBody("CREATE VIEW dbo.v AS SELECT id FROM dbo.t", SqlServerDelimiters)
                .ShouldBe("SELECT id FROM dbo.t");
        }

        [Fact]
        public void an_as_inside_a_declared_column_list_is_not_the_separator()
        {
            ViewDefinition.ExtractBody("CREATE VIEW dbo.v (a, b) AS SELECT x AS a, y AS b FROM dbo.t",
                    SqlServerDelimiters)
                .ShouldBe("SELECT x AS a, y AS b FROM dbo.t");
        }

        [Fact]
        public void an_as_inside_a_string_literal_is_not_the_separator()
        {
            ViewDefinition.ExtractBody("CREATE VIEW dbo.v AS SELECT 'x AS y' AS label", SqlServerDelimiters)
                .ShouldBe("SELECT 'x AS y' AS label");
        }

        [Fact]
        public void a_view_header_carrying_options_still_splits_on_its_own_as()
        {
            ViewDefinition.ExtractBody("CREATE VIEW dbo.v WITH SCHEMABINDING AS SELECT id FROM dbo.t",
                    SqlServerDelimiters)
                .ShouldBe("SELECT id FROM dbo.t");
        }

        [Fact]
        public void a_word_merely_beginning_with_as_is_not_the_separator()
        {
            ViewDefinition.ExtractBody("CREATE VIEW dbo.assets AS SELECT 1", SqlServerDelimiters)
                .ShouldBe("SELECT 1");
        }

        [Fact]
        public void text_with_no_separator_at_all_comes_back_unchanged()
        {
            ViewDefinition.ExtractBody("SELECT 1", SqlServerDelimiters).ShouldBe("SELECT 1");
        }
    }

    /// <summary>
    ///     A delimiter is only skipped when the dialect actually uses it, which is why the caller
    ///     supplies them rather than Core assuming every dialect's. The backtick case fails against
    ///     the previous implementation too; the bracket case passes on both.
    /// </summary>
    public class per_dialect_delimiters
    {
        [Fact]
        public void sqlite_skips_a_backtick_quoted_name()
        {
            ViewDefinition.ExtractBody("CREATE VIEW `my as view` AS SELECT 1", "\"[`")
                .ShouldBe("SELECT 1");
        }

        [Fact]
        public void a_bracket_is_an_ordinary_character_where_it_does_not_delimit()
        {
            ViewDefinition.ExtractBody("CREATE VIEW \"v\" AS SELECT a[1] AS first", "\"")
                .ShouldBe("SELECT a[1] AS first");
        }
    }
}
