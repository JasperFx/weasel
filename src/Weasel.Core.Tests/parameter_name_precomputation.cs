using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     The positional parameter names the builders stamp onto parameters come from a precomputed
///     table instead of a per-parameter string concatenation (weasel#556). The names must be
///     byte-for-byte what <c>"p" + position</c> produced before, at every position — inside the
///     table, at its edges, and past its end — so that no generated SQL text changes.
/// </summary>
public class parameter_name_precomputation
{
    [Fact]
    public void names_are_identical_to_the_concatenation_they_replace()
    {
        // Well past any plausible table size, so the fallback path is exercised too
        for (var i = 0; i < 5000; i++)
        {
            ParameterNames.ForPosition(i).ShouldBe("p" + i);
        }
    }

    [Fact]
    public void positions_within_the_table_do_not_allocate()
    {
        ParameterNames.ForPosition(0).ShouldBeSameAs(ParameterNames.ForPosition(0));
        ParameterNames.ForPosition(511).ShouldBeSameAs(ParameterNames.ForPosition(511));
    }

    [Fact]
    public void the_first_position_past_the_table_still_answers_correctly()
    {
        ParameterNames.ForPosition(512).ShouldBe("p512");
    }
}
