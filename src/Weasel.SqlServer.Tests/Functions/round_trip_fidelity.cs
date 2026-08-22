using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Functions;
using Xunit;

namespace Weasel.SqlServer.Tests.Functions;

/// <summary>
///     Whatever Weasel writes, Weasel must read back as unchanged. Any shape SQL Server reformats on
///     the way into <c>sys.sql_modules</c> shows up here as a phantom delta.
/// </summary>
[Collection("functions")]
public class round_trip_fidelity: IntegrationContext
{
    public round_trip_fidelity(): base("functions")
    {
    }

    public static TheoryData<string, string> Bodies() => new()
    {
        { "crlf line endings", "CREATE OR ALTER FUNCTION functions.rt()\r\nRETURNS int AS\r\nBEGIN\r\n    return 1;\r\nEND;" },
        { "lf line endings", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;" },
        { "crlf applied, lf compared", "CREATE OR ALTER FUNCTION functions.rt()\r\nRETURNS int AS\r\nBEGIN\r\n    return 1;\r\nEND;" },
        { "tab indentation", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n\treturn 1;\nEND;" },
        { "no OR ALTER", "CREATE FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;" },
        { "OR ALTER split over lines", "CREATE OR ALTER\nFUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;" },
        { "lower case keywords", "create or alter function functions.rt()\nreturns int as\nbegin\n    return 1;\nend;" },
        { "leading blank lines", "\n\n\nCREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;" },
        { "blank lines inside", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n\n\n    return 1;\n\nEND;" },
        { "trailing spaces on lines", "CREATE OR ALTER FUNCTION functions.rt()   \nRETURNS int AS   \nBEGIN   \n    return 1;   \nEND;" },
        { "line comment with an apostrophe", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    -- it's fine\n    return 1;\nEND;" },
        { "block comment", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    /* a  spaced   comment */\n    return 1;\nEND;" },
        { "string literal with runs of spaces", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS varchar(50) AS\nBEGIN\n    return 'foo       bar';\nEND;" },
        { "doubled quotes in a literal", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS varchar(50) AS\nBEGIN\n    return 'it''s   here';\nEND;" },
        { "bracketed identifier with a space", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    declare @t table ([unit  price] int);\n    return 1;\nEND;" },
        { "unicode literal", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS nvarchar(50) AS\nBEGIN\n    return N'naïve — ☕';\nEND;" },
        { "semicolon spacing", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1 ;\nEND;" },
        { "mixed case OR ALTER", "Create Or Alter Function functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;" },
        { "the words OR ALTER inside a literal", "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS varchar(50) AS\nBEGIN\n    return 'use CREATE OR ALTER FUNCTION here';\nEND;" },
        { "extra spaces between keywords", "CREATE OR ALTER FUNCTION    functions.rt()\nRETURNS     int AS\nBEGIN\n    return    1;\nEND;" }
    };

    [Theory]
    [MemberData(nameof(Bodies))]
    public async Task weasel_reads_back_what_it_wrote(string description, string body)
    {
        await ResetSchema();

        var function = new Function(new SqlServerObjectName("functions", "rt"), body);
        await function.ApplyChangesAsync(theConnection);

        var delta = await function.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None, description);
    }

    /// <summary>
    ///     The same source file is CRLF on one machine and LF on another. If that were a difference,
    ///     two environments pointed at one database would recreate the function past each other
    ///     forever instead of converging.
    /// </summary>
    [Fact]
    public async Task line_endings_do_not_decide_whether_a_body_changed()
    {
        await ResetSchema();

        const string lf = "CREATE OR ALTER FUNCTION functions.rt()\nRETURNS int AS\nBEGIN\n    return 1;\nEND;";
        await new Function(new SqlServerObjectName("functions", "rt"), lf).ApplyChangesAsync(theConnection);

        var crlf = new Function(new SqlServerObjectName("functions", "rt"), lf.Replace("\n", "\r\n"));

        (await crlf.FindDeltaAsync(theConnection)).Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
