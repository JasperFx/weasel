using System.Text.RegularExpressions;

namespace Weasel.SqlServer;

internal static class Canonicalization
{
    /// <summary>
    ///     The <c>CREATE [OR ALTER] FUNCTION</c> preamble, which is the one part of a body the
    ///     catalog does not return as written: SQL Server blanks <c>OR ALTER</c> in place rather than
    ///     removing it, so <c>CREATE OR ALTER FUNCTION</c> is stored as <c>CREATE</c>, three spaces,
    ///     <c>FUNCTION</c>.
    /// </summary>
    private static readonly Regex CreatePreamble =
        new(@"\b(CREATE)\s+(?:OR\s+ALTER\s+)?(FUNCTION)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    ///     The procedure counterpart of <see cref="CreatePreamble" />, in every spelling SQL Server
    ///     accepts: <c>PROC</c> or <c>PROCEDURE</c>, with or without <c>OR ALTER</c>.
    /// </summary>
    /// <remarks>
    ///     Anchored at the start of the body, behind a group that swallows leading whitespace,
    ///     <c>--</c> line comments and <c>/* */</c> block comments, so only the statement's own
    ///     keyword is matched and the same words further in, inside a string literal or a nested
    ///     <c>EXEC</c>, are left as written. The function regex cannot do this because
    ///     <c>Function.Body()</c> wraps its body in <c>EXEC sp_executesql</c>, which puts the
    ///     preamble mid-string; a procedure body starts with its own statement.
    /// </remarks>
    private static readonly Regex ProcedurePreamble =
        new(@"\A(?<lead>(?>(?:\s+|--[^\r\n]*|/\*.*?\*/)*))CREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\b",
            RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    /// <summary>
    ///     Rewrite a procedure's leading <c>CREATE [OR ALTER] PROC[EDURE]</c> keyword to the
    ///     <c>CREATE OR ALTER PROCEDURE</c> form, which is the one spelling a rendered migration
    ///     script can run a second time without failing (weasel#593). A body already in that form
    ///     comes back unchanged.
    /// </summary>
    public static string ToCreateOrAlterProcedure(this string sql)
        => ProcedurePreamble.Replace(sql, "${lead}CREATE OR ALTER PROCEDURE", 1);

    /// <summary>
    ///     Reduce a procedure's leading <c>CREATE [OR ALTER] PROC[EDURE]</c> keyword to the bare
    ///     <c>CREATE PROCEDURE</c> form, so a body authored any of those ways meets the catalog's
    ///     own rendering: SQL Server blanks <c>OR ALTER</c> in place and stores
    ///     <c>CREATE   PROCEDURE</c>. Without this the delta reads <c>Update</c> forever, because
    ///     applying it writes back the very text that does not match.
    /// </summary>
    public static string ToBareCreateProcedure(this string sql)
        => ProcedurePreamble.Replace(sql, "${lead}CREATE PROCEDURE", 1);

    /// <summary>
    ///     Normalize a T-SQL function body for comparison against what <c>sys.sql_modules</c> stores.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Line endings are normalized because the same source file is CRLF on one machine and
    ///         LF on another, and both apply against the same database. Left alone they never
    ///         converge: each side recreates the function and the other sees drift again.
    ///     </para>
    ///     <para>
    ///         Otherwise only the preamble, which is the only thing the catalog rewrites. It cannot
    ///         be matched by position — <c>Function.Body()</c> wraps the body in
    ///         <c>EXEC sp_executesql</c> — so a body that spells those same keywords inside a string
    ///         literal is rewritten there too.
    ///     </para>
    /// </remarks>
    public static string CanonicizeSql(this string sql)
        => CreatePreamble.Replace(sql.Trim().Replace("\r\n", "\n"), "$1 $2").TrimEnd(';').TrimEnd();
}
