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
