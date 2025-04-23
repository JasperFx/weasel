using System.Text.RegularExpressions;

namespace Weasel.SqlServer;

internal static class Canonicalization
{
    public static string ReplaceMultiSpace(this string str, string newStr)
    {
        var regex = new Regex("\\s+");
        return regex.Replace(str, newStr);
    }


    public static string CanonicizeSql(this string sql)
    {
        var replaced = sql
            .Trim()
            .Replace('\n', ' ')
            .Replace('\r', ' ')
            .Replace('\t', ' ')
            .ReplaceMultiSpace(" ")
            .Replace(" ;", ";")
            .Replace("SECURITY INVOKER", "")
            .Replace("  ", " ")
            .Replace("LANGUAGE plpgsql AS $function$", "")
            .Replace("$$ LANGUAGE plpgsql", "$function$")
            .Replace("AS $$ DECLARE", "DECLARE")
            .Replace("character varying", "varchar")
            .Replace("Boolean", "boolean")
            .Replace("bool,", "boolean,")
            .Replace("int[]", "integer[]")
            .Replace("numeric", "decimal")
            .TrimEnd(';').TrimEnd();

        return replaced;
    }
}
