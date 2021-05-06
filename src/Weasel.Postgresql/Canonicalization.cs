using System;
using System.Text.RegularExpressions;
using Baseline;

namespace Weasel.Postgresql
{
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
                .Replace("numeric", "decimal").TrimEnd(';').TrimEnd();

            
            if (replaced.ContainsIgnoreCase("PLV8"))
            {
                replaced = replaced
                    .Replace("LANGUAGE plv8 IMMUTABLE STRICT AS $function$", "AS $$");

                const string languagePlv8ImmutableStrict = "$$ LANGUAGE plv8 IMMUTABLE STRICT";
                const string functionMarker = "$function$";
                if (replaced.EndsWith(functionMarker))
                {
                    replaced = replaced.Substring(0, replaced.LastIndexOf(functionMarker)) + languagePlv8ImmutableStrict;
                }
            }

            return replaced
                .Replace("  ", " ").TrimEnd().TrimEnd(';');
        }

    }
}
