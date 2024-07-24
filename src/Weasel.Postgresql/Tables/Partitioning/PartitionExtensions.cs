using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public static class PartitionExtensions
{
    public static void WriteDefaultPartition(this TextWriter writer, DbObjectName identifier)
    {
        writer.WriteLine($"CREATE TABLE {identifier}_default PARTITION OF {identifier} DEFAULT;");
    }

    public static string FormatSqlValue<T>(this T value)
    {
        if (typeof(T).IsNumeric()) return value.ToString();

        if (value is string v && v.StartsWith("'") && v.EndsWith("'")) return v;

        return $"'{value.ToString()}'";
    }

    public static string GetSuffixName(this DbObjectName identifier, string tableName)
    {
        return tableName.TrimStart(identifier.Name.ToCharArray()).TrimStart('_');
    }

    public static string GetStringWithinParantheses(this string raw)
    {
        var start = raw.IndexOf('(');
        var end = raw.IndexOf(')');
        return raw.Substring(start + 1, end - start - 1);
    }

}
