using JasperFx.Core.Reflection;
using Weasel.Core;
using Weasel.Postgresql;

namespace Weasel.Postgresql.Tables.Partitioning;

public static class PartitionExtensions
{
    /// <summary>
    /// Write the SQL for the default partition for the given table name. Uses "_default" as the suffix
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="identifier"></param>
    public static void WriteDefaultPartition(this TextWriter writer, DbObjectName identifier)
    {
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(identifier.Schema, identifier.Name + "_default"));
        var parentName = PostgresqlObjectName.From(identifier);
        writer.WriteLine($"CREATE TABLE {partitionName} PARTITION OF {parentName} DEFAULT;");
    }

    /// <summary>
    /// Write the literal value of SQL for a given value. Places single quotes around strings
    /// </summary>
    /// <param name="value"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static string FormatSqlValue<T>(this T value)
    {
        if (typeof(T).IsNumeric()) return value.ToString();

        if (value is bool b) return b.ToString().ToLowerInvariant();

        if (value is string v && v.StartsWith("'") && v.EndsWith("'")) return v;

        return $"'{value.ToString()}'";
    }

    /// <summary>
    /// Picks off the suffix name for a partition name based on a table name
    /// </summary>
    /// <param name="identifier"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public static string GetSuffixName(this DbObjectName identifier, string tableName)
    {
        var suffix = tableName;
        if (tableName.StartsWith(identifier.Name))
        {
            suffix = suffix.Substring(identifier.Name.Length);
        }

        return suffix.TrimStart('_');
    }

    internal static string GetStringWithinParantheses(this string raw)
    {
        var start = raw.IndexOf('(');
        var end = raw.IndexOf(')');
        return raw.Substring(start + 1, end - start - 1);
    }

    internal static ReadOnlySpan<char> GetSpanWithinParentheses(this string raw)
    {
        var start = raw.IndexOf('(');
        var end = raw.IndexOf(')');
        return raw.AsSpan(start + 1, end - start - 1);
    }

}
