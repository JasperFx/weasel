using Weasel.Core;
using Weasel.Postgresql.Functions;

namespace Weasel.Postgresql.Tables;

internal static class StringWriterExtensions
{
    public static void WriteCascadeAction(this TextWriter writer, string prefix, CascadeAction action)
    {
        switch (action)
        {
            case CascadeAction.Cascade:
                writer.WriteLine($"{prefix} CASCADE");
                break;
            case CascadeAction.Restrict:
                writer.WriteLine($"{prefix} RESTRICT");
                break;
            case CascadeAction.NoAction:
                return;
            case CascadeAction.SetDefault:
                writer.WriteLine($"{prefix} SET DEFAULT");
                break;
            case CascadeAction.SetNull:
                writer.WriteLine($"{prefix} SET NULL");
                break;
        }
    }

    public static void WriteDropIndex(this TextWriter writer, Table table, IndexDefinition index)
    {
        var concurrently = index.IsConcurrent ? "concurrently " : string.Empty;
        writer.WriteLine($"drop index {concurrently}if exists {table.Identifier.Schema}.{index.QuotedName};");
    }

    public static void WriteDropFunction(this TextWriter writer, Function function)
    {
        foreach (var drop in function.DropStatements())
        {
            var sql = drop;
            if (!sql.EndsWith("cascade", StringComparison.OrdinalIgnoreCase))
            {
                sql = sql.TrimEnd(';') + " cascade;";
            }

            writer.WriteLine(sql);
        }
    }

    public static void WriteReplaceFunction(this TextWriter writer, Migrator rules, Function oldFunction,
        Function newFunction)
    {
        writer.WriteDropFunction(oldFunction);
        newFunction.WriteCreateStatement(rules, writer);
    }
}
