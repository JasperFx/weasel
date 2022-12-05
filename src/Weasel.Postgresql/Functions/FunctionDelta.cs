using Weasel.Core;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql.Functions;

public class FunctionDelta: SchemaObjectDelta<Function>
{
    public FunctionDelta(Function expected, Function? actual): base(expected, actual)
    {
    }

    protected override SchemaPatchDifference compare(Function expected, Function? actual)
    {
        if (expected.IsRemoved)
        {
            return actual == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update;
        }

        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        if (!expected.Body().CanonicizeSql().Equals(actual.Body().CanonicizeSql(), StringComparison.OrdinalIgnoreCase))
        {
            return SchemaPatchDifference.Update;
        }

        return SchemaPatchDifference.None;
    }

    public override void WriteRollback(Migrator rules, TextWriter writer)
    {
        if (Expected.IsRemoved)
        {
            Actual!.WriteCreateStatement(rules, writer);
        }
        else
        {
            if (Actual != null)
            {
                writer.WriteReplaceFunction(rules, Expected, Actual);
            }
            else
            {
                writer.WriteDropFunction(Expected);
            }
        }
    }


    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Expected.IsRemoved)
        {
            writer.WriteDropFunction(Actual!);
        }
        else
        {
            writer.WriteReplaceFunction(rules, Actual!, Expected);
        }
    }

    public override string ToString()
    {
        return Expected.Identifier.QualifiedName + " Diff";
    }
}
