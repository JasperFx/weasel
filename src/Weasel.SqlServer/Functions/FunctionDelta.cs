using Weasel.Core;

namespace Weasel.SqlServer.Functions;

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

        var expectedSql = expected.Body().Replace(" OR ALTER", "").CanonicizeSql();
        var actualSql = actual.Body().Replace(" OR ALTER", "").CanonicizeSql();
        if (!expectedSql.Equals(actualSql, StringComparison.OrdinalIgnoreCase))
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
                Expected.WriteDropStatement(rules, writer);
                Actual.WriteCreateStatement(rules, writer);
            }
            else
            {
                Expected.WriteDropStatement(rules, writer);
            }
        }
    }


    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        Actual!.WriteDropStatement(rules, writer);
        if (!Expected.IsRemoved)
        {
            Expected.WriteCreateStatement(rules, writer);
        }
    }

    public override string ToString()
    {
        return Expected.Identifier.QualifiedName + " Diff";
    }
}
