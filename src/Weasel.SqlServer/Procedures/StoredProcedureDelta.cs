using Weasel.Core;

namespace Weasel.SqlServer.Procedures;

public class StoredProcedureDelta: SchemaObjectDelta<StoredProcedure>
{
    public StoredProcedureDelta(StoredProcedure expected, StoredProcedure? actual): base(expected, actual)
    {
    }

    protected override SchemaPatchDifference compare(StoredProcedure expected, StoredProcedure? actual)
    {
        if (expected.IsRemoved)
        {
            return actual == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update;
        }

        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        // Both sides drop to the bare CREATE PROCEDURE spelling first. The body may be authored as
        // CREATE PROC or CREATE OR ALTER PROCEDURE, and whichever it is, sys.sql_modules hands back
        // CREATE + 3 spaces + PROCEDURE, because SQL Server blanks OR ALTER in place. Comparing
        // those as written reads Update forever, and applying the update writes the same text again
        // (weasel#593).
        var expectedSql = expected.CanonicizeSql().ToBareCreateProcedure();
        var actualSql = actual.CanonicizeSql().ToBareCreateProcedure();
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
                Expected.WriteCreateOrAlterStatement(rules, writer);
            }
            else
            {
                Expected.WriteDropStatement(rules, writer);
            }
        }
    }


    public override void WriteUpdate(Migrator rules, TextWriter writer)
    {
        if (Expected.IsRemoved)
        {
            Expected.WriteDropStatement(rules, writer);
        }
        else
        {
            Expected.WriteCreateOrAlterStatement(rules, writer);
        }
    }

    public override string ToString()
    {
        return Expected.Identifier.QualifiedName + " Diff";
    }
}
