using System;
using System.IO;

namespace Weasel.SqlServer.Procedures
{
    public class StoredProcedureDelta : SchemaObjectDelta<StoredProcedure>
    {
        public StoredProcedureDelta(StoredProcedure expected, StoredProcedure actual) : base(expected, actual)
        {

        }

        protected override SchemaPatchDifference compare(StoredProcedure expected, StoredProcedure actual)
        {
            if (expected.IsRemoved)
            {
                return actual == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update;
            }

            if (actual == null)
            {
                return SchemaPatchDifference.Create;
            }

            if (!expected.CanonicizeSql().Equals(actual.CanonicizeSql(), StringComparison.OrdinalIgnoreCase))
            {
                return SchemaPatchDifference.Update;
            }

            return SchemaPatchDifference.None;
        }

        public override void WriteRollback(DdlRules rules, TextWriter writer)
        {
            if (Expected.IsRemoved)
            {
                Actual.WriteCreateStatement(rules, writer);
            }
            else
            {
                if (Actual != null)
                {
                    Expected.WriteDropStatement(rules, writer);
                    Expected.WriteCreateStatement(rules, writer);
                }
                else
                {
                    Expected.WriteDropStatement(rules, writer);
                }
            }
        }



        public override void WriteUpdate(DdlRules rules, TextWriter writer)
        {
            if (Expected.IsRemoved)
            {
                Expected.WriteDropStatement(rules, writer);
            }
            else
            {
                Expected.WriteDropStatement(rules, writer);
                Expected.WriteCreateStatement(rules, writer);
            }
        }

        public override string ToString()
        {
            return Expected.Identifier.QualifiedName + " Diff";
        }
    }
}