using System;
using System.IO;
using Baseline;

namespace Weasel.Postgresql.Functions
{
    public class FunctionDelta : ISchemaObjectDelta
    {
        public Function Expected { get; set; }
        public Function Actual { get; set; }

        public FunctionDelta(Function expected, Function actual)
        {
            Expected = expected;
            Actual = actual;
            
            SchemaObject = expected;

            
            if (Actual == null)
            {
                Difference = SchemaPatchDifference.Create;
            }
            else if (!Expected.Body().CanonicizeSql().Equals(Actual.Body().CanonicizeSql(), StringComparison.OrdinalIgnoreCase))
            {
                Difference = SchemaPatchDifference.Update;
            }
            else
            {
                Difference = SchemaPatchDifference.None;
            }
        }

        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference { get; }
        public void WriteUpdate(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }

        public void WriteRollback(DdlRules rules, StringWriter writer)
        {
            throw new NotImplementedException();
        }


        public void WritePatch(SchemaPatch patch)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            return Expected.Identifier.QualifiedName + " Diff";
        }
    }
}
