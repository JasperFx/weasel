using System;
using System.IO;

namespace Weasel.Postgresql.Functions
{
    public class FunctionDelta : SchemaObjectDelta<Function>
    {
        public FunctionDelta(Function expected, Function actual) : base(expected, actual)
        {

        }

        protected override SchemaPatchDifference compare(Function expected, Function actual)
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


        public override void WriteUpdate(DdlRules rules, TextWriter writer)
        {
            if (Expected.IsRemoved)
            {
                foreach (var drop in Actual.DropStatements())
                {
                    var sql = drop;
                    if (!sql.EndsWith("cascade", StringComparison.OrdinalIgnoreCase))
                    {
                        sql = sql.TrimEnd(';') + " cascade;";
                    }

                    writer.WriteLine(sql);
                }
            }
            else
            {
                foreach (var drop in Actual.DropStatements())
                {
                    var sql = drop;
                    if (!sql.EndsWith("cascade", StringComparison.OrdinalIgnoreCase))
                    {
                        sql = sql.TrimEnd(';') + " cascade;";
                    }

                    writer.WriteLine(sql);
                }  
                
                Expected.WriteCreateStatement(rules, writer);
            }
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
