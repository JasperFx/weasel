using System.IO;
using System.Linq;

namespace Weasel.SqlServer.Tables
{
    public class TableTypeDelta : SchemaObjectDelta<TableType>
    {
        public TableTypeDelta(TableType expected, TableType? actual) : base(expected, actual)
        {
        }

        public override void WriteUpdate(DdlRules rules, TextWriter writer)
        {
            Expected.WriteDropStatement(rules, writer);
            Expected.WriteCreateStatement(rules, writer);
        }

        protected override SchemaPatchDifference compare(TableType expected, TableType? actual)
        {
            if (actual == null) return SchemaPatchDifference.Create;
            
            if (expected.Columns.SequenceEqual(actual.Columns))
            {
                return SchemaPatchDifference.None;
            }

            return SchemaPatchDifference.Update;
        }
    }
}