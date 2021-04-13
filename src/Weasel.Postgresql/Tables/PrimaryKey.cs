using System.Collections.Generic;
using System.Linq;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class PrimaryKey : ColumnCheck
    {
        public PrimaryKey(string name, IEnumerable<TableColumn> columns)
        {
            Name = name;
            Columns = columns.ToArray();
        }

        public TableColumn[] Columns { get; set; }

        public override string Declaration()
        {
            if (Columns.Length == 1)
            {
                return "PRIMARY KEY";
            }
            else
            {
                return $"PRIMARY KEY ({Columns.Select(x => x.Name).Join(", ")})";
            }
        }
    }
}