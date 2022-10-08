using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Baseline;
using Weasel.Core;

namespace Weasel.Postgresql.Tables
{


    public class MisconfiguredForeignKeyException : Exception
    {
        public MisconfiguredForeignKeyException(string? message) : base(message)
        {
        }
    }

    public class ForeignKey : INamed
    {
        protected bool Equals(ForeignKey other)
        {
            return Name == other.Name && ColumnNames.SequenceEqual(other.ColumnNames) && LinkedNames.SequenceEqual(other.LinkedNames) && Equals(LinkedTable, other.LinkedTable) && OnDelete == other.OnDelete && OnUpdate == other.OnUpdate;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (!obj.GetType().CanBeCastTo<ForeignKey>())
            {
                return false;
            }

            return Equals((ForeignKey) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ColumnNames != null ? ColumnNames.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LinkedNames != null ? LinkedNames.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LinkedTable != null ? LinkedTable.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int) OnDelete;
                hashCode = (hashCode * 397) ^ (int) OnUpdate;
                return hashCode;
            }
        }


        public ForeignKey(string name)
        {
            Name = name;
        }

        /// <summary>
        /// Read the DDL definition from the server
        /// </summary>
        /// <param name="definition"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void Parse(string definition, string schema = "public")
        {
            var open1 = definition.IndexOf('(');
            var closed1 = definition.IndexOf(')');

            ColumnNames = definition.Substring(open1 + 1, closed1 - open1 - 1).ToDelimitedArray(',');

            var open2 = definition.IndexOf('(', closed1);
            var closed2 = definition.IndexOf(')', open2);
            
            LinkedNames = definition.Substring(open2 + 1, closed2 - open2 - 1).ToDelimitedArray(',');


            var references = "REFERENCES";
            var tableStart = definition.IndexOf(references) + references.Length;

            var tableName = definition.Substring(tableStart, open2 - tableStart).Trim();
			if (!tableName.Contains('.'))
            {
                tableName = $"{schema}.{tableName}";
            }
            LinkedTable = DbObjectName.Parse(PostgresqlProvider.Instance, tableName);

            if (definition.ContainsIgnoreCase("ON DELETE CASCADE"))
            {
                OnDelete = CascadeAction.Cascade;
            }
            else if (definition.ContainsIgnoreCase("ON DELETE RESTRICT"))
            {
                OnDelete = CascadeAction.Restrict;
            }
            else if (definition.ContainsIgnoreCase("ON DELETE SET NULL"))
            {
                OnDelete = CascadeAction.SetNull;
            }
            else if (definition.ContainsIgnoreCase("ON DELETE SET DEFAULT"))
            {
                OnDelete = CascadeAction.SetDefault;
            }
            
            if (definition.ContainsIgnoreCase("ON UPDATE CASCADE"))
            {
                OnUpdate = CascadeAction.Cascade;
            }
            else if (definition.ContainsIgnoreCase("ON UPDATE RESTRICT"))
            {
                OnUpdate = CascadeAction.Restrict;
            }
            else if (definition.ContainsIgnoreCase("ON UPDATE SET NULL"))
            {
                OnUpdate = CascadeAction.SetNull;
            }
            else if (definition.ContainsIgnoreCase("ON UPDATE SET DEFAULT"))
            {
                OnUpdate = CascadeAction.SetDefault;
            }
            
            
        }

        public string Name { get; set; }
        public string[] ColumnNames { get; set; } = null!;
        public string[] LinkedNames { get; set; } = null!;

        public DbObjectName LinkedTable { get; set; } = null!;

        public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;
        public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

        public string ToDDL(Table parent)
        {
            var writer = new StringWriter();
            WriteAddStatement(parent, writer);

            return writer.ToString();
        }

        public void WriteAddStatement(Table parent, TextWriter writer)
        {
            writer.WriteLine($"ALTER TABLE {parent.Identifier}");
            writer.WriteLine($"ADD CONSTRAINT {Name} FOREIGN KEY({ColumnNames.Join(", ")})");
            writer.Write($"REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
            writer.WriteCascadeAction("ON DELETE", OnDelete);
            writer.WriteCascadeAction("ON UPDATE", OnUpdate);
            writer.Write(";");
            writer.WriteLine();
        }

        public void WriteDropStatement(Table parent, TextWriter writer)
        {
            writer.WriteLine($"ALTER TABLE {parent.Identifier} DROP CONSTRAINT IF EXISTS {Name};");
        }
    }
}
