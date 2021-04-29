using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public enum CascadeAction
    {
        SetNull,
        SetDefault,
        Restrict,
        NoAction,
        Cascade
    }

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
            return ColumnNames.SequenceEqual(other.ColumnNames) 
                   && LinkedNames.SequenceEqual(other.LinkedNames) 
                   && Equals(LinkedTable, other.LinkedTable) 
                   && OnDelete == other.OnDelete 
                   && OnUpdate == other.OnUpdate;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((ForeignKey) obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(ColumnNames, LinkedNames, LinkedTable, (int) OnDelete, (int) OnUpdate);
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
        public void Parse(string definition)
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
            LinkedTable = DbObjectName.Parse(tableName);

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
        public string[] ColumnNames { get; set; }
        public string[] LinkedNames { get; set; } 
        
        public DbObjectName LinkedTable { get; set; }

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
