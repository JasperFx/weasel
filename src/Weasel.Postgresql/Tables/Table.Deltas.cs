using System;
using System.Collections.Generic;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
        internal List<ActualForeignKey> ActualForeignKeys { get; set; } = new List<ActualForeignKey>();

        internal Dictionary<string, ActualIndex> ActualIndices { get; set; } = new Dictionary<string, ActualIndex>();

    }
    
    public class ActualForeignKey
    {
        public DbObjectName Table { get; }
        public string Name { get; }
        public string DDL { get; }

        public bool DoesCascadeDeletes()
        {
            // NOTE: Use .IndexOf() so it's not effected by whitespace
            return DDL.IndexOf("on delete cascade", StringComparison.OrdinalIgnoreCase) != -1;
        }

        public ActualForeignKey(DbObjectName table, string name, string ddl)
        {
            Table = table;
            Name = name;
            DDL = ddl;
        }

        public override string ToString()
        {
            return $"Table: {Table}, Name: {Name}, DDL: {DDL}";
        }

        protected bool Equals(ActualIndex other)
        {
            return string.Equals(Name, other.Name) && string.Equals(DDL, other.DDL);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((ActualForeignKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (DDL != null ? DDL.GetHashCode() : 0);
            }
        }
    }
}