namespace Weasel.Postgresql.Tables
{
    public class ActualIndex : IIndexDefinition
    {
        public static bool Matches(IIndexDefinition expected, IIndexDefinition actual, Table parent)
        {
            var expectedSql = expected.ToDDL(parent)
                .Replace("INDEX CONCURRENTLY", "INDEX");

            var actualSql = actual.ToDDL(parent);

            return expectedSql == actualSql;
        }
        
        public DbObjectName Table { get; }

        public string Name { get; }
        public string DDL { get; }

        public ActualIndex(DbObjectName table, string name, string ddl)
        {
            Table = table;
            Name = name;
            DDL = ddl.Replace("  ", " ") + ";";
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
            return Equals((ActualIndex)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (DDL != null ? DDL.GetHashCode() : 0);
            }
        }

        public string ToDDL(Table parent)
        {
            return DDL;
        }

    }
}
