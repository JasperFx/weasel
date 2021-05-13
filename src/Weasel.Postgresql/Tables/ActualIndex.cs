namespace Weasel.Postgresql.Tables
{
    public class ActualIndex : IIndexDefinition
    {
        public static bool Matches(IIndexDefinition expected, IIndexDefinition actual, Table parent)
        {
            var expectedSql = CanonicizeDdl(expected, parent);

            var actualSql = CanonicizeDdl(actual, parent);

            return expectedSql == actualSql;
        }

        public static string CanonicizeDdl(IIndexDefinition index, Table parent)
        {
            return index.ToDDL(parent)
                    .Replace("INDEX CONCURRENTLY", "INDEX")
                    .Replace("::text", "")
                ;
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
