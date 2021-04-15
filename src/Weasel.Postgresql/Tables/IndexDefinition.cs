using System;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class IndexDefinition : IIndexDefinition
    {
        public IndexDefinition(string indexName)
        {
            IndexName = indexName;
        }

        public string IndexName { get; }
        
        public IndexMethod Method { get; set; } = IndexMethod.btree;

        public SortOrder SortOrder { get; set; } = SortOrder.Asc;

        public bool IsUnique { get; set; }

        public bool IsConcurrent { get; set; }

        public string Expression { get; set; }

        /// <summary>
        /// Set the Index expression against the supplied columns
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IndexDefinition AgainstColumns(params string[] columns)
        {
            Expression = $"({columns.Join(", ")})";
            return this;
        }
        
        /// <summary>
        /// The tablespace in which to create the index. If not specified, default_tablespace is consulted,
        /// </summary>
        public string TableSpace { get; set; }

        /// <summary>
        /// The constraint expression for a partial index.
        /// </summary>
        public string Predicate { get; set; }

        public string ToDDL(Table parent)
        {
            if (Expression.IsEmpty())
                throw new InvalidOperationException($"{nameof(Expression)} cannot be null or empty");
            
            var builder = new StringBuilder();

            builder.Append("CREATE ");

            if (IsUnique) builder.Append("UNIQUE ");
            builder.Append("INDEX ");
            
            if (IsConcurrent) builder.Append("CONCURRENTLY ");

            builder.Append(IndexName);

            

            builder.Append(" ON ");
            builder.Append(parent.Identifier);
            builder.Append(" USING ");
            builder.Append(Method);
            builder.Append(" ");
            builder.Append(correctedExpression());
            builder.Append(SortOrder == SortOrder.Asc ? " ASC" : " DESC");

            if (TableSpace.IsNotEmpty())
            {
                builder.Append(" TABLESPACE ");
                builder.Append(TableSpace);
            }

            if (Predicate.IsNotEmpty())
            {
                builder.Append(" WHERE ");
                builder.Append(Predicate);
            }


            return builder.ToString();
        }

        private string correctedExpression()
        {
            if (Expression.StartsWith('(') && Expression.EndsWith(')')) return Expression;

            return $"({Expression})";
        }

        public bool Matches(ActualIndex index)
        {
            throw new System.NotImplementedException();
        }
    }
}