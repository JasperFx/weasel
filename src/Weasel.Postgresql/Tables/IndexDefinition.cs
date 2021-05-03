using System;
using System.Linq;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class IndexDefinition : IIndexDefinition
    {
        private string _indexName;
        
        public IndexDefinition(string indexName)
        {
            _indexName = indexName;
        }

        protected IndexDefinition()
        {
        }


        public string Name
        {
            get
            {
                if (_indexName.IsNotEmpty())
                {
                    return _indexName;
                }

                return deriveIndexName();
            }
            set
            {
                _indexName = value;
            }
        }

        protected virtual string deriveIndexName()
        {
            throw new NotSupportedException();
        }

        public IndexMethod Method { get; set; } = IndexMethod.btree;

        public SortOrder SortOrder { get; set; } = SortOrder.Asc;

        public bool IsUnique { get; set; }

        public bool IsConcurrent { get; set; }

        public virtual string[] Columns { get; set; }
        
        public string Expression { get; set; }

        /// <summary>
        /// Set the Index expression against the supplied columns
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IndexDefinition AgainstColumns(params string[] columns)
        {
            Columns = columns;
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

            var builder = new StringBuilder();

            builder.Append("CREATE ");

            if (IsUnique) builder.Append("UNIQUE ");
            builder.Append("INDEX ");
            
            if (IsConcurrent) builder.Append("CONCURRENTLY ");

            builder.Append(Name);

            

            builder.Append(" ON ");
            builder.Append(parent.Identifier);
            builder.Append(" USING ");
            builder.Append(Method);
            builder.Append(" ");
            builder.Append(correctedExpression());

            if (TableSpace.IsNotEmpty())
            {
                builder.Append(" TABLESPACE ");
                builder.Append(TableSpace);
            }

            if (Predicate.IsNotEmpty())
            {
                builder.Append(" WHERE ");
                
                
                
                builder.Append($"({Predicate})");
            }

            builder.Append(";");


            return builder.ToString();
        }

        private string correctedExpression()
        {
            var suffix = "";
            if (SortOrder != SortOrder.Asc)
            {
                suffix = " DESC";
            }
            
            if (Columns != null && Columns.Any())
            {
                return $"({Columns.Select(x => $"{x}{suffix}").Join(", ")})";
            }
            
            if (Expression.IsEmpty())
                throw new InvalidOperationException($"Either {nameof(Expression)} or {nameof(Columns)} must be specified");

            return $"({Expression} {suffix})";
        }

    }
}