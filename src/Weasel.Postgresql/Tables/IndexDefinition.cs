using System;
using System.Linq;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public class IndexDefinition : IIndexDefinition
    {
        private const string JsonbPathOps = "jsonb_path_ops";
        private static readonly string[] _reserved_words = new string[] {"trim", "lower", "upper"};
        
        private string _indexName;
        private string _expression;

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

        public string Expression
        {
            get => _expression;
            set
            {
                if (value.IsNotEmpty())
                {
                    if (!value.StartsWith("("))
                    {
                        value = "(" + value + ")";
                    }

                    _expression = value;
                }
                else
                {
                    _expression = value;
                }
                
            }
        }

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
            var ordering = "";
            if (Method == IndexMethod.btree && SortOrder != SortOrder.Asc)
            {
                ordering = " DESC";
            }
            
            if (Columns != null && Columns.Any())
            {
                var columns = Columns
                    .Select(x => _reserved_words.Contains(x) ? $"\"{x}\"" : x)
                    .Select(x => $"{x}{ordering}").Join(", ");
                if (Expression.IsEmpty())
                {
                    return $"({columns})";
                }

                if (Expression.Contains(JsonbPathOps))
                {
                    return Expression.Replace("?", columns);
                }
                
                return Expression.Replace("?", $"({columns})");
            }
            
            if (Expression.IsEmpty())
                throw new InvalidOperationException($"Either {nameof(Expression)} or {nameof(Columns)} must be specified");

            return $"({Expression} {ordering})";
        }

        /// <summary>
        /// Makes this index use the Gin method with the jsonb_path_ops operator
        /// </summary>
        public void ToGinWithJsonbPathOps()
        {
            Method = IndexMethod.gin;
            _expression = $"(? {JsonbPathOps})";
        }
    }
}