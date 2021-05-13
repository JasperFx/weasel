using System;
using Baseline;
using NpgsqlTypes;

namespace Weasel.Postgresql.SqlGeneration
{
    public class EnumIsOneOfWhereFragment: ISqlFragment
    {
        private readonly object _values;
        private readonly string _locator;
        private readonly NpgsqlDbType _dbType;

        public EnumIsOneOfWhereFragment(object values, EnumStorage enumStorage, string locator)
        {
            var array = values.As<Array>();
            if (enumStorage == EnumStorage.AsInteger)
            {
                var numbers = new int[array.Length];

                for (int i = 0; i < array.Length; i++)
                {
                    numbers[i] = array.GetValue(i).As<int>();
                }

                _values = numbers;
                _dbType = NpgsqlDbType.Integer | NpgsqlDbType.Array;
            }
            else
            {
                var strings = new string[array.Length];

                for (int i = 0; i < array.Length; i++)
                {
                    strings[i] = array.GetValue(i).ToString();
                }

                _values = strings;
                _dbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
            }

            _locator = locator;
        }

        public void Apply(CommandBuilder builder)
        {
            var param = builder.AddParameter(_values, _dbType);

            builder.Append(_locator);
            builder.Append(" = ANY(:");
            builder.Append(param.ParameterName);
            builder.Append(")");
        }

        public bool Contains(string sqlText)
        {
            return false;
        }
    }
}
