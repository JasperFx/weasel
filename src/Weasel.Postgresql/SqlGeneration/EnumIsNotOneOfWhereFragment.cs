using JasperFx.Core.Reflection;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql.SqlGeneration;

public class EnumIsNotOneOfWhereFragment: ISqlFragment
{
    private readonly NpgsqlDbType _dbType;
    private readonly string _locator;
    private readonly object _values;

    public EnumIsNotOneOfWhereFragment(object values, EnumStorage enumStorage, string locator)
    {
        var array = values.As<Array>();
        if (enumStorage == EnumStorage.AsInteger)
        {
            var numbers = new int[array.Length];

            for (var i = 0; i < array.Length; i++)
            {
                numbers[i] = array.GetValue(i)!.As<int>();
            }

            _values = numbers;
            _dbType = NpgsqlDbType.Integer | NpgsqlDbType.Array;
        }
        else
        {
            var strings = new string[array.Length];

            for (var i = 0; i < array.Length; i++)
            {
                strings[i] = array.GetValue(i)!.ToString()!;
            }

            _values = strings;
            _dbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
        }

        _locator = locator;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append("NOT(");
        builder.Append(_locator);
        builder.Append(" = ANY(");
        builder.AppendParameter(_values, _dbType);
        builder.Append(")");
        builder.Append(")");
    }

    public bool Contains(string sqlText)
    {
        return false;
    }
}
