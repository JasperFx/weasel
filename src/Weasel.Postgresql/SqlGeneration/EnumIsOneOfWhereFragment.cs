using JasperFx.Core.Reflection;
using NpgsqlTypes;
using Weasel.Core;

namespace Weasel.Postgresql.SqlGeneration;

public class EnumIsOneOfWhereFragment: ISqlFragment
{
    private readonly NpgsqlDbType _dbType;
    private readonly bool _listContainsNullEntry;
    private readonly string _locator;
    private readonly object _values;

    public EnumIsOneOfWhereFragment(object values, EnumStorage enumStorage, string locator)
    {
        var array = values.As<Array>();
        if (enumStorage == EnumStorage.AsInteger)
        {
            var numberEntries = new int?[array.Length];
            for (var i = 0; i < array.Length; i++)
            {
                var numberEntry = array.GetValue(i);
                if (numberEntry is null)
                {
                    _listContainsNullEntry = true;
                    numberEntries[i] = null;
                    continue;
                }

                numberEntries[i] = numberEntry.As<int>();
            }

            _values = numberEntries.Where(n => n != null).ToArray();
            _dbType = NpgsqlDbType.Integer | NpgsqlDbType.Array;
        }
        else
        {
            var stringEntries = new string?[array.Length];
            for (var i = 0; i < array.Length; i++)
            {
                var stringEntry = array.GetValue(i);
                if (stringEntry is null)
                {
                    _listContainsNullEntry = true;
                    stringEntries[i] = null;
                    continue;
                }

                stringEntries[i] = stringEntry.ToString()!;
            }

            _values = stringEntries.Where(n => n != null).ToArray();
            _dbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
        }

        _locator = locator;
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append("(");
        builder.Append(_locator);
        builder.Append(" = ANY(");
        builder.AppendParameter(_values, _dbType);
        builder.Append(")");
        if (_listContainsNullEntry)
        {
            builder.Append(" OR ");
            builder.Append(_locator);
            builder.Append(" is null");
        }
        else
        {
            builder.Append(" AND ");
            builder.Append(_locator);
            builder.Append(" is not null");
        }

        builder.Append(")");
    }

    public bool Contains(string sqlText)
    {
        return false;
    }
}
