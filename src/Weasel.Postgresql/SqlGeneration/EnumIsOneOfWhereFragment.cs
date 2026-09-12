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
        : this(values, enumStorage, locator, null)
    {
    }

    /// <param name="nameForValue">
    ///     Renders one enum value as the string the serializer actually stored for it. Null keeps
    ///     the historical <see cref="object.ToString" /> behaviour, which is the member's
    ///     <em>declared</em> name. Those differ as soon as the member was renamed —
    ///     <c>[JsonStringEnumMemberName]</c> on System.Text.Json, <c>[EnumMember]</c> on Newtonsoft —
    ///     and a filter comparing against the declared name matches nothing and reports it as "no
    ///     rows" rather than as an error. See weasel#591, raised from marten#5376.
    /// </param>
    public EnumIsOneOfWhereFragment(object values, EnumStorage enumStorage, string locator,
        Func<object, string>? nameForValue)
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

                stringEntries[i] = nameForValue?.Invoke(stringEntry) ?? stringEntry.ToString()!;
            }

            _values = stringEntries.Where(n => n != null).ToArray();
            _dbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
        }

        _locator = locator;
    }

    public void Apply(ICommandBuilder builder)
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
}
