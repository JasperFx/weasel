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
        : this(values, enumStorage, locator, null)
    {
    }

    /// <param name="nameForValue">
    ///     Renders one enum value as the string the serializer actually stored for it. Null keeps
    ///     the historical <see cref="object.ToString" /> behaviour, which is the member's
    ///     <em>declared</em> name. See the sibling <see cref="EnumIsOneOfWhereFragment" /> and
    ///     weasel#591.
    /// </param>
    public EnumIsNotOneOfWhereFragment(object values, EnumStorage enumStorage, string locator,
        Func<object, string>? nameForValue)
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
                var entry = array.GetValue(i)!;
                strings[i] = nameForValue?.Invoke(entry) ?? entry.ToString()!;
            }

            _values = strings;
            _dbType = NpgsqlDbType.Varchar | NpgsqlDbType.Array;
        }

        _locator = locator;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append("NOT(");
        builder.Append(_locator);
        builder.Append(" = ANY(");
        builder.AppendParameter(_values, _dbType);
        builder.Append(")");
        builder.Append(")");
    }
}
