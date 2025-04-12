using System.Data.Common;
using ImTools;
using JasperFx.Core;
using Weasel.Core.Names;

namespace Weasel.Core;

/// <summary>
///     Primarily responsible for handling .Net to database engine type mappings
/// </summary>
public interface IDatabaseProvider
{
    string DefaultDatabaseSchemaName { get; }
    /// <summary>
    /// Maps databaseObjectName to include local conventions around case sensitivity etc.
    /// </summary>
    /// <param name="objectName">database object name (e.g. table, sequence)</param>
    /// <returns></returns>
    string ToQualifiedName(string objectName) => objectName;

    string ToQualifiedName(string schemaName, string objectName) =>
        $"{ToQualifiedName(schemaName)}.{ToQualifiedName(objectName)}";

    public DbObjectName Parse(string qualifiedName)
    {
        var parts = QualifiedNameParser.Parse(this, qualifiedName);

        return Parse(parts[0], parts[1]);
    }

    DbObjectName Parse(string schemaName, string objectName);
}

/// <summary>
///     Primarily responsible for handling .Net to database engine type mappings
/// </summary>
/// <typeparam name="TCommand"></typeparam>
/// <typeparam name="TParameter"></typeparam>
/// <typeparam name="TTransaction"></typeparam>
/// <typeparam name="TParameterType"></typeparam>
/// <typeparam name="TDataReader"></typeparam>
public interface IDatabaseProvider<in TCommand, in TParameter, TParameterType>: IDatabaseProvider
    where TCommand : DbCommand
    where TParameter : DbParameter
    where TParameterType : struct
{
    TParameterType StringParameterType { get; }
    TParameterType IntegerParameterType { get; }
    TParameterType LongParameterType { get; }
    TParameterType GuidParameterType { get; }
    TParameterType BoolParameterType { get; }
    TParameterType DoubleParameterType { get; }
    TParameterType? TryGetDbType(Type? type);
    TParameterType ToParameterType(Type type);
    Type[] ResolveTypes(TParameterType parameterType);
    string GetDatabaseType(Type memberType, EnumStorage enumStyle);
    void AddParameter(TCommand command, TParameter parameter);
    void SetParameterType(TParameter parameter, TParameterType dbType);
}

/// <summary>
///     Base type for database providers. Primarily responsible for handling .Net to database engine type mappings
/// </summary>
/// <typeparam name="TCommand"></typeparam>
/// <typeparam name="TParameter"></typeparam>
/// <typeparam name="TConnection"></typeparam>
/// <typeparam name="TTransaction"></typeparam>
/// <typeparam name="TParameterType"></typeparam>
/// <typeparam name="TDataReader"></typeparam>
public abstract class DatabaseProvider<TCommand, TParameter, TParameterType>
    : IDatabaseProvider<TCommand, TParameter, TParameterType>
    where TCommand : DbCommand
    where TParameter : DbParameter
    where TParameterType : struct
{
    protected readonly Ref<ImHashMap<Type, string>> DatabaseTypeMemo = Ref.Of(ImHashMap<Type, string>.Empty);

    protected readonly Ref<ImHashMap<Type, TParameterType?>> ParameterTypeMemo =
        Ref.Of(ImHashMap<Type, TParameterType?>.Empty);

    protected readonly Ref<ImHashMap<TParameterType, Type[]>>
        TypeMemo = Ref.Of(ImHashMap<TParameterType, Type[]>.Empty);

    protected DatabaseProvider(string defaultDatabaseSchemaName)
    {
        DefaultDatabaseSchemaName = defaultDatabaseSchemaName;
        storeMappings();
        StringParameterType = ToParameterType(typeof(string));
        IntegerParameterType = ToParameterType(typeof(int));
        LongParameterType = ToParameterType(typeof(long));
        GuidParameterType = ToParameterType(typeof(Guid));
        BoolParameterType = ToParameterType(typeof(bool));
        DoubleParameterType = ToParameterType(typeof(double));
    }

    public string DefaultDatabaseSchemaName { get; }

    public TParameterType? TryGetDbType(Type? type)
    {
        if (type == null || !determineParameterType(type, out var dbType))
        {
            return null;
        }

        return dbType;
    }

    public TParameterType ToParameterType(Type type)
    {
        if (determineParameterType(type, out var dbType))
        {
            return dbType;
        }

        throw new NotSupportedException($"Can't infer {typeof(TParameterType).Name} for type " + type);
    }

    public Type[] ResolveTypes(TParameterType parameterType)
    {
        if (TypeMemo.Value.TryFind(parameterType, out var values))
        {
            return values;
        }

        values = determineClrTypesForParameterType(parameterType);

        TypeMemo.Swap(d => d.AddOrUpdate(parameterType, values!));

        return values;
    }

    public abstract string GetDatabaseType(Type memberType, EnumStorage enumStyle);


    public abstract void AddParameter(TCommand command, TParameter parameter);

    public abstract void SetParameterType(TParameter parameter, TParameterType dbType);


    public TParameterType StringParameterType { get; }
    public TParameterType IntegerParameterType { get; }
    public TParameterType LongParameterType { get; }
    public TParameterType GuidParameterType { get; }
    public TParameterType BoolParameterType { get; }
    public TParameterType DoubleParameterType { get; }

    protected abstract void storeMappings();

    protected abstract bool determineParameterType(Type type, out TParameterType dbType);

    protected void store<T>(TParameterType parameterType, string databaseType)
    {
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(typeof(T), databaseType));
        ParameterTypeMemo.Swap(d => d.AddOrUpdate(typeof(T), parameterType));
    }

    public void RegisterMapping(Type type, string databaseType, TParameterType? parameterType)
    {
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
        ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, parameterType));
    }

    protected abstract Type[] determineClrTypesForParameterType(TParameterType dbType);

    public TParameter AddParameter(TCommand command, object? value, TParameterType? dbType = null)
    {
        var name = "p" + command.Parameters.Count;

        var parameter = (TParameter)command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;

        if (dbType.HasValue)
        {
            SetParameterType(parameter, dbType.Value);
        }

        command.Parameters.Add(parameter);

        return parameter;
    }

    /// <summary>
    ///     Finds or adds a new parameter with the specified name and returns the parameter
    /// </summary>
    /// <param name="command"></param>
    /// <param name="name"></param>
    /// <param name="value"></param>
    /// <param name="dbType"></param>
    /// <returns></returns>
    public TParameter AddNamedParameter(TCommand command, string name, object? value, TParameterType? dbType = null)
    {
        var existing = command.Parameters.OfType<TParameter>().FirstOrDefault(x => x.ParameterName == name);
        if (existing != null)
        {
            return existing;
        }

        var parameter = (TParameter)command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;


        if (dbType.HasValue)
        {
            SetParameterType(parameter, dbType.Value);
        }
        else if (value != null)
        {
            SetParameterType(parameter, ToParameterType(value.GetType()));
        }

        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);

        return parameter;
    }

    public TParameter AddNamedParameter(TCommand command, string name, string value)
    {
        return AddNamedParameter(command, name, value, StringParameterType);
    }

    public TParameter AddNamedParameter(TCommand command, string name, int value)
    {
        return AddNamedParameter(command, name, value, IntegerParameterType);
    }

    public TParameter AddNamedParameter(TCommand command, string name, long value)
    {
        return AddNamedParameter(command, name, value, LongParameterType);
    }

    public TParameter AddNamedParameter(TCommand command, string name, double value)
    {
        return AddNamedParameter(command, name, value, DoubleParameterType);
    }

    public TParameter AddNamedParameter(TCommand command, string name, Guid value)
    {
        return AddNamedParameter(command, name, value, GuidParameterType);
    }

    public TParameter AddNamedParameter(TCommand command, string name, bool value)
    {
        return AddNamedParameter(command, name, value, BoolParameterType);
    }

    public DbObjectName Parse(string qualifiedName)
    {
        var parts = QualifiedNameParser.Parse(this, qualifiedName);

        return Parse(parts[0], parts[1]);
    }

    public virtual string ToQualifiedName(string objectName) => objectName;

    public string ToQualifiedName(string schemaName, string objectName) =>
        $"{ToQualifiedName(schemaName)}.{ToQualifiedName(objectName)}";

    public abstract DbObjectName Parse(string schemaName, string objectName);
}
