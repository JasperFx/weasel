using System.Data.Common;
using ImTools;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core.Names;

namespace Weasel.Core;

/// <summary>
///     Which CLR type's parameter mapping stands in for an enum, given how the enum is stored.
/// </summary>
/// <remarks>
///     <para>
///     <c>GetDatabaseType</c> has always generated an integer column for an enum under
///     <see cref="EnumStorage.AsInteger" /> and a character column under
///     <see cref="EnumStorage.AsString" />. The parameter path had no <see cref="EnumStorage" /> in
///     scope at all, so every provider picked one constant and three of six then threw against the
///     column Weasel itself generates. This is the rule that lets the two agree. weasel#761.
///     </para>
///     <para>
///     <see cref="EnumStorage.AsInteger" /> substitutes the enum's <em>underlying</em> type rather
///     than <see cref="int" /> flatly, so a <c>long</c>-backed enum carrying a value past
///     <see cref="int.MaxValue" /> is not truncated and a <c>byte</c>-backed one is not widened past
///     what its column holds. A provider with no mapping for that underlying type falls back to
///     <see cref="int" />, which every provider maps.
///     </para>
/// </remarks>
public static class EnumParameterType
{
    /// <summary>
    ///     The stand-in type for <paramref name="type" />, or null when it is not an enum — in which
    ///     case there is nothing to substitute and the type resolves as itself. A
    ///     <c>Nullable&lt;TEnum&gt;</c> is treated as its enum.
    /// </summary>
    public static Type? Substitute(Type type, EnumStorage enumStyle)
    {
        var inner = type.IsNullable() ? type.GetInnerTypeFromNullable() : type;

        if (!inner.IsEnum)
        {
            return null;
        }

        return enumStyle == EnumStorage.AsString ? typeof(string) : Enum.GetUnderlyingType(inner);
    }
}

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

    /// <summary>
    ///     This dialect's identifier rules, when it has them. Used to split a qualified name on the
    ///     dots that separate identifiers rather than on every dot (weasel#501).
    /// </summary>
    /// <remarks>
    ///     Defaulted rather than required so that an implementation outside Weasel keeps compiling and
    ///     keeps its current splitting. Every provider in the box supplies its own.
    /// </remarks>
    IdentifierRules? Rules => null;

    public DbObjectName Parse(string qualifiedName)
    {
        var parts = QualifiedNameParser.Parse(this, qualifiedName);

        return Parse(parts[0], parts[1]);
    }

    DbObjectName Parse(string schemaName, string objectName);


    /// <summary>
    /// Modifies the connection string by adding or changing the application name to the specified
    /// applicationName argument
    /// </summary>
    /// <param name="connectionString"></param>
    /// <param name="applicationName"></param>
    /// <returns></returns>
    string AddApplicationNameToConnectionString(string connectionString, string applicationName);
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

    /// <summary>
    ///     Resolve the parameter type for a value the caller supplied without an explicit type,
    ///     or null when this provider has no mapping for it — in which case the parameter is
    ///     left untyped and the driver infers it. Defaults to keying off the value's CLR type.
    ///     Providers whose choice depends on the <em>value</em> rather than its type override
    ///     this — PostgreSQL picks <c>timestamptz</c> vs <c>timestamp</c> from
    ///     <see cref="DateTime.Kind" />, which a per-type mapping cannot express. weasel#403.
    /// </summary>
    /// <remarks>
    ///     Stands to <see cref="ToParameterTypeForValue(object)" /> as
    ///     <see cref="TryGetDbType(Type)" /> does to <see cref="ToParameterType(Type)" />: null means
    ///     "unmapped, defer". A provider that
    ///     actively rejects a type — SQL Server and arrays, say — still throws. weasel#404.
    /// </remarks>
    TParameterType? TryGetDbTypeForValue(object value) => TryGetDbType(value.GetType());

    /// <summary>
    ///     Resolve the parameter type for a value, throwing when this provider has no mapping.
    ///     Prefer <see cref="TryGetDbTypeForValue(object)" /> where deferring to the driver is
    ///     acceptable. weasel#403.
    /// </summary>
    TParameterType ToParameterTypeForValue(object value) =>
        TryGetDbTypeForValue(value) ??
        throw new NotSupportedException($"Can't infer {typeof(TParameterType).Name} for value of type " +
                                        value.GetType());

    /// <summary>
    ///     The parameter type for a CLR type, resolving an enum the way <paramref name="enumStyle" />
    ///     says it is stored — so the parameter agrees with the column
    ///     <see cref="GetDatabaseType" /> generates for the same enum. weasel#761.
    /// </summary>
    /// <remarks>
    ///     Defaulted so that an implementation outside Weasel keeps compiling. Prefer this over
    ///     <see cref="ToParameterType(Type)" /> wherever the caller knows its
    ///     <see cref="EnumStorage" />: the parameterless overload has to assume
    ///     <see cref="EnumStorage.AsInteger" />, and binding an enum as an integer against a
    ///     <c>varchar</c> column is the same defect weasel#758 was about, one type over.
    /// </remarks>
    TParameterType ToParameterType(Type type, EnumStorage enumStyle) =>
        EnumParameterType.Substitute(type, enumStyle) is { } substitute
            ? TryGetDbType(substitute) ?? ToParameterType(typeof(int))
            : ToParameterType(type);

    /// <summary>
    ///     <see cref="TryGetDbType(Type)" />, resolving an enum by its <see cref="EnumStorage" />.
    ///     Null still means "unmapped, defer to the driver". weasel#761.
    /// </summary>
    TParameterType? TryGetDbType(Type? type, EnumStorage enumStyle) =>
        type is null
            ? null
            : TryGetDbType(EnumParameterType.Substitute(type, enumStyle) ?? type);

    /// <summary>
    ///     <see cref="TryGetDbTypeForValue(object)" />, resolving an enum by its
    ///     <see cref="EnumStorage" />. weasel#761.
    /// </summary>
    TParameterType? TryGetDbTypeForValue(object value, EnumStorage enumStyle) =>
        EnumParameterType.Substitute(value.GetType(), enumStyle) is { } substitute
            ? TryGetDbType(substitute)
            : TryGetDbTypeForValue(value);

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

    /// <summary>
    ///     The parameter type for a CLR type, resolving an enum the way <paramref name="enumStyle" />
    ///     says it is stored — so the parameter agrees with the column
    ///     <see cref="GetDatabaseType" /> generates for the same enum.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     Prefer this over <see cref="ToParameterType(Type)" /> wherever the caller knows its
    ///     <see cref="EnumStorage" />. The parameterless overload has no <see cref="EnumStorage" />
    ///     in scope, so it has to assume <see cref="EnumStorage.AsInteger" /> — which is the
    ///     <c>JasperFx</c> default, but binding an enum as an integer against the <c>varchar</c>
    ///     column an <see cref="EnumStorage.AsString" /> consumer generated is the same defect
    ///     weasel#758 was about, one type over.
    ///     </para>
    ///     <para>
    ///     What each provider did before this existed, measured against the column its own
    ///     <c>GetDatabaseType</c> generates rather than reasoned about — and three of the six did
    ///     not merely convert, they threw:
    ///     </para>
    ///     <list type="table">
    ///         <item><term>SQL Server, <c>Variant</c></term><description>threw against both columns:
    ///         "Implicit conversion from data type sql_variant to int is not allowed"</description></item>
    ///         <item><term>MySQL, <c>VarChar</c></term><description>threw against the AsInteger
    ///         column: "Incorrect integer value: 'Strawberry'"</description></item>
    ///         <item><term>Oracle, <c>Varchar2</c></term><description>threw against the AsInteger
    ///         column: ORA-01722</description></item>
    ///         <item><term>SQLite, <c>Text</c></term><description>stored the integer either way —
    ///         type affinity makes it moot</description></item>
    ///         <item><term>PostgreSQL and Firebird, <c>Integer</c></term><description>correct for
    ///         AsInteger, wrong for AsString</description></item>
    ///     </list>
    /// </remarks>
    public TParameterType ToParameterType(Type type, EnumStorage enumStyle)
    {
        if (EnumParameterType.Substitute(type, enumStyle) is not { } substitute)
        {
            return ToParameterType(type);
        }

        // Fall back to int when the provider has no mapping for the underlying type -- every
        // provider maps int, and PostgreSQL resolves integers through Npgsql's own map rather
        // than a storeMappings() entry.
        return TryGetDbType(substitute) ?? ToParameterType(typeof(int));
    }

    /// <summary>
    ///     <see cref="TryGetDbType(Type)" />, resolving an enum by its <see cref="EnumStorage" />.
    ///     Null still means "unmapped, defer to the driver". weasel#761.
    /// </summary>
    public TParameterType? TryGetDbType(Type? type, EnumStorage enumStyle)
    {
        if (type == null)
        {
            return null;
        }

        return TryGetDbType(EnumParameterType.Substitute(type, enumStyle) ?? type);
    }

    /// <summary>
    ///     <see cref="TryGetDbTypeForValue(object)" />, resolving an enum by its
    ///     <see cref="EnumStorage" />. weasel#761.
    /// </summary>
    public TParameterType? TryGetDbTypeForValue(object value, EnumStorage enumStyle)
    {
        if (EnumParameterType.Substitute(value.GetType(), enumStyle) is { } substitute)
        {
            return TryGetDbType(substitute);
        }

        return TryGetDbTypeForValue(value);
    }

    /// <summary>
    ///     Resolve the parameter type for a value the caller supplied without an explicit type,
    ///     or null when this provider has no mapping for it. Keys off the value's CLR type
    ///     unless a provider overrides it because its choice depends on the value itself.
    ///     weasel#403, weasel#404.
    /// </summary>
    public virtual TParameterType? TryGetDbTypeForValue(object value)
    {
        return TryGetDbType(value.GetType());
    }

    /// <summary>
    ///     Resolve the parameter type for a value, throwing when this provider has no mapping.
    ///     Prefer <see cref="TryGetDbTypeForValue(object)" /> where deferring to the driver is
    ///     acceptable. weasel#403.
    /// </summary>
    public TParameterType ToParameterTypeForValue(object value)
    {
        return TryGetDbTypeForValue(value) ??
               throw new NotSupportedException($"Can't infer {typeof(TParameterType).Name} for value of type " +
                                               value.GetType());
    }

    public Type[] ResolveTypes(TParameterType parameterType)
    {
        if (TypeMemo.Value.TryFind(parameterType, out var values))
        {
            return values;
        }

        values = determineClrTypesForParameterType(parameterType);
        rememberClrTypes(parameterType, values);

        return values;
    }

    // The memo writes below live in their own methods on purpose. A lambda that captures a
    // method's parameter makes the compiler allocate its closure at the *top* of that method, so
    // a lookup that hit the memo and never reached the Swap still paid for the closure: 32 bytes
    // on every TryGetDbType / ToParameterType call, i.e. on every bound parameter (weasel#804).
    private void rememberClrTypes(TParameterType parameterType, Type[] values)
    {
        TypeMemo.Swap(d => d.AddOrUpdate(parameterType, values));
    }

    /// <summary>
    ///     Records <paramref name="databaseType" /> for <paramref name="type" /> in the database-type
    ///     memo. Kept out of line so a caller's memo hit allocates no closure. weasel#804.
    /// </summary>
    protected void rememberDatabaseType(Type type, string? databaseType)
    {
        DatabaseTypeMemo.Swap(d => d.AddOrUpdate(type, databaseType));
    }

    /// <summary>
    ///     Records <paramref name="parameterType" /> for <paramref name="type" /> in the
    ///     parameter-type memo — null meaning "looked up, unmapped". Kept out of line so a caller's
    ///     memo hit allocates no closure. weasel#804.
    /// </summary>
    protected void rememberParameterType(Type type, TParameterType? parameterType)
    {
        ParameterTypeMemo.Swap(d => d.AddOrUpdate(type, parameterType));
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

    /// <summary>
    ///     Shared memo lookup for the database-type string of a CLR type, with the
    ///     standard nullable-promote fallback (if <c>T?</c> is asked but <c>T</c> is
    ///     in the memo, copy the entry to <c>T?</c> and return it). Returns null
    ///     when neither the type nor its inner-nullable is mapped — subclasses then
    ///     apply provider-specific fallbacks (PG queries Npgsql's plugin type map,
    ///     SS/Oracle/MySQL throw, SQLite returns null to signal "TEXT/JSON").
    /// </summary>
    protected string? ResolveDatabaseTypeFromMemo(Type type)
    {
        if (DatabaseTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        if (type.IsNullable() && DatabaseTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var inner))
        {
            rememberDatabaseType(type, inner);
            return inner;
        }

        return null;
    }

    /// <summary>
    ///     Shared memo lookup for the provider parameter-type enum of a CLR type,
    ///     mirror of <see cref="ResolveDatabaseTypeFromMemo" />. Subclasses apply
    ///     their own fallback when this returns null (PG queries Npgsql plugin;
    ///     SS returns <c>Variant</c>; Oracle returns <c>Varchar2</c>; MySQL returns
    ///     <c>VarChar</c>; SQLite returns <c>Text</c>).
    /// </summary>
    protected TParameterType? ResolveParameterTypeFromMemo(Type type)
    {
        if (ParameterTypeMemo.Value.TryFind(type, out var value))
        {
            return value;
        }

        if (type.IsNullable() && ParameterTypeMemo.Value.TryFind(type.GetInnerTypeFromNullable(), out var inner))
        {
            rememberParameterType(type, inner);
            return inner;
        }

        return null;
    }

    protected abstract Type[] determineClrTypesForParameterType(TParameterType dbType);

    public TParameter AddParameter(TCommand command, object? value, TParameterType? dbType = null)
    {
        var name = ParameterNames.ForPosition(command.Parameters.Count);

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
        else if (value != null && TryGetDbTypeForValue(value) is { } inferred)
        {
            // Unmapped types are left untyped for the driver to infer, which is what the
            // sibling AddParameter has always done. Providers that actively reject a type
            // still throw out of TryGetDbTypeForValue. weasel#404.
            SetParameterType(parameter, inferred);
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

    /// <summary>
    ///     This dialect's identifier rules. Declared here as well as on <see cref="IDatabaseProvider" />
    ///     so that a derived provider can supply it.
    /// </summary>
    /// <remarks>
    ///     A default interface member alone would not do. The interface mapping is fixed at the class
    ///     that implements the interface -- this one -- so a <c>Rules</c> declared on a derived provider
    ///     would never be reached and every provider would silently keep the default. Virtual, and
    ///     null by default, so an implementation outside Weasel keeps compiling.
    /// </remarks>
    public virtual IdentifierRules? Rules => null;

    public DbObjectName Parse(string qualifiedName)
    {
        var parts = QualifiedNameParser.Parse(this, qualifiedName);

        return Parse(parts[0], parts[1]);
    }

    public virtual string ToQualifiedName(string objectName) => objectName;

    public string ToQualifiedName(string schemaName, string objectName) =>
        $"{ToQualifiedName(schemaName)}.{ToQualifiedName(objectName)}";

    public abstract DbObjectName Parse(string schemaName, string objectName);
    public abstract string AddApplicationNameToConnectionString(string connectionString, string applicationName);
}
