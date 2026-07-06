#nullable enable
using System;
using System.Data.Common;
using Weasel.Core.SqlGeneration;

namespace Weasel.Storage;

/// <summary>
///     The ADO/SQL-dialect strategy the shared document-storage runtime delegates its
///     provider-specific concerns to, instead of constructing provider command/parameter types and
///     emitting dialect SQL directly. One implementation is supplied per dialect (e.g. Marten's
///     <c>PostgresStorageDialect</c>), keeping the storage runtime free of any direct provider
///     reference so it can be shared.
/// </summary>
/// <remarks>
///     Only the genuinely dialect-specific concerns live here — command/parameter materialization,
///     the id-equality filter (which carries the provider parameter type), and provider error-code
///     interpretation. Mechanical scalar parameter typing routes through Weasel's
///     <c>IDatabaseProvider.ToParameterType</c>, and the document-body JSON parameter is
///     dialect-polymorphic via <see cref="IStorageSerializer.WriteToParameter(DbParameter, object?)"/>.
/// </remarks>
public interface IStorageDialect
{
    /// <summary>
    ///     Build a single-document load command over the storage's prebuilt loader SQL, binding the
    ///     raw id and, for conjoined tenancy, the tenant id.
    /// </summary>
    DbCommand BuildLoadCommand(string loaderSql, object rawId, string? tenant);

    /// <summary>
    ///     Create the id-array parameter for a load-many command from the raw id values and the .NET
    ///     type of a single raw id. A provider may bind a single array parameter (<c>= ANY($1)</c>) or
    ///     shape it differently.
    /// </summary>
    DbParameter CreateIdArrayParameter(Array rawIds, Type rawSqlType);

    /// <summary>
    ///     Build a load-many command over the storage's prebuilt array-loader SQL, binding the
    ///     id-array parameter (from <see cref="CreateIdArrayParameter"/>) and, for conjoined tenancy,
    ///     the tenant id.
    /// </summary>
    DbCommand BuildLoadManyCommand(string loadArraySql, DbParameter idArrayParameter, string? tenant);

    /// <summary>
    ///     The id-equality <see cref="ISqlFragment"/> for a raw id — carries the dialect's parameter
    ///     type so the emitted predicate binds correctly.
    /// </summary>
    ISqlFragment ByIdFilter(object rawId);

    /// <summary>
    ///     True when the exception represents an "undefined table" (e.g. truncating a table that
    ///     hasn't been created yet) — the one provider error code the storage runtime swallows.
    /// </summary>
    bool IsUndefinedTable(Exception exception);

    /// <summary>
    ///     Set the provider parameter type on a write parameter for a fixed <see cref="StorageColumnType"/>,
    ///     letting the storage runtime bind id/tenant/version/revision slots as <see cref="DbParameter"/>
    ///     without naming a provider type.
    /// </summary>
    void SetParameterType(DbParameter parameter, StorageColumnType type);

    /// <summary>
    ///     Set the provider parameter type for a document id slot from the .NET type of the raw id
    ///     value. Distinct from <see cref="SetParameterType"/> because an id can be any supported
    ///     scalar / strong-typed-id inner type.
    /// </summary>
    void SetIdParameterType(DbParameter parameter, Type rawSqlType);
}
