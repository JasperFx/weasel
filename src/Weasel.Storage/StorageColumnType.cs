#nullable enable

namespace Weasel.Storage;

/// <summary>
///     A small, dialect-neutral vocabulary of logical column types a metadata binder uses when
///     contributing a value to the bulk-load path. A dialect maps each to its own provider parameter
///     type (Postgres: <c>NpgsqlDbType</c>; SQL Server: <c>SqlDbType</c>). This is the seam that lets
///     the fixed metadata binders drop their direct provider-writer dependency so they can be shared.
/// </summary>
/// <remarks>
///     Deliberately covers only the fixed metadata columns' types. User-defined duplicated fields
///     carry an arbitrary, user-overridable provider type that cannot be reduced to this set, so their
///     bulk writing stays on the provider-native path.
/// </remarks>
public enum StorageColumnType
{
    String,
    Guid,
    Long,
    Int,
    Boolean,
    Timestamp,
    Json
}
