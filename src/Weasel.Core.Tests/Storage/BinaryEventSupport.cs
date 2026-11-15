using System.Buffers;
using System.Data.Common;
using JasperFx.Events;
using Npgsql;
using NpgsqlTypes;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;

namespace Weasel.Core.Tests.StorageHelpers;

/// <summary>weasel#831: an event serialized to <c>bdata</c>.</summary>
public record BinaryHappened(byte[] Payload);

/// <summary>weasel#831: an event serialized to <c>data</c>, with NULL <c>bdata</c>.</summary>
public record JsonHappened(string Name);

/// <summary>
///     weasel#831: builds all three event-storage descriptors over one per-event INSERT, with a <c>bdata</c>
///     slot that serializes <see cref="BinaryHappened" /> and counts its calls. With
///     <see cref="UseBufferSlot" />, the descriptors also carry <c>WriteEventBdata</c>.
/// </summary>
internal sealed class BinaryEventDialect(IStorageDialect storageDialect, string sqlPrefix = "insert into events values (",
    string sqlSuffix = ")"): IEventStoreSqlDialect
{
    public bool UseBufferSlot { get; init; } = true;

    public int SerializeCalls { get; private set; }
    public int WriteCalls { get; private set; }

    /// <summary>Thrown from the buffer slot, once, when set.</summary>
    public Exception? WriteFailure { get; set; }

    public byte[]? SerializeBdata(IEvent e)
    {
        SerializeCalls++;

        // As a byte[]-returning serializer does: a fresh exact-length array per call
        return e.Data is BinaryHappened b ? b.Payload.ToArray() : null;
    }

    public bool WriteBdata(IEvent e, IBufferWriter<byte> writer)
    {
        WriteCalls++;
        if (WriteFailure is { } failure)
        {
            WriteFailure = null;
            writer.Write<byte>([1, 2, 3]);
            throw failure;
        }

        if (e.Data is not BinaryHappened b)
        {
            return false;
        }

        writer.Write(b.Payload);
        return true;
    }

    private static string data(IEvent e) => e.Data is JsonHappened j ? $"{{\"Name\":\"{j.Name}\"}}" : "{}";

    private Func<IEvent, IBufferWriter<byte>, bool>? write => UseBufferSlot ? WriteBdata : null;

    public RichEventStorageDescriptor BuildRichDescriptor(EventRegistry graph, IStorageSerializer serializer)
        => new(sqlPrefix, sqlSuffix, string.Empty, string.Empty, data, SerializeBdata, [])
        {
            AppendEventQuickWithVersionSqlSuffix = sqlSuffix,
            IsGuidStreamIdentity = true,
            Dialect = storageDialect,
            WriteEventBdata = write
        };

    public QuickEventStorageDescriptor BuildQuickDescriptor(EventRegistry graph, IStorageSerializer serializer)
        => new(string.Empty, string.Empty, string.Empty, data, SerializeBdata)
        {
            AppendEventSqlPrefix = sqlPrefix,
            AppendEventSqlSuffix = sqlSuffix,
            AppendEventFullSqlSuffix = sqlSuffix,
            IsGuidStreamIdentity = true,
            Dialect = storageDialect,
            WriteEventBdata = write
        };

    public QuickWithServerTimestampsEventStorageDescriptor BuildQuickWithServerTimestampsDescriptor(
        EventRegistry graph, IStorageSerializer serializer)
        => new(string.Empty, string.Empty, string.Empty, data, SerializeBdata)
        {
            AppendEventSqlPrefix = sqlPrefix,
            AppendEventSqlSuffix = sqlSuffix,
            AppendEventFullSqlSuffix = sqlSuffix,
            IsGuidStreamIdentity = true,
            Dialect = storageDialect,
            WriteEventBdata = write
        };
}

/// <summary>
///     An <see cref="IStorageDialect" /> for the per-event append path only: Npgsql parameter types, and the
///     interface's default <see cref="IStorageDialect.SetBinaryParameterValue" />, as a dialect compiled
///     before weasel#831 gets.
/// </summary>
internal class StaleStorageDialect: IStorageDialect
{
    public DbCommand BuildLoadCommand(string loaderSql, object rawId, string? tenant) => throw new NotSupportedException();
    public DbParameter CreateIdArrayParameter(Array rawIds, Type rawSqlType) => throw new NotSupportedException();

    public DbCommand BuildLoadManyCommand(string loadArraySql, DbParameter idArrayParameter, string? tenant)
        => throw new NotSupportedException();

    public ISqlFragment ByIdFilter(object rawId) => throw new NotSupportedException();
    public bool IsUndefinedTable(Exception exception) => false;
    public void SetIdParameterType(DbParameter parameter, Type rawSqlType) => throw new NotSupportedException();

    public void SetParameterType(DbParameter parameter, StorageColumnType type)
    {
        if (parameter is not NpgsqlParameter npgsql)
        {
            return;
        }

        npgsql.NpgsqlDbType = type switch
        {
            StorageColumnType.String => NpgsqlDbType.Text,
            StorageColumnType.Guid => NpgsqlDbType.Uuid,
            StorageColumnType.Long => NpgsqlDbType.Bigint,
            StorageColumnType.Int => NpgsqlDbType.Integer,
            StorageColumnType.Boolean => NpgsqlDbType.Boolean,
            StorageColumnType.Timestamp => NpgsqlDbType.TimestampTz,
            StorageColumnType.Json => NpgsqlDbType.Jsonb,
            StorageColumnType.Binary => NpgsqlDbType.Bytea,
            _ => npgsql.NpgsqlDbType
        };
    }
}

/// <summary>Binds the slice itself, as Npgsql allows for <c>bytea</c>.</summary>
internal sealed class ZeroCopyStorageDialect: StaleStorageDialect, IStorageDialect
{
    public int BinaryValues { get; private set; }

    public void SetBinaryParameterValue(DbParameter parameter, ReadOnlyMemory<byte> value)
    {
        BinaryValues++;
        parameter.Value = value;
    }
}
