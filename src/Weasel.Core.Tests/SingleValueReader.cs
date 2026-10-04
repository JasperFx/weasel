using System.Data.Common;
using Weasel.Core.Identity;

namespace Weasel.Core.Tests;

/// <summary>
///     The smallest reader that can answer <c>GetFieldValue&lt;T&gt;(0)</c> — enough to exercise
///     <see cref="IIdentification.ReadIdFromReader" /> without a database.
/// </summary>
internal sealed class SingleValueReader(object value): DbDataReader
{
    public override T GetFieldValue<T>(int ordinal) => (T)value;
    public override object GetValue(int ordinal) => value;
    public override int FieldCount => 1;
    public override bool HasRows => true;
    public override bool IsClosed => false;
    public override int RecordsAffected => 0;
    public override int Depth => 0;
    public override object this[int ordinal] => value;
    public override object this[string name] => value;
    public override bool Read() => true;
    public override bool NextResult() => false;
    public override bool IsDBNull(int ordinal) => false;
    public override Type GetFieldType(int ordinal) => value.GetType();
    public override string GetName(int ordinal) => "id";
    public override int GetOrdinal(string name) => 0;
    public override string GetDataTypeName(int ordinal) => value.GetType().Name;
    public override IEnumerator<object> GetEnumerator() => throw new NotSupportedException();
    public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
    public override byte GetByte(int ordinal) => throw new NotSupportedException();

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    public override char GetChar(int ordinal) => throw new NotSupportedException();

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
        throw new NotSupportedException();

    public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
    public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
    public override double GetDouble(int ordinal) => throw new NotSupportedException();
    public override float GetFloat(int ordinal) => throw new NotSupportedException();
    public override Guid GetGuid(int ordinal) => (Guid)value;
    public override short GetInt16(int ordinal) => throw new NotSupportedException();
    public override int GetInt32(int ordinal) => (int)value;
    public override long GetInt64(int ordinal) => (long)value;
    public override string GetString(int ordinal) => (string)value;
    public override int GetValues(object[] values) => throw new NotSupportedException();
}
