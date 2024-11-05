using System.Data;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IGroupedParameterBuilder
{
    void AppendParameter<T>(T? value) where T : notnull;

    void AppendParameter(object value, DbType dbType);

    void AppendStringArrayParameter(string[] values);
    void AppendGuidArrayParameter(Guid[] values);
    void AppendJsonParameter(ISerializer serializer, object value);
    void AppendJsonArrayParameter(ISerializer serializer, object[] value);

    void AppendTextParameter(string value);

    void AppendNull(DbType dbType);

    void AppendEnumAsString<T>(T? value) where T : struct;
    void AppendEnumAsInteger<T>(T? value) where T : struct;

}
