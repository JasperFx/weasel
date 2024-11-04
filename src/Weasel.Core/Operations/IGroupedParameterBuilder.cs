using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface IGroupedParameterBuilder<TParameter, TDbType>
{
    TParameter AppendParameter<T>(T? value) where T : notnull;
    TParameter AppendParameter<T>(T? value, TDbType dbType) where T : notnull;

    void AppendJsonParameter(ISerializer serializer, object value);

    void AppendTextParameter(string value);
}
