using System.Data.Common;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface ICommandBuilder<TParameter, TDbType> : ICommandBatchBuilder
    where TParameter : DbParameter
    where TDbType : struct
{
    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    string TenantId { get; set; }

    /// <summary>
    /// Preview the parameter name of the last appended parameter
    /// </summary>
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);

    TParameter AppendParameter<T>(T value);
    TParameter AppendParameter<T>(T value, TDbType? dbType);
    TParameter AppendParameter(object value);

    void AppendParameters(params object[] parameters);

    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? separator = null);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    TParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    TParameter[] AppendWithParameters(string text, char placeholder);


    /// <summary>
    /// Use an anonymous type to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(object parameters);

    void AppendStringArrayParameter(string[] values);
    void AppendGuidArrayParameter(Guid[] values);
    void AppendLongArrayParameter(long[] values);
    void AppendJsonParameter(ISerializer serializer, object value);
    void AppendJsonParameter(string json);
}
