using System.Data;
using System.Data.Common;
using Weasel.Core.Serialization;

namespace Weasel.Core.Operations;

public interface ICommandBuilder : ICommandBatchBuilder
{
    /// <summary>
    /// It became so common, that it's turned out to be convenient to place
    /// this here
    /// </summary>
    string TenantId { get; set; }

    void SetParameterAsJson(DbParameter parameter, string json);

    /// <summary>
    /// Preview the parameter name of the last appended parameter
    /// </summary>
    string? LastParameterName { get; }

    void Append(string sql);
    void Append(char character);

    void AppendParameter<T>(T value);
    void AppendParameter<T>(T value, DbType? dbType);

    void AppendParameter(object value);

    void AppendParameters(params object[] parameters);

    IGroupedParameterBuilder CreateGroupedParameterBuilder(char? separator = null);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    DbParameter[] AppendWithParameters(string text);

    /// <summary>
    ///     Append a SQL string with user defined placeholder characters for new parameters, and returns an
    ///     array of the newly created parameters
    /// </summary>
    /// <param name="text"></param>
    /// <param name="separator"></param>
    /// <returns></returns>
    DbParameter[] AppendWithParameters(string text, char placeholder);


    /// <summary>
    ///     Use an anonymous type to add named parameters.
    ///     If a dictionary is passed in then its key-value pairs will be used as named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(object parameters);

    /// <summary>
    ///     Use a dictionary to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters(IDictionary<string, object?> parameters);

    /// <summary>
    ///     Use a dictionary to add named parameters
    /// </summary>
    /// <param name="parameters"></param>
    void AddParameters<T>(IDictionary<string, T> parameters);

    void AppendStringArrayParameter(string[] values);
    void AppendGuidArrayParameter(Guid[] values);
    void AppendLongArrayParameter(long[] values);
    void AppendJsonParameter(ISerializer serializer, object value);
    void AppendJsonParameter(string json);
}
