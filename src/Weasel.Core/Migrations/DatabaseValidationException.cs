namespace Weasel.Core.Migrations;

/// <summary>
///     Thrown when the actual database configuration does not match the expected configuration
/// </summary>
public class DatabaseValidationException: Exception
{
    public DatabaseValidationException(string ddl)
        : base("Configuration to Schema Validation Failed! These changes detected:\n\n" + ddl)
    {
    }

    public DatabaseValidationException(string databaseName, string ddl)
        : base($"Configuration to Schema Validation for Database '{databaseName}' Failed! These changes detected:\n\n" +
               ddl)
    {
    }

    public DatabaseValidationException(string databaseName, Exception inner)
        : base($"Configuration to Schema Validation for Database '{databaseName}' Failed!", inner)
    {
    }

    /// <summary>
    ///     Drift that cannot be applied incrementally (weasel#601). Still a validation failure --
    ///     the assert is a report, not an apply -- but the refusal that produced it is kept as the
    ///     inner exception.
    /// </summary>
    public DatabaseValidationException(string databaseName, string ddl, Exception inner)
        : base($"Configuration to Schema Validation for Database '{databaseName}' Failed! These changes detected:\n\n" +
               ddl, inner)
    {
    }

#if SERIALIZE
        protected DatabaseValidationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
#endif
}
