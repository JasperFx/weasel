using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     Thrown when a migration statement or an introspection query is refused by the database
///     because the connection's role lacks the privilege to run it, rather than because anything
///     is wrong with the DDL itself (weasel#598).
/// </summary>
/// <remarks>
///     <para>
///     This is, from support traffic, the most common real-world migration failure, and before
///     this type the only signal was a SQLSTATE buried in an inner exception -- <c>42501</c> from
///     PostgreSQL, <c>262</c> / <c>229</c> / <c>297</c> from SQL Server, <c>1142</c> / <c>1044</c>
///     from MySQL, <c>ORA-01031</c> from Oracle -- next to a statement the migration logger had
///     already printed. The provider's own exception is always kept as
///     <see cref="Exception.InnerException" />; nothing is hidden, only named.
///     </para>
///     <para>
///     The message names the two remedies deliberately, because which one applies is a deployment
///     decision rather than a code one: grant the role what it needs, or pre-provision the schema
///     from <c>db-patch</c> output as a privileged user and run the application with
///     <see cref="JasperFx.AutoCreate.None" />.
///     </para>
/// </remarks>
public class InsufficientDatabasePrivilegeException: Exception
{
    /// <summary>
    ///     Build the message. Exposed so providers and tests can assert on the exact text without
    ///     reconstructing it.
    /// </summary>
    public static string ToMessage(string? role, string? database, string? statement, string providerMessage)
    {
        var who = role.IsEmpty()
            ? "The connection's role"
            : $"The role '{role}'";

        var where = database.IsEmpty() ? "" : $" in database '{database}'";

        var message =
            $"{who} does not have permission to apply this schema change{where}. The database said: {providerMessage}";

        if (statement.IsNotEmpty())
        {
            message += $"{Environment.NewLine}Statement:{Environment.NewLine}{statement}";
        }

        message +=
            $"{Environment.NewLine}Either grant the role the privileges it needs (CREATE on the schema, or ownership of the objects being altered), or pre-provision the schema by running the output of db-patch as a privileged user and running this application with AutoCreate.None.";

        return message;
    }

    public InsufficientDatabasePrivilegeException(
        string? role,
        string? database,
        string? statement,
        Exception innerException)
        : base(ToMessage(role, database, statement, innerException.Message), innerException)
    {
        Role = role;
        Database = database;
        Statement = statement;
    }

    /// <summary>
    ///     The login the refused connection was using, when the provider or the connection string
    ///     makes it available. Null under integrated / managed-identity authentication.
    /// </summary>
    public string? Role { get; }

    /// <summary>
    ///     The database the refused statement ran against, when the connection reports one.
    /// </summary>
    public string? Database { get; }

    /// <summary>
    ///     The statement that was refused, exactly as it was sent.
    /// </summary>
    public string? Statement { get; }
}
