using System.Runtime.Serialization;

namespace Weasel.Postgresql;

public class PostgresqlIdentifierInvalidException: Exception
{
    public PostgresqlIdentifierInvalidException(string name)
        : base(MessageFor(name, null))
    {
        Name = name;
    }

    /// <summary>
    ///     Names the rule the identifier broke, so the message says more than "not valid" (weasel#416).
    /// </summary>
    public PostgresqlIdentifierInvalidException(string name, string reason)
        : base(MessageFor(name, reason))
    {
        Name = name;
        Reason = reason;
    }

    public string Name { get; set; }

    /// <summary>
    ///     Why the identifier was rejected, when the thrower said. Null for the legacy constructor.
    /// </summary>
    public string? Reason { get; }

    private static string MessageFor(string name, string? reason)
    {
        var because = reason is null ? "" : $" because {reason}";

        return
            $"Database identifier {name} is not valid{because}. See https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html for valid unquoted identifiers (Weasel does not quote identifiers).";
    }
}
