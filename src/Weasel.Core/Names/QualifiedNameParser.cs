namespace Weasel.Core.Names;

public static class QualifiedNameParser
{
    /// <remarks>
    ///     Split through the dialect's identifier rules where they are available, because a dot inside
    ///     a delimited identifier is an ordinary character and not a separator -- <c>"my.schema".things</c>
    ///     is two parts, and splitting on every dot made such a name unparseable (weasel#501). A
    ///     provider that supplies no rules falls back to the plain split it always had.
    /// </remarks>
    public static string[] Parse(IDatabaseProvider databaseProvider, string qualifiedName)
    {
        var parts = databaseProvider.Rules?.SplitQualifiedName(qualifiedName) ?? qualifiedName.Split('.');
        if (parts.Length == 1)
        {
            return new[] { databaseProvider.DefaultDatabaseSchemaName, qualifiedName };
        }

        if (parts.Length != 2)
        {
            throw new InvalidOperationException(
                $"Could not parse QualifiedName: '{qualifiedName}'. Number or parts should be 2s but is {parts.Length}");
        }

        return parts;
    }
}
