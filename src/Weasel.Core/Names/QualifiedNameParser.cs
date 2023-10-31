namespace Weasel.Core.Names;

public static class QualifiedNameParser
{
    public static string[] Parse(IDatabaseProvider databaseProvider, string qualifiedName)
    {
        var parts = qualifiedName.Split('.');
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
