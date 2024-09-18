using System.Data.Common;

namespace Weasel.Core.Migrations;

public interface IDatabaseInitializer<T> where T : DbConnection
{
    Task InitializeAsync(T connection, CancellationToken token);
}

/// <summary>
///     Defines a distinct part of your database. Can represent relationships
///     between groups as well
/// </summary>
public interface IFeatureSchema
{
    ISchemaObject[] Objects { get; }

    /// <summary>
    ///     Really just the filename when the SQL is exported
    /// </summary>
    string Identifier { get; }

    Migrator Migrator { get; }

    /// <summary>
    ///     Identifier by type for this feature. Used along with the DependentTypes()
    ///     collection to control the proper ordering of object creation or scripting
    /// </summary>
    Type StorageType { get; }

    /// <summary>
    ///     Write any permission SQL when this feature is exported to a SQL
    ///     file
    /// </summary>
    /// <param name="rules"></param>
    /// <param name="writer"></param>
    void WritePermissions(Migrator rules, TextWriter writer);

    /// <summary>
    ///     Any document or feature types that this feature depends on. Used
    ///     to intelligently order the creation and scripting of database
    ///     schema objects
    /// </summary>
    /// <returns></returns>
    IEnumerable<Type> DependentTypes();
}
