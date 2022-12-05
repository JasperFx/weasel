namespace Weasel.Core.Migrations;

/// <summary>
///     Base class for easier creation of custom IFeatureSchema objects
/// </summary>
public abstract class FeatureSchemaBase: IFeatureSchema
{
    protected FeatureSchemaBase(string identifier, Migrator migrator)
    {
        Identifier = identifier;
        Migrator = migrator;
    }

    public string Identifier { get; }
    public Migrator Migrator { get; }

    public virtual IEnumerable<Type> DependentTypes()
    {
        return Type.EmptyTypes;
    }

    public ISchemaObject[] Objects => schemaObjects().ToArray();

    public virtual Type StorageType => GetType();

    public virtual void WritePermissions(Migrator rules, TextWriter writer)
    {
        // Nothing
    }

    protected abstract IEnumerable<ISchemaObject> schemaObjects();
}
