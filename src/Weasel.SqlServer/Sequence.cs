using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer;

public class Sequence: SequenceBase
{
    public Sequence(string identifier)
        : base(DbObjectName.Parse(SqlServerProvider.Instance, identifier))
    {
    }

    public Sequence(DbObjectName identifier) : base(identifier)
    {
    }

    public Sequence(DbObjectName identifier, long startWith) : base(identifier, startWith)
    {
    }

    /// <summary>
    ///     Guarded by an <c>OBJECT_ID</c> existence check so a rendered migration script can be run
    ///     more than once (weasel#593). <c>CREATE SEQUENCE</c> has no <c>OR ALTER</c> and no
    ///     <c>IF NOT EXISTS</c> of its own, and unguarded it raises "There is already an object
    ///     named ... in the database" on the second run, which aborts the whole script, including
    ///     the objects after it.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var startsWith = StartWith ?? 1;

        writer.WriteLine($"IF OBJECT_ID(N'{SchemaUtils.EscapeLiteral(Identifier.QualifiedName)}', N'SO') IS NULL");
        writer.WriteLine(
            $"CREATE SEQUENCE {Identifier} START WITH {startsWith}{(IncrementBy.HasValue ? $" INCREMENT BY {IncrementBy.Value}" : string.Empty)};");

        if (Owner != null)
        {
            writer.WriteLine($"ALTER SEQUENCE {Identifier} OWNED BY {Owner}.{OwnerColumn};");
        }
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP SEQUENCE IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        builder.Append(
            $"select count(*) from sys.sequences inner join sys.schemas on sys.sequences.schema_id = sys.schemas.schema_id where sys.schemas.name = @{schemaParam} and sys.sequences.name = @{nameParam};");
    }

    /// <summary>
    ///     Provider-specific overload that accepts a <see cref="SqlConnection" /> for caller
    ///     convenience. Forwards to the base
    ///     <see cref="SchemaObjectBase.FindDeltaAsync(System.Data.Common.DbConnection, CancellationToken)" />.
    /// </summary>
    public Task<ISchemaObjectDelta> FindDeltaAsync(SqlConnection conn, CancellationToken ct = default)
        => FindDeltaAsync((System.Data.Common.DbConnection)conn, ct);
}
