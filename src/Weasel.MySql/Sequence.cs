using Weasel.Core;

namespace Weasel.MySql;

/// <summary>
///     MySQL sequence. Because MySQL 5.x doesn't have native sequences, this is emulated as
///     a single-row auto-increment table — the underlying database object is therefore a
///     TABLE, not a SEQUENCE. The fluent surface still mirrors the other providers'
///     <see cref="SequenceBase" /> for portability of caller code.
/// </summary>
public class Sequence: SequenceBase
{
    public Sequence(DbObjectName identifier) : base(identifier, startWith: 1)
    {
    }

    public Sequence(string sequenceName)
        : this(DbObjectName.Parse(MySqlProvider.Instance, sequenceName))
    {
    }

    /// <summary>
    ///     Reserved for future use — MySQL's table-based emulation doesn't currently honor
    ///     a non-1 increment, but the property is here for fluent-API parity and may be
    ///     wired into the emulation later.
    /// </summary>
    public long IncrementBy { get; set; } = 1;

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // MySQL 8.0+ supports sequences, but older versions don't.
        // Using a table-based sequence for broader compatibility.
        var seed = StartWith ?? 1;
        writer.WriteLine($"CREATE TABLE IF NOT EXISTS {Identifier.QualifiedName} (");
        writer.WriteLine("    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
        writer.WriteLine($"    current_value BIGINT NOT NULL DEFAULT {seed}");
        writer.WriteLine(");");
        writer.WriteLine($"INSERT IGNORE INTO {Identifier.QualifiedName} (current_value) VALUES ({seed});");
    }

    public override void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"DROP TABLE IF EXISTS {Identifier.QualifiedName};");
    }

    public override void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append($@"
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = @{schemaParam} AND table_name = @{nameParam};
");
    }

    // No CreateDeltaAsync override needed — base class handles COUNT(*) -> long natively.
}
