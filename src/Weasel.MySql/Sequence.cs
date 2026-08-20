using Weasel.Core;

namespace Weasel.MySql;

/// <summary>
///     A MySQL "sequence", emulated as a single-row auto-increment table. Obsolete: nothing can
///     consume it.
/// </summary>
/// <remarks>
///     <para>
///         MySQL has never had <c>CREATE SEQUENCE</c> — the comment that used to say 5.x lacked it
///         and 8.0 added it was wrong; it is MariaDB 10.3 that has them. So this was always an
///         emulation, and an incomplete one: <see cref="WriteCreateStatement" /> creates a table
///         with a <c>current_value</c> column that is never read and never incremented, there is no
///         next-value operation anywhere in <c>Weasel.MySql</c> or on <see cref="SequenceBase" />,
///         and <see cref="ConfigureQueryCommand" /> only checks existence, so a change to
///         <see cref="SequenceBase.StartWith" /> is never detected.
///     </para>
///     <para>
///         The general rule this follows (weasel#453): emulate <em>operations</em> an engine lacks
///         freely — SQLite's table recreation around its <c>ALTER</c> limits, MySQL's DDL ordering
///         around foreign-key backing indexes — because the end state is the object the caller
///         declared and the emulation is invisible. Do not emulate <em>objects</em>: an emulated
///         object makes <c>AllObjects()</c> and introspection lie, and the semantics diverge where
///         it matters. A real sequence does not roll back and does not serialize writers; a
///         table-backed counter does both.
///     </para>
///     <para>
///         Use <c>AUTO_INCREMENT</c> on the column that needs generated values.
///     </para>
/// </remarks>
[Obsolete(
    "MySQL has no sequences and this emulation cannot be consumed - nothing reads or increments "
    + "current_value. Use AUTO_INCREMENT on the column instead. See weasel#453.")]
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
    ///     Always 1. Setting anything else throws.
    /// </summary>
    /// <remarks>
    ///     This used to be settable and documented as "reserved for future use" while being
    ///     silently ignored, which left a caller with a sequence stepping by one when they asked for
    ///     ten and nothing to notice it by. Refusing is the same rule weasel#449 applied to the
    ///     index predicates that did nothing: a caller gets what they set, or an exception.
    /// </remarks>
    public long IncrementBy
    {
        get => 1;
        set
        {
            if (value != 1)
            {
                throw new NotSupportedException(
                    "MySQL has no sequences, and this table-based emulation does not honour an "
                    + "increment other than 1. Use AUTO_INCREMENT, whose step is set server-wide by "
                    + "auto_increment_increment.");
            }
        }
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        // MySQL has no CREATE SEQUENCE at any version, so this is a table.
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
