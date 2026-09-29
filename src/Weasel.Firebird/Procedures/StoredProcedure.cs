using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird.Procedures;

/// <summary>
///     A Firebird stored procedure, executable or selectable, given as its whole statement:
///     <c>CREATE PROCEDURE name (…) RETURNS (…) AS BEGIN … END</c>.
/// </summary>
/// <remarks>
///     <para>
///         Firebird keeps the body as source and records the rest of the header in
///         <c>RDB$PROCEDURE_PARAMETERS</c>, so a delta compares the whole definition: every input and
///         output parameter's name, type, <c>NOT NULL</c>, collation and default, <c>SQL SECURITY</c>
///         (Firebird 4 and later), and the body. A type written with a synonym matches the type it is
///         stored as, and a character set, collation or precision the statement leaves out is not
///         compared.
///     </para>
///     <para>
///         Whatever verb the statement is written with, it is run as <c>CREATE OR ALTER PROCEDURE</c>,
///         which creates the procedure or changes it in place, callers and grants intact.
///     </para>
///     <para>
///         A procedure inside a package, and an external (UDR) procedure, are different objects and are
///         not this.
///     </para>
/// </remarks>
public class StoredProcedure: StoredProcedureBase
{
    private PsqlRoutine? _parsed;

    public StoredProcedure(string name, string body)
        : this(FirebirdProvider.Instance.Parse(name), body)
    {
    }

    public StoredProcedure(DbObjectName identifier, string body)
        : base(FirebirdObjectName.From(identifier ?? throw new ArgumentNullException(nameof(identifier))), body)
    {
    }

    /// <summary>
    ///     The procedure's name as Firebird's catalog stores it.
    /// </summary>
    internal string CatalogName => SchemaUtils.CatalogName(Identifier.Name);

    /// <summary>
    ///     The statement, parsed. Refuses a statement that is not a PSQL procedure, or that names a
    ///     different procedure from this one's identifier -- a mismatch that would otherwise create one
    ///     procedure and look for another, and report it missing forever.
    /// </summary>
    internal PsqlRoutine Parsed => _parsed ??= parse(null);

    private PsqlRoutine parse(FirebirdServerVersion? version)
    {
        var routine = PsqlRoutine.Parse(BodyText(), PsqlRoutineKind.Procedure, version);

        if (routine.CatalogName != CatalogName)
        {
            throw new ArgumentException(
                $"Stored procedure {Identifier.Name} is stored as {CatalogName}, but its statement creates {routine.CatalogName}. "
                + "Firebird folds an undelimited name to upper case and keeps a delimited one as written, so name "
                + "the procedure the same way in both places.");
        }

        return routine;
    }

    /// <summary>
    ///     <c>CREATE OR ALTER PROCEDURE</c>, as a PSQL statement.
    /// </summary>
    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        FirebirdObjectName.AssertDefaultSchema(Identifier.Schema, $"stored procedure {Identifier.Name}");
        FirebirdScript.WritePsql(writer, Parsed.Statement);
    }

    /// <summary>
    ///     A <c>DROP PROCEDURE</c> that runs only while the procedure exists. Firebird before 6 has no
    ///     <c>DROP PROCEDURE IF EXISTS</c>.
    /// </summary>
    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        FirebirdScript.WriteGuardedWhenExists(writer, CatalogProbes.Procedure(CatalogName),
            $"DROP PROCEDURE {SchemaUtils.QuoteName(Identifier.Name)}");
    }

    /// <summary>
    ///     Firebird executes one statement per command, so introspection goes through
    ///     <see cref="FirebirdDbCommandBuilder" />.
    /// </summary>
    protected override DbCommandBuilder CreateCommandBuilder(DbConnection conn)
        => conn is FbConnection firebird ? new FirebirdDbCommandBuilder(firebird) : base.CreateCommandBuilder(conn);

    /// <summary>
    ///     One query: a row per parameter, inputs then outputs, each carrying the procedure's source and
    ///     options. Unterminated, because Firebird runs one statement per command.
    /// </summary>
    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var name = builder.AddParameter(CatalogName).ParameterName;
        builder.Append(PsqlRoutine.ProcedureQuery("@" + name, PsqlRoutine.ReadsSqlSecurity(builder)));
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await PsqlRoutine.ReadAsync(reader, PsqlRoutineKind.Procedure, CatalogName, ct)
            .ConfigureAwait(false);
        var actual = existing == null ? null : new StoredProcedure(Identifier, existing.Statement);

        if (IsRemoved)
        {
            return new CreateOrAlterDelta(this, actual,
                existing == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update, isRemoved: true);
        }

        if (existing == null)
        {
            return new CreateOrAlterDelta(this, null, SchemaPatchDifference.Create);
        }

        var differences = parse(existing.Version).DifferencesFrom(existing);

        return new CreateOrAlterDelta(this, actual,
            differences.Count == 0 ? SchemaPatchDifference.None : SchemaPatchDifference.Update,
            differences: differences);
    }

    /// <summary>
    ///     The procedure as the database has it, rebuilt into a statement that would create it, or null
    ///     when there is no procedure of this name.
    /// </summary>
    public async Task<StoredProcedure?> FetchExistingAsync(FbConnection conn, CancellationToken ct = default)
    {
        var builder = new FirebirdDbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var existing = await PsqlRoutine.ReadAsync(reader, PsqlRoutineKind.Procedure, CatalogName, ct)
            .ConfigureAwait(false);

        return existing == null ? null : new StoredProcedure(Identifier, existing.Statement);
    }

    public async Task<bool> ExistsInDatabaseAsync(FbConnection conn, CancellationToken ct = default)
        => await FetchExistingAsync(conn, ct).ConfigureAwait(false) != null;
}
