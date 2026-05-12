using System.Data.Common;

namespace Weasel.Core;

/// <summary>
///     Abstract base class for <see cref="ISchemaObject" /> implementations that share the
///     same boilerplate: a single qualified name as their identifier, a single result-set
///     "does this exist?" delta check, and the standard <c>FindDeltaAsync</c> pattern of
///     "build command → execute reader → produce delta → close reader".
///     <para>
///     Provider-specific subclasses (Sequence, Function, View, Extension, …) inherit the
///     boilerplate and only override the parts that truly differ per database:
///     <see cref="WriteCreateStatement" />, <see cref="WriteDropStatement" />,
///     <see cref="ConfigureQueryCommand" />, and optionally
///     <see cref="readExistingAsync" /> for objects that need richer delta semantics
///     than "exists / does not exist".
///     </para>
/// </summary>
public abstract class SchemaObjectBase: ISchemaObject
{
    protected SchemaObjectBase(DbObjectName identifier)
    {
        Identifier = identifier ?? throw new ArgumentNullException(nameof(identifier));
    }

    /// <inheritdoc />
    public DbObjectName Identifier { get; protected set; }

    /// <inheritdoc />
    public abstract void WriteCreateStatement(Migrator migrator, TextWriter writer);

    /// <inheritdoc />
    public abstract void WriteDropStatement(Migrator rules, TextWriter writer);

    /// <inheritdoc />
    public abstract void ConfigureQueryCommand(DbCommandBuilder builder);

    /// <inheritdoc />
    /// <remarks>
    ///     The default implementation handles the common "single COUNT(*) row" delta pattern:
    ///     if the reader yields zero or a count of 0, the object needs to be created; otherwise
    ///     it already exists and no migration is needed. Subclasses that need richer comparison
    ///     (e.g. function body diffing, view SQL diffing) should override; subclasses whose
    ///     <c>COUNT(*)</c> returns a different CLR type than <see cref="long" /> (e.g. Oracle
    ///     returns <see cref="decimal" />) should override only <see cref="ReadExistsCountAsync" />.
    /// </remarks>
    public virtual async Task<ISchemaObjectDelta> CreateDeltaAsync(
        DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false) ||
            await ReadExistsCountAsync(reader, ct).ConfigureAwait(false) == 0)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return new SchemaObjectDelta(this, SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Reads the first column of the current row as a count of matching rows in the catalog.
    ///     The CLR type returned by <c>COUNT(*)</c> varies by provider:
    ///     SQL Server returns <see cref="int" />, MySQL/Npgsql return <see cref="long" />, and
    ///     Oracle returns <see cref="decimal" />. The default reads the raw value and converts
    ///     via <see cref="Convert.ToInt64(object)" />, which handles all three. Subclasses
    ///     normally have no reason to override.
    /// </summary>
    protected virtual async Task<long> ReadExistsCountAsync(
        DbDataReader reader, CancellationToken ct)
    {
        var raw = await reader.GetFieldValueAsync<object>(0, ct).ConfigureAwait(false);
        return Convert.ToInt64(raw);
    }

    /// <inheritdoc />
    /// <remarks>
    ///     The vast majority of <see cref="ISchemaObject" /> implementations expose exactly
    ///     one named database object — their own <see cref="Identifier" />. Subclasses that
    ///     create additional named artifacts (a table that creates indexes, for example)
    ///     should override.
    /// </remarks>
    public virtual IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    /// <summary>
    ///     Standard "find delta against an open <see cref="DbConnection" />" pattern:
    ///     build a query command, execute it, ask <see cref="CreateDeltaAsync" /> to produce
    ///     a delta from the reader, close the reader, return the delta. Provider-specific
    ///     subclasses can expose a strongly-typed overload (e.g.
    ///     <c>FindDeltaAsync(NpgsqlConnection, CancellationToken)</c>) that simply forwards
    ///     to this method.
    /// </summary>
    public async Task<ISchemaObjectDelta> FindDeltaAsync(
        DbConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var result = await CreateDeltaAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }
}
