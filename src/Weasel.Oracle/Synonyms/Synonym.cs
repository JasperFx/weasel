using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Oracle.Synonyms;

/// <summary>
///     An Oracle synonym, private or public.
/// </summary>
/// <remarks>
///     <para>
///         A public synonym belongs to no schema — it is visible to every user, and
///         <c>all_synonyms</c> reports its owner as <c>PUBLIC</c>. Set <see cref="IsPublic" /> and
///         the identifier's schema is ignored, because there is nowhere for it to go.
///     </para>
///     <para>
///         <c>CREATE OR REPLACE SYNONYM</c> exists, so a change is one statement — which is what
///         Oracle's migrator can execute per delta.
///     </para>
/// </remarks>
public class Synonym: SchemaObjectBase
{
    public Synonym(string name, string target)
        : this(DbObjectName.Parse(OracleProvider.Instance, name), target)
    {
    }

    public Synonym(DbObjectName identifier, string target): base(identifier)
    {
        Target = target ?? throw new ArgumentNullException(nameof(target));
    }

    /// <summary>The object this synonym stands for.</summary>
    public string Target { get; }

    /// <summary>
    ///     A public synonym, visible to every user and owned by no schema.
    /// </summary>
    public bool IsPublic { get; set; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var scope = IsPublic ? "PUBLIC " : string.Empty;
        writer.WriteLine($"CREATE OR REPLACE {scope}SYNONYM {QualifiedName()} FOR {Target}");
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        var scope = IsPublic ? "PUBLIC " : string.Empty;

        // No DROP SYNONYM IF EXISTS on Oracle. ORA-01434 is a private synonym that is not there,
        // ORA-01432 a public one.
        writer.WriteLine($@"BEGIN
    EXECUTE IMMEDIATE 'DROP {scope}SYNONYM {SchemaUtils.EscapeLiteral(QualifiedName())}';
EXCEPTION
    WHEN OTHERS THEN IF SQLCODE NOT IN (-1434, -1432) THEN RAISE; END IF;
END;");
    }

    private string QualifiedName()
        => IsPublic ? SchemaUtils.QuoteName(Identifier.Name) : Identifier.QualifiedName;

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var ownerParam = builder
            .AddParameter(IsPublic ? "PUBLIC" : Identifier.Schema.ToUpperInvariant()).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name.ToUpperInvariant()).ParameterName;

        builder.Append(
            "SELECT table_owner || '.' || table_name FROM all_synonyms "
            + $"WHERE owner = :{ownerParam} AND synonym_name = :{nameParam}");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readTargetAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SynonymDelta(this, SchemaPatchDifference.Create);
        }

        return Normalize(existing) == Normalize(Target)
            ? new SynonymDelta(this, SchemaPatchDifference.None)
            : new SynonymDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     Oracle folds an unquoted name to uppercase and always reports the target fully qualified,
    ///     so both sides are uppercased and a caller's unqualified target is qualified with this
    ///     synonym's own schema before comparing.
    /// </summary>
    internal string Normalize(string target)
    {
        var normalized = target.Replace("\"", "").Trim().ToUpperInvariant();

        return normalized.Contains('.')
            ? normalized
            : $"{Identifier.Schema.ToUpperInvariant()}.{normalized}";
    }

    public async Task<string?> FetchExistingTargetAsync(OracleConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var target = await readTargetAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return target;
    }

    public async Task<bool> ExistsInDatabaseAsync(OracleConnection conn, CancellationToken ct = default)
        => await FetchExistingTargetAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readTargetAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var target = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(target) ? null : target;
    }
}

/// <summary>
///     <c>CREATE OR REPLACE SYNONYM</c> does the whole job, so an update is that statement alone —
///     Oracle's migrator executes one statement per delta, and its drop is a PL/SQL block.
/// </summary>
internal class SynonymDelta: ISchemaObjectDelta
{
    private readonly Synonym _synonym;

    public SynonymDelta(Synonym synonym, SchemaPatchDifference difference)
    {
        _synonym = synonym;
        Difference = difference;
    }

    public ISchemaObject SchemaObject => _synonym;

    public SchemaPatchDifference Difference { get; }

    public void WriteUpdate(Migrator rules, TextWriter writer) => _synonym.WriteCreateStatement(rules, writer);

    public void WriteRollback(Migrator rules, TextWriter writer)
    {
    }

    public void WriteRestorationOfPreviousState(Migrator rules, TextWriter writer)
        => throw new NotSupportedException();
}
