using System.Data.Common;
using System.Text.RegularExpressions;
using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     Cross-provider base for a stored procedure, alongside <see cref="FunctionBase" /> and
///     <see cref="ViewBase" />.
/// </summary>
/// <remarks>
///     <para>
///         Lifted from <c>Weasel.SqlServer.Procedures.StoredProcedure</c>, which implemented
///         <see cref="ISchemaObject" /> directly and so gave a second provider nothing to reuse
///         (weasel#451). The base came first, with the working implementation refitted onto it, so
///         the abstraction was validated against something real before three more providers
///         depended on it.
///     </para>
///     <para>
///         The body is the whole <c>CREATE PROCEDURE …</c> statement rather than just the
///         procedure's contents. That is how <see cref="FunctionBase" /> already works, and it is
///         what lets a caller write the parameter list, the language clause and the option flags
///         their engine supports without Weasel modelling any of it.
///     </para>
/// </remarks>
public abstract class StoredProcedureBase: SchemaObjectBase
{
    protected StoredProcedureBase(DbObjectName identifier) : base(identifier)
    {
    }

    protected StoredProcedureBase(DbObjectName identifier, string body) : base(identifier)
    {
        RawBody = body;
    }

    /// <summary>
    ///     The statement the procedure was constructed with, or <c>null</c> when a subclass
    ///     generates it instead through <see cref="GenerateBody" />.
    /// </summary>
    protected string? RawBody { get; }

    /// <summary>
    ///     True when this procedure has been marked for removal. A removed procedure emits no
    ///     create statement, and its delta reports the drop.
    /// </summary>
    public bool IsRemoved { get; set; }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (IsRemoved)
        {
            return;
        }

        writer.WriteLine(BodyText());
    }

    /// <summary>
    ///     The create statement as text, whether it came from the constructor or from
    ///     <see cref="GenerateBody" />.
    /// </summary>
    public string BodyText()
    {
        if (RawBody.IsNotEmpty())
        {
            return RawBody!;
        }

        var writer = new StringWriter();
        GenerateBody(writer);
        return writer.ToString();
    }

    /// <summary>
    ///     Subclasses that build their statement rather than being handed one override this.
    ///     Throwing is the right default: a procedure with neither a body nor a generator has
    ///     nothing to create.
    /// </summary>
    protected virtual void GenerateBody(TextWriter writer)
        => throw new NotSupportedException(
            $"Stored procedure {Identifier} was constructed without a body, so a subclass has to "
            + $"override {nameof(GenerateBody)} to supply one.");

    /// <summary>
    ///     Normalize the statement for comparison against what the catalog stores: trim every line,
    ///     drop the blank ones, and collapse runs of spaces. Every engine reformats what it keeps to
    ///     some degree, and none of those differences change what the procedure does.
    /// </summary>
    public string CanonicizeSql() => Canonicize(BodyText());

    public static string Canonicize(string sql)
        => sql.ReadLines()
            .Select(x => Whitespace.Replace(x, " ").Trim())
            .Where(x => x.IsNotEmpty())
            .Join(Environment.NewLine);

    /// <summary>
    ///     Any run of whitespace collapses to one space. Oracle stores <c>PROCEDURE\t name</c> with
    ///     a tab where the caller wrote a space, and none of these engines treat the difference as
    ///     meaningful.
    /// </summary>
    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.Compiled);

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(
        DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await ReadExistingAsync(reader, ct).ConfigureAwait(false);
        return CreateDelta(existing);
    }

    /// <summary>
    ///     Read the procedure's stored statement out of the catalog query, or <c>null</c> when it
    ///     does not exist.
    /// </summary>
    protected virtual async Task<string?> ReadExistingAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var body = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return body.IsEmpty() ? null : body;
    }

    /// <summary>
    ///     Compare what the catalog holds against what this procedure would create. Subclasses
    ///     override when the engine rewrites the statement enough that a canonicalized comparison
    ///     needs help.
    /// </summary>
    protected virtual ISchemaObjectDelta CreateDelta(string? existing)
    {
        if (IsRemoved)
        {
            return new SchemaObjectDelta(this,
                existing == null ? SchemaPatchDifference.None : SchemaPatchDifference.Update);
        }

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return Matches(existing)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    ///     Whether the catalog's rendering is this same procedure. Case-insensitive, because SQL
    ///     keywords are.
    /// </summary>
    protected virtual bool Matches(string existing)
        => Canonicize(existing).Equals(CanonicizeSql(), StringComparison.OrdinalIgnoreCase);
}
