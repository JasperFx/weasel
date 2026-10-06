using System.Collections;
using System.Collections.Specialized;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Weasel.Core;

namespace Weasel.Postgresql.Tables;

public partial class Table
{
    /// <summary>
    ///     Table level storage parameters (<c>CREATE TABLE ... WITH (name = value, ...)</c>), keyed by the lower
    ///     case PostgreSQL reloption name, for example <c>fillfactor</c> or <c>autovacuum_vacuum_scale_factor</c>.
    ///     Same shape as <see cref="IndexDefinition.StorageParameters" />. <c>toast.*</c> parameters are not
    ///     supported and are rejected when the table's SQL is written or compared.
    /// </summary>
    /// <remarks>
    ///     Only the parameters declared here take part in a migration: a parameter that exists in the database
    ///     but is not declared is never reset, because someone else may have set it. For a partitioned table
    ///     PostgreSQL rejects storage parameters on the parent, so they are written on, and compared against,
    ///     each partition instead.
    /// </remarks>
    public OrderedDictionary StorageParameters { get; set; } = new();

    /// <summary>
    ///     Set a non-default fill factor on this table. Shorthand for <c>StorageParameters["fillfactor"]</c>;
    ///     assigning null removes the parameter.
    /// </summary>
    public int? FillFactor
    {
        get => StorageParameters["fillfactor"] is { } value
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : null;
        set
        {
            if (value.HasValue)
            {
                StorageParameters["fillfactor"] = value.Value;
            }
            else
            {
                StorageParameters.Remove("fillfactor");
            }
        }
    }

    /// <summary>
    ///     The storage parameters of each existing partition, read from the database. Keyed by the
    ///     partition's qualified name, only populated on a table read back by FetchExisting.
    /// </summary>
    internal Dictionary<DbObjectName, OrderedDictionary> PartitionStorageParameters { get; } = new();

    private static readonly Regex _parameterName = new("^[a-z_][a-z0-9_]*$", RegexOptions.Compiled);
    private static readonly Regex _bareValue = new("^[A-Za-z0-9_.+-]+$", RegexOptions.Compiled);

    internal bool HasStorageParameters => StorageParameters.Count > 0;

    /// <summary>
    ///     The declared storage parameters as name/value text, validated.
    /// </summary>
    internal (string Name, string Value)[] DeclaredStorageParameters()
    {
        var list = new List<(string, string)>();
        foreach (DictionaryEntry entry in StorageParameters)
        {
            var name = Convert.ToString(entry.Key, CultureInfo.InvariantCulture)!.Trim().ToLowerInvariant();
            if (name.StartsWith("toast.", StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Table '{Identifier}' declares the storage parameter '{name}'. toast.* storage parameters are not supported because they live on the TOAST relation, not on the table.");
            }

            if (!_parameterName.IsMatch(name))
            {
                throw new InvalidOperationException(
                    $"Table '{Identifier}' declares an invalid storage parameter name '{name}'.");
            }

            var value = Convert.ToString(entry.Value, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException(
                    $"Table '{Identifier}' declares the storage parameter '{name}' without a value.");
            }

            list.Add((name, value.Trim()));
        }

        return list.ToArray();
    }

    /// <summary>
    ///     <c>name = value, name = value</c>, with the values quoted only when they are not a bare token
    /// </summary>
    internal static string RenderStorageParameters(IEnumerable<(string Name, string Value)> parameters)
        => string.Join(", ", parameters.Select(x => $"{x.Name} = {RenderStorageValue(x.Value)}"));

    private static string RenderStorageValue(string value)
        => _bareValue.IsMatch(value) ? value : "'" + value.Replace("'", "''") + "'";

    /// <summary>
    ///     <c> WITH (...)</c> for a CREATE TABLE statement, or an empty string when nothing is declared
    /// </summary>
    internal string StorageParametersClause()
        => HasStorageParameters ? $" WITH ({RenderStorageParameters(DeclaredStorageParameters())})" : string.Empty;

    /// <summary>
    ///     Compares two storage parameter values: case-insensitively, and numerically when both are numbers
    ///     (PostgreSQL stores the text as written, so 0.05 and 0.050 are the same setting).
    /// </summary>
    internal static bool StorageValuesEqual(string expected, string actual)
    {
        if (string.Equals(expected.Trim(), actual.Trim(), StringComparison.OrdinalIgnoreCase)) return true;

        return decimal.TryParse(expected, NumberStyles.Float, CultureInfo.InvariantCulture, out var e)
               && decimal.TryParse(actual, NumberStyles.Float, CultureInfo.InvariantCulture, out var a)
               && e == a;
    }

    internal static OrderedDictionary ParseReloptions(string[]? reloptions)
    {
        var dict = new OrderedDictionary();
        if (reloptions == null) return dict;

        foreach (var option in reloptions)
        {
            var index = option.IndexOf('=');
            if (index <= 0) continue;
            dict[option[..index].Trim().ToLowerInvariant()] = option[(index + 1)..].Trim();
        }

        return dict;
    }

    internal static string RenderAlterSet(DbObjectName target, IEnumerable<(string Name, string Value)> parameters)
        => $"ALTER TABLE {target} SET ({RenderStorageParameters(parameters)});";

    internal static string RenderAlterReset(DbObjectName target, IEnumerable<string> names)
        => $"ALTER TABLE {target} RESET ({string.Join(", ", names)});";
}
