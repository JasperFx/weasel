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
        get => StorageParameters[StorageParameterNames.FillFactor] is { } value
            ? Convert.ToInt32(value, CultureInfo.InvariantCulture)
            : null;
        set
        {
            if (value.HasValue)
            {
                StorageParameters[StorageParameterNames.FillFactor] = value.Value;
            }
            else
            {
                StorageParameters.Remove(StorageParameterNames.FillFactor);
            }
        }
    }

    /// <summary>
    ///     Set a storage parameter, replacing any value already declared for it. Prefer a name from
    ///     <see cref="StorageParameterNames" /> over a literal, which is what every overload below
    ///     does: <see cref="StorageParameters" /> has case-sensitive keys, so two spellings of one
    ///     parameter render as one duplicated setting and PostgreSQL rejects the DDL with 22023.
    /// </summary>
    /// <param name="name">The reloption name, lower case.</param>
    /// <param name="value">
    ///     The value, rendered with <see cref="CultureInfo.InvariantCulture" />. Null removes the
    ///     parameter, which is NOT the same as resetting it on an existing table -- an undeclared
    ///     parameter is left alone by the delta rather than reset, since a DBA may have set it.
    /// </param>
    public Table WithStorageParameter(string name, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var key = name.Trim().ToLowerInvariant();
        if (value is null)
        {
            StorageParameters.Remove(key);
        }
        else
        {
            StorageParameters[key] = value;
        }

        return this;
    }

    /// <summary>
    ///     Set this table's fill factor (10-100): how full PostgreSQL packs a page on insert.
    ///     Lowering it leaves room for HOT updates in place. Null removes the declaration.
    /// </summary>
    public Table WithFillFactor(int? fillFactor)
        => WithStorageParameter(StorageParameterNames.FillFactor, fillFactor);

    /// <summary>
    ///     Set this table's autovacuum thresholds. Every parameter is optional and only the ones
    ///     supplied are declared, so this can be called more than once to build the set up.
    /// </summary>
    /// <param name="enabled">
    ///     Whether autovacuum and autoanalyze run on this table at all. Note that turning it off
    ///     does not stop an anti-wraparound vacuum.
    /// </param>
    /// <param name="vacuumThreshold">Minimum dead tuples before a vacuum.</param>
    /// <param name="vacuumScaleFactor">Fraction of the table size added to the vacuum threshold.</param>
    /// <param name="analyzeThreshold">Minimum changed tuples before an analyze.</param>
    /// <param name="analyzeScaleFactor">Fraction of the table size added to the analyze threshold.</param>
    /// <param name="insertThreshold">Minimum inserted tuples before a vacuum. PostgreSQL 13+.</param>
    /// <param name="insertScaleFactor">
    ///     Fraction of the table size added to the insert vacuum threshold. PostgreSQL 13+.
    /// </param>
    public Table WithAutovacuum(
        bool? enabled = null,
        int? vacuumThreshold = null,
        double? vacuumScaleFactor = null,
        int? analyzeThreshold = null,
        double? analyzeScaleFactor = null,
        int? insertThreshold = null,
        double? insertScaleFactor = null)
    {
        // Only the arguments actually supplied are declared. A null here means "say nothing about
        // this one", not "reset it": the delta never resets a parameter the table does not declare.
        if (enabled.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumEnabled, enabled.Value);
        }

        if (vacuumThreshold.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumVacuumThreshold, vacuumThreshold.Value);
        }

        if (vacuumScaleFactor.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumVacuumScaleFactor, vacuumScaleFactor.Value);
        }

        if (analyzeThreshold.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumAnalyzeThreshold, analyzeThreshold.Value);
        }

        if (analyzeScaleFactor.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumAnalyzeScaleFactor, analyzeScaleFactor.Value);
        }

        if (insertThreshold.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumVacuumInsertThreshold, insertThreshold.Value);
        }

        if (insertScaleFactor.HasValue)
        {
            WithStorageParameter(StorageParameterNames.AutovacuumVacuumInsertScaleFactor, insertScaleFactor.Value);
        }

        return this;
    }

    /// <summary>
    ///     Set the number of parallel workers a parallel scan of this table may use, overriding the
    ///     estimate PostgreSQL makes from the table's size. Null removes the declaration.
    /// </summary>
    public Table WithParallelWorkers(int? workers)
        => WithStorageParameter(StorageParameterNames.ParallelWorkers, workers);

    /// <summary>
    ///     Log any autovacuum of this table that runs longer than <paramref name="duration" />.
    ///     <see cref="TimeSpan.Zero" /> logs every one; null removes the declaration. PostgreSQL
    ///     takes -1 for "none", which <see cref="Timeout.InfiniteTimeSpan" /> maps to.
    /// </summary>
    public Table WithAutovacuumLogging(TimeSpan? duration)
        => WithStorageParameter(StorageParameterNames.LogAutovacuumMinDuration,
            duration is null
                ? null
                : duration == Timeout.InfiniteTimeSpan
                    ? -1
                    : (int)duration.Value.TotalMilliseconds);

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

            if (list.Any(x => x.Item1 == name))
            {
                throw new InvalidOperationException(
                    $"Table '{Identifier}' declares the storage parameter '{name}' more than once. {nameof(StorageParameters)} keys are case sensitive but are normalized to lower case when written, so two spellings collapse into one duplicated setting that PostgreSQL rejects. Use the names on {nameof(StorageParameterNames)}.");
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
