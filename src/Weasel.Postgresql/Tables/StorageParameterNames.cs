namespace Weasel.Postgresql.Tables;

/// <summary>
///     The PostgreSQL table storage parameter (<c>reloption</c>) names, so a caller setting one
///     through <see cref="Table.StorageParameters" /> does not have to spell it.
/// </summary>
/// <remarks>
///     <para>
///     Every name here is lower case, which is not cosmetic. <see cref="Table.StorageParameters" />
///     is a <see cref="System.Collections.Specialized.OrderedDictionary" /> with the default
///     comparer, so its keys are case-<em>sensitive</em>, while the DDL writer and the catalog
///     reader both normalize to lower case. Two differently-cased spellings of one parameter are
///     therefore two entries that render as one duplicated setting, which PostgreSQL rejects with
///     22023. Using these constants is what keeps that from being possible.
///     </para>
///     <para>
///     The list is the server's table-level set, not everything <c>CREATE TABLE ... WITH</c> will
///     parse: <c>toast.*</c> parameters are deliberately absent because they live on the TOAST
///     relation's own <c>reloptions</c> rather than the table's, and
///     <see cref="Table.DeclaredStorageParameters" /> refuses them.
///     </para>
/// </remarks>
public static class StorageParameterNames
{
    /// <summary>
    ///     How full PostgreSQL packs a page on insert, 10-100. Lowering it leaves room for
    ///     <c>HOT</c> updates in place; the server's default is 100 for a table.
    /// </summary>
    public const string FillFactor = "fillfactor";

    /// <summary>
    ///     Whether autovacuum and autoanalyze run on this table at all. Turning it off does not
    ///     stop an anti-wraparound vacuum.
    /// </summary>
    public const string AutovacuumEnabled = "autovacuum_enabled";

    /// <summary>Minimum number of dead tuples before autovacuum runs.</summary>
    public const string AutovacuumVacuumThreshold = "autovacuum_vacuum_threshold";

    /// <summary>Fraction of the table size added to the vacuum threshold.</summary>
    public const string AutovacuumVacuumScaleFactor = "autovacuum_vacuum_scale_factor";

    /// <summary>Minimum number of inserted tuples before autovacuum runs. PostgreSQL 13+.</summary>
    public const string AutovacuumVacuumInsertThreshold = "autovacuum_vacuum_insert_threshold";

    /// <summary>Fraction of the table size added to the insert vacuum threshold. PostgreSQL 13+.</summary>
    public const string AutovacuumVacuumInsertScaleFactor = "autovacuum_vacuum_insert_scale_factor";

    /// <summary>Minimum number of changed tuples before autoanalyze runs.</summary>
    public const string AutovacuumAnalyzeThreshold = "autovacuum_analyze_threshold";

    /// <summary>Fraction of the table size added to the analyze threshold.</summary>
    public const string AutovacuumAnalyzeScaleFactor = "autovacuum_analyze_scale_factor";

    /// <summary>Cost delay, in milliseconds, for autovacuum on this table.</summary>
    public const string AutovacuumVacuumCostDelay = "autovacuum_vacuum_cost_delay";

    /// <summary>Cost limit for autovacuum on this table.</summary>
    public const string AutovacuumVacuumCostLimit = "autovacuum_vacuum_cost_limit";

    /// <summary>Age at which autovacuum forces a vacuum to freeze tuples.</summary>
    public const string AutovacuumFreezeMaxAge = "autovacuum_freeze_max_age";

    /// <summary>Freeze age below which a vacuum will not bother freezing.</summary>
    public const string AutovacuumFreezeMinAge = "autovacuum_freeze_min_age";

    /// <summary>Age at which a vacuum scans the whole table to freeze tuples.</summary>
    public const string AutovacuumFreezeTableAge = "autovacuum_freeze_table_age";

    /// <summary>Multixact equivalent of <see cref="AutovacuumFreezeMaxAge" />.</summary>
    public const string AutovacuumMultixactFreezeMaxAge = "autovacuum_multixact_freeze_max_age";

    /// <summary>Multixact equivalent of <see cref="AutovacuumFreezeMinAge" />.</summary>
    public const string AutovacuumMultixactFreezeMinAge = "autovacuum_multixact_freeze_min_age";

    /// <summary>Multixact equivalent of <see cref="AutovacuumFreezeTableAge" />.</summary>
    public const string AutovacuumMultixactFreezeTableAge = "autovacuum_multixact_freeze_table_age";

    /// <summary>
    ///     Milliseconds an autovacuum must exceed before it is logged; 0 logs every one, -1 none.
    /// </summary>
    public const string LogAutovacuumMinDuration = "log_autovacuum_min_duration";

    /// <summary>
    ///     Number of parallel workers a parallel scan of this table may use, overriding the
    ///     estimate from the table's size.
    /// </summary>
    public const string ParallelWorkers = "parallel_workers";

    /// <summary>
    ///     Whether a vacuum of this table performs index cleanup. PostgreSQL 12+, where it is a
    ///     boolean; PostgreSQL 14 added the <c>auto</c> / <c>on</c> / <c>off</c> spellings.
    /// </summary>
    public const string VacuumIndexCleanup = "vacuum_index_cleanup";

    /// <summary>Whether a vacuum tries to truncate empty pages off the end of the table.</summary>
    public const string VacuumTruncate = "vacuum_truncate";

    /// <summary>
    ///     Declares the table as an additional catalog table for the purposes of logical
    ///     replication decoding.
    /// </summary>
    public const string UserCatalogTable = "user_catalog_table";
}
