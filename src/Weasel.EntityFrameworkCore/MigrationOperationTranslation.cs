using JasperFx.Core;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Weasel.Core;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     Which EF Core relational provider the translated operations target.
///     Determines the provider annotations emitted for identity columns and
///     index extensions, and the referential-action normalization.
/// </summary>
public enum EfMigrationProvider
{
    PostgreSql,
    SqlServer
}

/// <summary>
///     Options for <see cref="MigrationOperationTranslation" />.
/// </summary>
public class MigrationOperationTranslationOptions
{
    public MigrationOperationTranslationOptions(EfMigrationProvider provider)
    {
        Provider = provider;
    }

    public EfMigrationProvider Provider { get; }

    /// <summary>
    ///     The provider's default schema ("public" for PostgreSQL, "dbo" for
    ///     SQL Server). Objects in the default schema are emitted with a null
    ///     Schema — matching what EF Core's own migration scaffolding does —
    ///     and no EnsureSchema operation is generated for it.
    /// </summary>
    public string DefaultSchema => Provider == EfMigrationProvider.PostgreSql ? "public" : "dbo";

    /// <summary>
    ///     Used to render raw-SQL fallback operations (schema objects EF cannot
    ///     model: partitioned tables, functions, stored procedures, ...) through
    ///     the object's own <see cref="ISchemaObject.WriteCreateStatement" />.
    ///     Required whenever a fallback is actually needed.
    /// </summary>
    public Migrator? Migrator { get; set; }

    /// <summary>
    ///     Force specific schema objects down the raw-SQL fallback path even
    ///     though they could be translated — e.g. a PostgreSQL partitioned
    ///     table, which EF has no model for (npgsql/efcore.pg#1035). Callers
    ///     with provider references downcast here.
    /// </summary>
    public Func<ISchemaObject, bool>? ForceRawSql { get; set; }

    /// <summary>
    ///     Annotation value written for identity columns on PostgreSQL. The
    ///     in-memory default is the enum member's string name
    ///     ("IdentityByDefaultColumn"); the C# migration file emitter renders
    ///     it as the proper <c>NpgsqlValueGenerationStrategy</c> literal.
    ///     Callers that feed the operations directly to the Npgsql
    ///     migrations SQL generator should overwrite this with the actual
    ///     <c>NpgsqlValueGenerationStrategy.IdentityByDefaultColumn</c> enum
    ///     value (Weasel.EntityFrameworkCore deliberately does not reference
    ///     the provider package).
    /// </summary>
    public object NpgsqlIdentityAnnotationValue { get; set; } = "IdentityByDefaultColumn";
}

/// <summary>
///     Translates Weasel's strongly-typed schema model into EF Core
///     <see cref="MigrationOperation" /> instances — the reverse direction of
///     <c>MapToTable</c> (EF model → Weasel) in <see cref="DbContextExtensions" />.
///     The operation list is the intermediate representation consumed by the
///     C# migration file emitter; raw store type strings are used everywhere
///     (<see cref="AddColumnOperation.ColumnType" />) so EF's CLR type mapping
///     is bypassed and the resulting DDL matches Weasel's own exactly.
/// </summary>
public static class MigrationOperationTranslation
{
    // annotation names, spelled as string literals so this project needs no
    // provider package references
    public const string NpgsqlValueGenerationStrategy = "Npgsql:ValueGenerationStrategy";
    public const string NpgsqlIndexMethod = "Npgsql:IndexMethod";
    public const string NpgsqlIndexInclude = "Npgsql:IndexInclude";
    public const string SqlServerIdentity = "SqlServer:Identity";
    public const string SqlServerIndexInclude = "SqlServer:Include";

    /// <summary>
    ///     Translate a set of Weasel schema objects into the ordered operation
    ///     list for an EF Core migration's Up() body: EnsureSchema operations
    ///     first (deduplicated, non-default schemas only), then each object in
    ///     the given order (callers are responsible for foreign-key dependency
    ///     ordering, e.g. via the same topological sort MapToTables uses).
    ///     Tables and sequences are translated structurally; everything else —
    ///     and any object matched by <see cref="MigrationOperationTranslationOptions.ForceRawSql" /> —
    ///     is wrapped in a <see cref="SqlOperation" /> holding its own CREATE DDL.
    /// </summary>
    public static IReadOnlyList<MigrationOperation> ToMigrationOperations(
        this IEnumerable<ISchemaObject> schemaObjects,
        MigrationOperationTranslationOptions options)
    {
        var objects = schemaObjects.ToArray();
        var operations = new List<MigrationOperation>();

        foreach (var schema in objects
                     .Select(x => x.Identifier.Schema)
                     .Where(s => s.IsNotEmpty() && !s.EqualsIgnoreCase(options.DefaultSchema))
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            operations.Add(new EnsureSchemaOperation { Name = schema });
        }

        foreach (var schemaObject in objects)
        {
            operations.AddRange(translateObject(schemaObject, options));
        }

        return operations;
    }

    /// <summary>
    ///     Translate the reverse (Down()) operation list for the same set of
    ///     schema objects: raw-SQL drops, then DropTable / DropSequence in
    ///     reverse order. Schemas are deliberately never dropped — they may be
    ///     shared with other tools (Marten, Wolverine, ...).
    /// </summary>
    public static IReadOnlyList<MigrationOperation> ToDropMigrationOperations(
        this IEnumerable<ISchemaObject> schemaObjects,
        MigrationOperationTranslationOptions options)
    {
        var operations = new List<MigrationOperation>();

        foreach (var schemaObject in schemaObjects.Reverse())
        {
            if (options.ForceRawSql?.Invoke(schemaObject) == true || schemaObject is not (ITable or SequenceBase))
            {
                operations.Add(rawSql(schemaObject, options, drop: true));
            }
            else if (schemaObject is ITable table)
            {
                operations.Add(new DropTableOperation
                {
                    Name = table.Identifier.Name, Schema = SchemaFor(table.Identifier.Schema, options)
                });
            }
            else if (schemaObject is SequenceBase sequence)
            {
                operations.Add(new DropSequenceOperation
                {
                    Name = sequence.Identifier.Name, Schema = SchemaFor(sequence.Identifier.Schema, options)
                });
            }
        }

        return operations;
    }

    /// <summary>
    ///     Translate a single Weasel table into its EF operations: an optional
    ///     EnsureSchema, the CreateTable (with columns, primary key, check
    ///     constraints and foreign keys nested), then one CreateIndex per index.
    /// </summary>
    public static IReadOnlyList<MigrationOperation> ToMigrationOperations(
        this ITable table,
        MigrationOperationTranslationOptions options)
    {
        return new ISchemaObject[] { table }.ToMigrationOperations(options);
    }

    private static IEnumerable<MigrationOperation> translateObject(
        ISchemaObject schemaObject,
        MigrationOperationTranslationOptions options)
    {
        if (options.ForceRawSql?.Invoke(schemaObject) == true)
        {
            yield return rawSql(schemaObject, options, drop: false);
            yield break;
        }

        switch (schemaObject)
        {
            case ITable table:
                foreach (var operation in TableOperations(SnapshotTable.From(table), options))
                {
                    yield return operation;
                }

                break;

            case SequenceBase sequence:
                yield return new CreateSequenceOperation
                {
                    Name = sequence.Identifier.Name,
                    Schema = SchemaFor(sequence.Identifier.Schema, options),
                    ClrType = typeof(long),
                    StartValue = sequence.StartWith ?? 1L,
                    IncrementBy = (int)(sequence.IncrementBy ?? 1L)
                };
                break;

            default:
                // functions, stored procedures, table types, extensions, ... —
                // EF has no model for these; carry the object's own DDL
                yield return rawSql(schemaObject, options, drop: false);
                break;
        }
    }

    /// <summary>
    ///     Build the operations for one table from its snapshot form — the
    ///     shared pipeline for both first-migration translation (ITable →
    ///     SnapshotTable → operations) and the incremental snapshot differ.
    /// </summary>
    internal static IEnumerable<MigrationOperation> TableOperations(
        SnapshotTable table,
        MigrationOperationTranslationOptions options)
    {
        var tableName = table.Name;
        var schema = SchemaFor(table.Schema, options);

        var createTable = new CreateTableOperation { Name = tableName, Schema = schema };

        foreach (var column in table.Columns)
        {
            createTable.Columns.Add(ColumnOperation(column, tableName, schema, options));
        }

        if (table.PrimaryKeyColumns.Any())
        {
            createTable.PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = table.PrimaryKeyName,
                Table = tableName,
                Schema = schema,
                Columns = table.PrimaryKeyColumns.ToArray()
            };
        }

        foreach (var check in table.CheckConstraints)
        {
            createTable.CheckConstraints.Add(new AddCheckConstraintOperation
            {
                Name = check.Name, Table = tableName, Schema = schema, Sql = check.Expression
            });
        }

        foreach (var foreignKey in table.ForeignKeys)
        {
            createTable.ForeignKeys.Add(ForeignKeyOperation(foreignKey, tableName, schema, options));
        }

        yield return createTable;

        foreach (var index in table.Indexes)
        {
            yield return IndexOperation(index, tableName, schema, options);
        }
    }

    internal static AddColumnOperation ColumnOperation(
        SnapshotColumn column,
        string tableName,
        string? schema,
        MigrationOperationTranslationOptions options)
    {
        var operation = new AddColumnOperation
        {
            Name = column.Name,
            Table = tableName,
            Schema = schema,
            // ColumnType always wins over the CLR mapping in generated DDL;
            // the CLR type is a best-effort inverse used only for the
            // table.Column<T>(...) generic argument in emitted C#
            ClrType = clrTypeFor(column.Type),
            ColumnType = column.Type,
            IsNullable = column.Nullable,
            DefaultValueSql = column.DefaultExpression
        };

        if (column.ComputedExpression.IsNotEmpty())
        {
            operation.ComputedColumnSql = column.ComputedExpression;
            // PostgreSQL only supports stored generated columns
            operation.IsStored = options.Provider == EfMigrationProvider.PostgreSql ||
                                 column.ComputedIsStored == true;
        }

        if (column.Identity)
        {
            switch (options.Provider)
            {
                case EfMigrationProvider.PostgreSql:
                    operation.AddAnnotation(NpgsqlValueGenerationStrategy, options.NpgsqlIdentityAnnotationValue);
                    break;
                case EfMigrationProvider.SqlServer:
                    operation.AddAnnotation(SqlServerIdentity, "1, 1");
                    break;
            }
        }

        return operation;
    }

    internal static MigrationOperation IndexOperation(
        SnapshotIndex index,
        string tableName,
        string? schema,
        MigrationOperationTranslationOptions options)
    {
        if (index.Columns.Count == 0)
        {
            throw new NotSupportedException(
                $"Index '{index.Name}' on {schema ?? "?"}.{tableName} has no key columns — " +
                "expression-based indexes cannot be expressed as an EF CreateIndex operation. " +
                $"Route the table through {nameof(MigrationOperationTranslationOptions.ForceRawSql)} instead.");
        }

        if (options.Provider == EfMigrationProvider.SqlServer && index.IsUnique && index.Predicate.IsEmpty())
        {
            // EF's SQL Server generator auto-appends a WHERE col IS NOT NULL
            // filter to unique indexes whenever it cannot prove the columns
            // non-nullable from the migration's target model — which, with
            // attribute-only migrations, is always. Emit the index as raw DDL
            // so the created index matches Weasel's own
            var columns = string.Join("], [", index.Columns);
            var include = index.IncludeColumns is { Count: > 0 }
                ? $" INCLUDE ([{string.Join("], [", index.IncludeColumns)}])"
                : string.Empty;
            var qualifiedTable = $"[{schema ?? options.DefaultSchema}].[{tableName}]";
            return new SqlOperation
            {
                Sql = $"CREATE UNIQUE INDEX [{index.Name}] ON {qualifiedTable} ([{columns}]){include};"
            };
        }

        var operation = new CreateIndexOperation
        {
            Name = index.Name,
            Table = tableName,
            Schema = schema,
            Columns = index.Columns.ToArray(),
            IsUnique = index.IsUnique,
            Filter = index.Predicate
        };

        if (index.IncludeColumns is { Count: > 0 })
        {
            operation.AddAnnotation(
                options.Provider == EfMigrationProvider.PostgreSql ? NpgsqlIndexInclude : SqlServerIndexInclude,
                index.IncludeColumns.ToArray());
        }

        if (index.Method.IsNotEmpty() && options.Provider == EfMigrationProvider.PostgreSql &&
            !index.Method!.EqualsIgnoreCase("btree"))
        {
            operation.AddAnnotation(NpgsqlIndexMethod, index.Method);
        }

        return operation;
    }

    internal static AddForeignKeyOperation ForeignKeyOperation(
        SnapshotForeignKey foreignKey,
        string tableName,
        string? schema,
        MigrationOperationTranslationOptions options)
    {
        if (foreignKey.PrincipalTable.IsEmpty())
        {
            throw new MisconfiguredForeignKeyException(
                $"Foreign key '{foreignKey.Name}' on {schema ?? "?"}.{tableName} has no linked table");
        }

        return new AddForeignKeyOperation
        {
            Name = foreignKey.Name,
            Table = tableName,
            Schema = schema,
            Columns = foreignKey.Columns.ToArray(),
            PrincipalTable = foreignKey.PrincipalTable,
            PrincipalSchema = SchemaFor(foreignKey.PrincipalSchema, options),
            PrincipalColumns = foreignKey.PrincipalColumns.ToArray(),
            OnDelete = referentialActionFor(foreignKey.OnDelete, options),
            OnUpdate = referentialActionFor(foreignKey.OnUpdate, options)
        };
    }

    private static ReferentialAction referentialActionFor(
        CascadeAction action,
        MigrationOperationTranslationOptions options)
    {
        return action switch
        {
            CascadeAction.Cascade => ReferentialAction.Cascade,
            CascadeAction.SetNull => ReferentialAction.SetNull,
            CascadeAction.SetDefault => ReferentialAction.SetDefault,
            // SQL Server has no RESTRICT — it is spelled NO ACTION, mirroring
            // the mapDeleteBehavior normalization in the EF → Weasel direction
            CascadeAction.Restrict when options.Provider == EfMigrationProvider.SqlServer =>
                ReferentialAction.NoAction,
            CascadeAction.Restrict => ReferentialAction.Restrict,
            _ => ReferentialAction.NoAction
        };
    }

    private static SqlOperation rawSql(
        ISchemaObject schemaObject,
        MigrationOperationTranslationOptions options,
        bool drop)
    {
        if (options.Migrator == null)
        {
            throw new InvalidOperationException(
                $"{schemaObject.Identifier.QualifiedName} ({schemaObject.GetType().Name}) requires the raw-SQL " +
                $"fallback, so {nameof(MigrationOperationTranslationOptions)}.{nameof(MigrationOperationTranslationOptions.Migrator)} must be provided");
        }

        var writer = new StringWriter();
        if (drop)
        {
            schemaObject.WriteDropStatement(options.Migrator, writer);
        }
        else
        {
            schemaObject.WriteCreateStatement(options.Migrator, writer);
        }

        return new SqlOperation { Sql = writer.ToString() };
    }

    internal static string? SchemaFor(string? schema, MigrationOperationTranslationOptions options)
    {
        if (schema.IsEmpty() || schema!.EqualsIgnoreCase(options.DefaultSchema))
        {
            return null;
        }

        return schema;
    }

    /// <summary>
    ///     Best-effort inverse of the provider type mappings, used only for the
    ///     table.Column&lt;T&gt;(...) generic argument in emitted C# — the raw
    ///     ColumnType string is what actually drives the DDL.
    /// </summary>
    private static Type clrTypeFor(string storeType)
    {
        var raw = storeType.ToLowerInvariant().Split('(')[0].Trim();

        return raw switch
        {
            "int" or "integer" or "int4" or "serial" => typeof(int),
            "bigint" or "int8" or "bigserial" => typeof(long),
            "smallint" or "int2" or "smallserial" => typeof(short),
            "tinyint" => typeof(byte),
            "bit" or "boolean" or "bool" => typeof(bool),
            "real" or "float4" => typeof(float),
            "float" or "double precision" or "float8" => typeof(double),
            "decimal" or "numeric" or "money" or "smallmoney" => typeof(decimal),
            "uuid" or "uniqueidentifier" => typeof(Guid),
            "date" => typeof(DateOnly),
            "time" or "time without time zone" => typeof(TimeOnly),
            "timestamp" or "timestamp without time zone" or "datetime" or "datetime2" or "smalldatetime"
                => typeof(DateTime),
            "timestamptz" or "timestamp with time zone" or "datetimeoffset" => typeof(DateTimeOffset),
            "bytea" or "varbinary" or "binary" or "image" or "rowversion" => typeof(byte[]),
            _ => typeof(string)
        };
    }
}
