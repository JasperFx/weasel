using System.Globalization;
using System.Text;
using JasperFx.Core;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace Weasel.EntityFrameworkCore;

/// <summary>
///     A generated EF Core migration source file.
/// </summary>
public class EfMigrationFile
{
    public EfMigrationFile(string migrationId, string className, string code)
    {
        MigrationId = migrationId;
        ClassName = className;
        Code = code;
    }

    /// <summary>The EF migration id, e.g. "20260718123456_InitialWeaselSchema"</summary>
    public string MigrationId { get; }

    /// <summary>The migration class name, e.g. "InitialWeaselSchema"</summary>
    public string ClassName { get; }

    /// <summary>Suggested file name: "&lt;MigrationId&gt;.cs"</summary>
    public string FileName => $"{MigrationId}.cs";

    /// <summary>The full C# source of the migration file</summary>
    public string Code { get; }
}

/// <summary>
///     Options for <see cref="EfMigrationFileEmitter" />.
/// </summary>
public class EfMigrationEmissionOptions
{
    public EfMigrationEmissionOptions(string contextTypeName)
    {
        if (string.IsNullOrWhiteSpace(contextTypeName))
        {
            throw new ArgumentException("contextTypeName must not be null or blank", nameof(contextTypeName));
        }

        ContextTypeName = contextTypeName;
    }

    /// <summary>
    ///     The stub DbContext type name the migrations bind to via
    ///     [DbContext(typeof(...))], e.g. "MartenSchemaDbContext".
    /// </summary>
    public string ContextTypeName { get; }

    /// <summary>Namespace for the generated files</summary>
    public string Namespace { get; set; } = "WeaselMigrations";

    /// <summary>
    ///     UTC timestamp used for the migration id. Defaults to the current UTC
    ///     time; fix it for deterministic output in tests.
    /// </summary>
    public DateTime? TimestampUtc { get; set; }

    /// <summary>
    ///     The id of the previously generated migration, if any. EF orders
    ///     migrations by plain string sort of the id, so when a new id would not
    ///     sort after this one (e.g. two migrations generated within the same
    ///     second) the timestamp is bumped forward until it does.
    /// </summary>
    public string? LastMigrationId { get; set; }
}

/// <summary>
///     Renders translated <see cref="MigrationOperation" /> lists (see
///     <see cref="MigrationOperationTranslation" />) into compilable, attribute-only
///     C# migration files plus the stub DbContext that hosts them. This is a small
///     hand-rolled emitter over the stable public MigrationBuilder fluent surface —
///     deliberately not EF's internal CSharpMigrationsGenerator scaffolding stack,
///     which is pubternal and unsupported for programmatic use (dotnet/efcore#23595).
///     The generated migrations carry no BuildTargetModel body: the empty target
///     model is legal and only `dotnet ef migrations remove` would miss it
///     (verified end-to-end by the #364 spike).
/// </summary>
public static class EfMigrationFileEmitter
{
    private static readonly HashSet<string> ReservedWords = new()
    {
        "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked", "class", "const",
        "continue", "decimal", "default", "delegate", "do", "double", "else", "enum", "event", "explicit", "extern",
        "false", "finally", "fixed", "float", "for", "foreach", "goto", "if", "implicit", "in", "int", "interface",
        "internal", "is", "lock", "long", "namespace", "new", "null", "object", "operator", "out", "override",
        "params", "private", "protected", "public", "readonly", "ref", "return", "sbyte", "sealed", "short",
        "sizeof", "stackalloc", "static", "string", "struct", "switch", "this", "throw", "true", "try", "typeof",
        "uint", "ulong", "unchecked", "unsafe", "ushort", "using", "virtual", "void", "volatile", "while"
    };

    /// <summary>
    ///     Render one migration file from the translated Up and Down operation
    ///     lists.
    /// </summary>
    public static EfMigrationFile EmitMigration(
        string name,
        IReadOnlyList<MigrationOperation> upOperations,
        IReadOnlyList<MigrationOperation> downOperations,
        EfMigrationEmissionOptions options)
    {
        var className = sanitizeIdentifier(name);
        var migrationId = buildMigrationId(className, options);

        var body = new StringBuilder();

        var usings = new SortedSet<string>(StringComparer.Ordinal)
        {
            "Microsoft.EntityFrameworkCore.Infrastructure",
            "Microsoft.EntityFrameworkCore.Migrations"
        };

        var allColumns = upOperations.Concat(downOperations)
            .SelectMany(op => op is CreateTableOperation createTable
                ? createTable.Columns.Cast<MigrationOperation>()
                : new[] { op });
        if (allColumns.Any(op => op[MigrationOperationTranslation.NpgsqlValueGenerationStrategy] != null))
        {
            usings.Add("Npgsql.EntityFrameworkCore.PostgreSQL.Metadata");
        }

        foreach (var u in usings)
        {
            body.AppendLine($"using {u};");
        }

        body.AppendLine();
        body.AppendLine($"namespace {options.Namespace};");
        body.AppendLine();
        body.AppendLine($"[DbContext(typeof({options.ContextTypeName}))]");
        body.AppendLine($"[Migration(\"{migrationId}\")]");
        body.AppendLine($"public partial class {className} : Migration");
        body.AppendLine("{");
        body.AppendLine("    protected override void Up(MigrationBuilder migrationBuilder)");
        body.AppendLine("    {");
        writeOperations(body, upOperations);
        body.AppendLine("    }");
        body.AppendLine();
        body.AppendLine("    protected override void Down(MigrationBuilder migrationBuilder)");
        body.AppendLine("    {");
        writeOperations(body, downOperations);
        body.AppendLine("    }");
        body.AppendLine("}");

        return new EfMigrationFile(migrationId, className, body.ToString());
    }

    /// <summary>
    ///     Render the stub DbContext that hosts the generated migrations: no
    ///     entities, provider configured, migrations history table relocated into
    ///     the given schema so it never collides with the application's own EF
    ///     context, and the EF 9+ pending-model-changes warning suppressed. A
    ///     design-time factory driven by the WEASEL_EF_CONNECTION environment
    ///     variable is included so `dotnet ef database update` works without an
    ///     application host.
    /// </summary>
    public static string EmitStubContext(
        EfMigrationProvider provider,
        EfMigrationEmissionOptions options,
        string historySchema,
        string historyTableName = "__EFMigrationsHistory")
    {
        var contextName = options.ContextTypeName;
        var useMethod = provider == EfMigrationProvider.PostgreSql ? "UseNpgsql" : "UseSqlServer";
        var cli = $"dotnet ef database update --context {contextName}";

        return $@"using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace {options.Namespace};

/// <summary>
///     Stub DbContext generated by Weasel to host the Weasel-authored EF Core
///     migrations. It has no entities on purpose — the migrations are
///     attribute-only and carry their own operations.
///     <para>Register at startup with:</para>
///     <code>
///     services.AddDbContext&lt;{contextName}&gt;(o =&gt;
///         o.{useMethod}(connectionString,
///             m =&gt; m.MigrationsHistoryTable(""{historyTableName}"", ""{historySchema}"")));
///     </code>
///     <para>
///     or apply from the command line (reads the WEASEL_EF_CONNECTION
///     environment variable): <c>{cli}</c>
///     </para>
/// </summary>
public partial class {contextName} : DbContext
{{
    public {contextName}()
    {{
    }}

    public {contextName}(DbContextOptions<{contextName}> options) : base(options)
    {{
    }}

    /// <summary>
    ///     Connection string used when the context is constructed without
    ///     options (e.g. by the EF design-time tools). Set it at startup or
    ///     let the design-time factory read WEASEL_EF_CONNECTION.
    /// </summary>
    public static string? ConnectionString {{ get; set; }}

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {{
        if (!optionsBuilder.IsConfigured)
        {{
            optionsBuilder.{useMethod}(
                ConnectionString
                ?? Environment.GetEnvironmentVariable(""WEASEL_EF_CONNECTION"")
                ?? throw new InvalidOperationException(
                    ""Set {contextName}.ConnectionString or the WEASEL_EF_CONNECTION environment variable""),
                m => m.MigrationsHistoryTable(""{historyTableName}"", ""{historySchema}""));
        }}

        // EF 9+ throws from Migrate() when the context model does not match the
        // last migration; harmless here (attribute-only migrations carry no
        // model snapshot) but suppressed defensively
        optionsBuilder.ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning));
    }}
}}

/// <summary>
///     Design-time factory so `{cli}`, `dotnet ef migrations script` and
///     `dotnet ef migrations bundle` work without an application host.
/// </summary>
public partial class {contextName}Factory : IDesignTimeDbContextFactory<{contextName}>
{{
    public {contextName} CreateDbContext(string[] args)
    {{
        var builder = new DbContextOptionsBuilder<{contextName}>();
        builder.{useMethod}(
            {contextName}.ConnectionString
            ?? Environment.GetEnvironmentVariable(""WEASEL_EF_CONNECTION"")
            ?? throw new InvalidOperationException(
                ""Set the WEASEL_EF_CONNECTION environment variable for design-time use""),
            m => m.MigrationsHistoryTable(""{historyTableName}"", ""{historySchema}""));
        return new {contextName}(builder.Options);
    }}
}}
";
    }

    // ------------------------------------------------------------------
    // migration ids
    // ------------------------------------------------------------------

    private static string buildMigrationId(string className, EfMigrationEmissionOptions options)
    {
        var timestamp = options.TimestampUtc ?? DateTime.UtcNow;

        if (options.LastMigrationId.IsNotEmpty())
        {
            var lastStamp = options.LastMigrationId!.Split('_')[0];
            // EF orders migrations by plain string sort of the id — bump until
            // the new timestamp sorts strictly after the previous one
            while (string.Compare(
                       timestamp.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture),
                       lastStamp, StringComparison.Ordinal) <= 0)
            {
                timestamp = timestamp.AddSeconds(1);
            }
        }

        return $"{timestamp.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture)}_{className}";
    }

    // ------------------------------------------------------------------
    // operation rendering
    // ------------------------------------------------------------------

    private static void writeOperations(StringBuilder body, IReadOnlyList<MigrationOperation> operations)
    {
        for (var i = 0; i < operations.Count; i++)
        {
            if (i > 0)
            {
                body.AppendLine();
            }

            writeOperation(body, operations[i]);
        }
    }

    private static void writeOperation(StringBuilder body, MigrationOperation operation)
    {
        switch (operation)
        {
            case EnsureSchemaOperation ensureSchema:
                body.AppendLine($"        migrationBuilder.EnsureSchema(name: {quote(ensureSchema.Name)});");
                break;

            case CreateTableOperation createTable:
                writeCreateTable(body, createTable);
                break;

            case CreateIndexOperation createIndex:
                writeCreateIndex(body, createIndex);
                break;

            case CreateSequenceOperation createSequence:
                writeCreateSequence(body, createSequence);
                break;

            case SqlOperation sql:
                body.AppendLine($"        migrationBuilder.Sql(@{verbatim(sql.Sql)});");
                break;

            case DropTableOperation dropTable:
                body.Append($"        migrationBuilder.DropTable(name: {quote(dropTable.Name)}");
                if (dropTable.Schema.IsNotEmpty())
                {
                    body.Append($", schema: {quote(dropTable.Schema!)}");
                }

                body.AppendLine(");");
                break;

            case DropSequenceOperation dropSequence:
                body.Append($"        migrationBuilder.DropSequence(name: {quote(dropSequence.Name)}");
                if (dropSequence.Schema.IsNotEmpty())
                {
                    body.Append($", schema: {quote(dropSequence.Schema!)}");
                }

                body.AppendLine(");");
                break;

            default:
                throw new NotSupportedException(
                    $"The emitter does not know how to render a {operation.GetType().Name}");
        }
    }

    private static void writeCreateTable(StringBuilder body, CreateTableOperation createTable)
    {
        body.AppendLine("        migrationBuilder.CreateTable(");
        body.AppendLine($"            name: {quote(createTable.Name)},");
        if (createTable.Schema.IsNotEmpty())
        {
            body.AppendLine($"            schema: {quote(createTable.Schema!)},");
        }

        body.AppendLine("            columns: table => new");
        body.AppendLine("            {");
        for (var i = 0; i < createTable.Columns.Count; i++)
        {
            writeTableColumn(body, createTable.Columns[i], i == createTable.Columns.Count - 1);
        }

        body.AppendLine("            },");
        body.AppendLine("            constraints: table =>");
        body.AppendLine("            {");

        if (createTable.PrimaryKey != null)
        {
            var pk = createTable.PrimaryKey;
            var accessor = pk.Columns.Length == 1
                ? $"x => x.{memberName(pk.Columns[0])}"
                : $"x => new {{ {pk.Columns.Select(c => "x." + memberName(c)).Join(", ")} }}";
            body.AppendLine($"                table.PrimaryKey({quote(pk.Name!)}, {accessor});");
        }

        foreach (var check in createTable.CheckConstraints)
        {
            body.AppendLine($"                table.CheckConstraint({quote(check.Name!)}, {quote(check.Sql)});");
        }

        foreach (var fk in createTable.ForeignKeys)
        {
            body.AppendLine("                table.ForeignKey(");
            body.AppendLine($"                    name: {quote(fk.Name!)},");
            if (fk.Columns.Length == 1)
            {
                body.AppendLine($"                    column: x => x.{memberName(fk.Columns[0])},");
            }
            else
            {
                body.AppendLine(
                    $"                    columns: x => new {{ {fk.Columns.Select(c => "x." + memberName(c)).Join(", ")} }},");
            }

            if (fk.PrincipalSchema.IsNotEmpty())
            {
                body.AppendLine($"                    principalSchema: {quote(fk.PrincipalSchema!)},");
            }

            body.AppendLine($"                    principalTable: {quote(fk.PrincipalTable)},");
            if (fk.PrincipalColumns is { Length: 1 })
            {
                body.AppendLine($"                    principalColumn: {quote(fk.PrincipalColumns[0])},");
            }
            else if (fk.PrincipalColumns is { Length: > 1 })
            {
                body.AppendLine(
                    $"                    principalColumns: {stringArray(fk.PrincipalColumns)},");
            }

            body.AppendLine($"                    onDelete: ReferentialAction.{fk.OnDelete},");
            body.AppendLine($"                    onUpdate: ReferentialAction.{fk.OnUpdate});");
        }

        body.AppendLine("            });");

        foreach (var annotation in createTable.GetAnnotations())
        {
            // table-level annotations attach after the call — none are produced
            // by the translation layer today, but keep the emitter honest
            throw new NotSupportedException(
                $"The emitter does not know how to render table annotation '{annotation.Name}'");
        }
    }

    private static void writeTableColumn(StringBuilder body, AddColumnOperation column, bool last)
    {
        var member = memberName(column.Name);

        body.Append($"                {member} = table.Column<{clrTypeName(column.ClrType)}>(");

        var arguments = new List<string>();
        if (!string.Equals(memberToColumnName(member), column.Name, StringComparison.Ordinal))
        {
            arguments.Add($"name: {quote(column.Name)}");
        }

        arguments.Add($"type: {quote(column.ColumnType!)}");
        arguments.Add($"nullable: {(column.IsNullable ? "true" : "false")}");

        if (column.DefaultValueSql.IsNotEmpty())
        {
            arguments.Add($"defaultValueSql: {quote(column.DefaultValueSql!)}");
        }

        if (column.ComputedColumnSql.IsNotEmpty())
        {
            arguments.Add($"computedColumnSql: {quote(column.ComputedColumnSql!)}");
            if (column.IsStored == true)
            {
                arguments.Add("stored: true");
            }
        }

        body.Append(arguments.Join(", "));
        body.Append(')');

        foreach (var annotation in column.GetAnnotations())
        {
            body.AppendLine();
            body.Append($"                    .Annotation({quote(annotation.Name)}, {annotationValue(annotation.Name, annotation.Value)})");
        }

        body.AppendLine(last ? string.Empty : ",");
    }

    private static void writeCreateIndex(StringBuilder body, CreateIndexOperation createIndex)
    {
        body.AppendLine("        migrationBuilder.CreateIndex(");
        body.AppendLine($"            name: {quote(createIndex.Name)},");
        if (createIndex.Schema.IsNotEmpty())
        {
            body.AppendLine($"            schema: {quote(createIndex.Schema!)},");
        }

        body.AppendLine($"            table: {quote(createIndex.Table!)},");
        body.Append(createIndex.Columns.Length == 1
            ? $"            column: {quote(createIndex.Columns[0])}"
            : $"            columns: {stringArray(createIndex.Columns)}");

        if (createIndex.IsUnique)
        {
            body.AppendLine(",");
            body.Append("            unique: true");
        }

        if (createIndex.Filter.IsNotEmpty())
        {
            body.AppendLine(",");
            body.Append($"            filter: {quote(createIndex.Filter!)}");
        }

        body.Append(')');

        foreach (var annotation in createIndex.GetAnnotations())
        {
            body.AppendLine();
            body.Append(
                $"            .Annotation({quote(annotation.Name)}, {annotationValue(annotation.Name, annotation.Value)})");
        }

        body.AppendLine(";");
    }

    private static void writeCreateSequence(StringBuilder body, CreateSequenceOperation createSequence)
    {
        body.AppendLine("        migrationBuilder.CreateSequence(");
        body.Append($"            name: {quote(createSequence.Name)}");
        if (createSequence.Schema.IsNotEmpty())
        {
            body.AppendLine(",");
            body.Append($"            schema: {quote(createSequence.Schema!)}");
        }

        if (createSequence.StartValue != 1L)
        {
            body.AppendLine(",");
            body.Append($"            startValue: {createSequence.StartValue}L");
        }

        if (createSequence.IncrementBy != 1)
        {
            body.AppendLine(",");
            body.Append($"            incrementBy: {createSequence.IncrementBy}");
        }

        body.AppendLine(");");
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static string annotationValue(string name, object? value)
    {
        if (name == MigrationOperationTranslation.NpgsqlValueGenerationStrategy)
        {
            // rendered as the provider enum literal — the generated file adds
            // the Npgsql.EntityFrameworkCore.PostgreSQL.Metadata using
            return $"NpgsqlValueGenerationStrategy.{value}";
        }

        return value switch
        {
            null => "null",
            string s => quote(s),
            string[] array => stringArray(array),
            bool b => b ? "true" : "false",
            int i => i.ToString(CultureInfo.InvariantCulture),
            long l => $"{l}L",
            _ => throw new NotSupportedException(
                $"The emitter does not know how to render annotation '{name}' with value type {value.GetType().Name}")
        };
    }

    private static string clrTypeName(Type type)
    {
        if (type == typeof(int)) return "int";
        if (type == typeof(long)) return "long";
        if (type == typeof(short)) return "short";
        if (type == typeof(byte)) return "byte";
        if (type == typeof(bool)) return "bool";
        if (type == typeof(float)) return "float";
        if (type == typeof(double)) return "double";
        if (type == typeof(decimal)) return "decimal";
        if (type == typeof(string)) return "string";
        if (type == typeof(Guid)) return "Guid";
        if (type == typeof(DateTime)) return "DateTime";
        if (type == typeof(DateTimeOffset)) return "DateTimeOffset";
        if (type == typeof(DateOnly)) return "DateOnly";
        if (type == typeof(TimeOnly)) return "TimeOnly";
        if (type == typeof(byte[])) return "byte[]";
        return type.FullName!;
    }

    /// <summary>
    ///     Anonymous-type member name for a column. Valid identifiers are used
    ///     directly (reserved words get an @ prefix, so the member name still
    ///     round-trips to the exact column name); anything else is sanitized and
    ///     the exact column name is passed via the name: argument instead.
    /// </summary>
    private static string memberName(string columnName)
    {
        if (isValidIdentifier(columnName))
        {
            return ReservedWords.Contains(columnName) ? "@" + columnName : columnName;
        }

        return sanitizeIdentifier(columnName);
    }

    private static string memberToColumnName(string member)
        => member.StartsWith('@') ? member[1..] : member;

    private static bool isValidIdentifier(string name)
    {
        if (name.IsEmpty())
        {
            return false;
        }

        if (!char.IsLetter(name[0]) && name[0] != '_')
        {
            return false;
        }

        return name.Skip(1).All(c => char.IsLetterOrDigit(c) || c == '_');
    }

    private static string sanitizeIdentifier(string name)
    {
        var builder = new StringBuilder();
        foreach (var c in name)
        {
            builder.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        }

        if (builder.Length == 0 || (!char.IsLetter(builder[0]) && builder[0] != '_'))
        {
            builder.Insert(0, '_');
        }

        var result = builder.ToString();
        return ReservedWords.Contains(result) ? "@" + result : result;
    }

    private static string quote(string value)
        => $"\"{value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r").Replace("\n", "\\n")}\"";

    private static string verbatim(string value)
        => $"\"{value.Replace("\"", "\"\"")}\"";

    private static string stringArray(IEnumerable<string> values)
        => $"new[] {{ {values.Select(quote).Join(", ")} }}";
}
