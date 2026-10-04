using System.Data.Common;
using System.Globalization;
using System.Text;
using JasperFx.Core;
using Weasel.Firebird.Tables;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Firebird;

internal enum PsqlRoutineKind
{
    Function,
    Procedure
}

/// <summary>
///     How a parameter's type is declared: a data type, a domain, <c>TYPE OF</c> a domain, or
///     <c>TYPE OF COLUMN</c> a table's column. The catalog records which, so each is compared as itself.
/// </summary>
internal enum PsqlTypeKind
{
    DataType,
    Domain,
    TypeOfDomain,
    TypeOfColumn
}

/// <summary>
///     One parameter of a function or procedure, or a function's return value (which has no name).
///     Names are in the spelling the catalog stores.
/// </summary>
internal sealed record PsqlParameter(
    string? Name,
    PsqlTypeKind Kind,
    FirebirdColumnType DataType,
    string? Domain,
    string? Relation,
    string? Column,
    bool NotNull,
    string? Collation,
    string? Default)
{
    /// <summary>
    ///     Does <paramref name="actual" />, read from the catalog, satisfy this parameter, the model's?
    ///     A character set, a collation and a numeric precision are compared only when the model states
    ///     them, as for a table's columns (weasel#644).
    /// </summary>
    public bool IsSatisfiedBy(PsqlParameter actual)
    {
        if (!string.Equals(Name, actual.Name, StringComparison.Ordinal) || Kind != actual.Kind
                                                                      || NotNull != actual.NotNull)
        {
            return false;
        }

        var typeMatches = Kind switch
        {
            PsqlTypeKind.DataType => (DataType with { Collation = Collation }).IsSatisfiedBy(
                actual.DataType with { Collation = actual.Collation }),
            PsqlTypeKind.TypeOfColumn => Relation == actual.Relation && Column == actual.Column,
            _ => Domain == actual.Domain
        };

        if (!typeMatches)
        {
            return false;
        }

        if (Kind != PsqlTypeKind.DataType && Collation != null
                                          && !string.Equals(Collation, actual.Collation, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return PsqlLexer.NormalizeSource(Default) == PsqlLexer.NormalizeSource(actual.Default);
    }

    /// <summary>
    ///     The declaration as a statement writes it: the type, then <c>NOT NULL</c>, <c>COLLATE</c> and
    ///     <c>DEFAULT</c>, in the order Firebird's grammar requires. Names are delimited, so a name read
    ///     out of the catalog is written back exactly.
    /// </summary>
    public string ToDeclaration()
    {
        var builder = new StringBuilder();
        if (Name != null)
        {
            builder.Append(delimit(Name)).Append(' ');
        }

        builder.Append(TypeText());

        if (NotNull)
        {
            builder.Append(" NOT NULL");
        }

        if (Collation != null)
        {
            builder.Append(" COLLATE ").Append(Collation);
        }

        if (Default != null)
        {
            builder.Append(" DEFAULT ").Append(Default);
        }

        return builder.ToString();
    }

    public string TypeText() => Kind switch
    {
        PsqlTypeKind.DataType => (DataType with { Collation = null }).ToString(),
        PsqlTypeKind.Domain => delimit(Domain!),
        PsqlTypeKind.TypeOfDomain => $"TYPE OF {delimit(Domain!)}",
        _ => $"TYPE OF COLUMN {delimit(Relation!)}.{delimit(Column!)}"
    };

    public override string ToString() => ToDeclaration();

    private static string delimit(string name) => FirebirdIdentifierRules.Instance.Delimit(name);
}

/// <summary>
///     A PSQL function or procedure as Weasel compares it: its parameters, its return type or output
///     parameters, its options and its body -- read either from the statement a model was given or from
///     the catalog.
/// </summary>
/// <remarks>
///     <para>
///         Firebird keeps only the body of a routine as source -- <c>RDB$FUNCTION_SOURCE</c> and
///         <c>RDB$PROCEDURE_SOURCE</c> hold the text after <c>AS</c>, trimmed -- and records the header
///         in <c>RDB$FUNCTION_ARGUMENTS</c> and <c>RDB$PROCEDURE_PARAMETERS</c>. So the model's statement
///         is parsed into the same parts and the two are compared part by part: comparing the body alone
///         would miss a changed parameter type for good.
///     </para>
///     <para>
///         The statement's verb is rewritten to <c>CREATE OR ALTER</c>, whether it was written
///         <c>CREATE</c>, <c>RECREATE</c> or <c>ALTER</c>: that is the one form that creates the routine
///         when it is missing and alters it in place, dependencies and privileges intact, when it is not.
///     </para>
/// </remarks>
internal sealed class PsqlRoutine
{
    private static readonly HashSet<string> DataTypeNames = new(StringComparer.Ordinal)
    {
        "SMALLINT", "INTEGER", "BIGINT", "INT128", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE PRECISION",
        "DECFLOAT", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "CHAR", "VARCHAR", "BLOB"
    };

    private PsqlRoutine(PsqlRoutineKind kind, string catalogName)
    {
        Kind = kind;
        CatalogName = catalogName;
    }

    public PsqlRoutineKind Kind { get; }

    /// <summary>
    ///     The routine's name as the catalog stores it.
    /// </summary>
    public string CatalogName { get; }

    public List<PsqlParameter> Inputs { get; } = [];

    /// <summary>
    ///     A procedure's <c>RETURNS (…)</c>.
    /// </summary>
    public List<PsqlParameter> Outputs { get; } = [];

    /// <summary>
    ///     A function's <c>RETURNS</c> type.
    /// </summary>
    public PsqlParameter? Returns { get; private set; }

    public bool Deterministic { get; private set; }

    /// <summary>
    ///     <c>DEFINER</c>, <c>INVOKER</c>, or null when the routine does not say (Firebird 4 and later).
    /// </summary>
    public string? SqlSecurity { get; private set; }

    /// <summary>
    ///     Whether <see cref="SqlSecurity" /> was read. It is not on Firebird 3, which has no such clause.
    /// </summary>
    public bool SqlSecurityKnown { get; private set; } = true;

    /// <summary>
    ///     The version of the server a routine was read from, which a model has to be parsed for.
    /// </summary>
    public FirebirdServerVersion? Version { get; private set; }

    /// <summary>
    ///     Everything after <c>AS</c>: the declarations and the <c>BEGIN … END</c> block.
    /// </summary>
    public string Body { get; private set; } = string.Empty;

    /// <summary>
    ///     The statement, with its verb rewritten to <c>CREATE OR ALTER</c> and any trailing <c>;</c> left
    ///     off.
    /// </summary>
    public string Statement { get; private set; } = string.Empty;

    private string KindKeyword => Kind == PsqlRoutineKind.Function ? "FUNCTION" : "PROCEDURE";

    /// <summary>
    ///     Parse <c>CREATE [OR ALTER] | RECREATE | ALTER {FUNCTION | PROCEDURE} name [(…)] [RETURNS …]
    ///     [DETERMINISTIC] [SQL SECURITY …] AS body</c>.
    /// </summary>
    /// <param name="version">
    ///     The server the routine is compared against, which decides where <c>FLOAT(p)</c> becomes a
    ///     double. Irrelevant when the statement is only being written.
    /// </param>
    /// <exception cref="ArgumentException">The statement is not a PSQL routine of this kind.</exception>
    /// <exception cref="NotSupportedException">The routine is external (a UDR).</exception>
    public static PsqlRoutine Parse(string statement, PsqlRoutineKind kind, FirebirdServerVersion? version = null)
    {
        var sql = statement.TrimEnd().TrimEnd(';').TrimEnd();
        var tokens = PsqlLexer.Tokenize(sql);
        var reader = new HeaderReader(sql, tokens, kind);

        return reader.Read(version);
    }

    /// <summary>
    ///     The columns every parameter row carries, after the columns of the routine itself, in the order
    ///     <see cref="readParameter" /> reads them. <c>{0}</c> is the parameter table's alias and
    ///     <c>{1}</c> the prefix of its name and mechanism columns, which the two tables spell
    ///     differently.
    /// </summary>
    private const string ParameterColumns = """
            TRIM({0}.RDB${1}_NAME),
            TRIM({0}.RDB$FIELD_SOURCE),
            COALESCE({0}.RDB${1}_MECHANISM, 0),
            COALESCE({0}.RDB$NULL_FLAG, 0),
            {0}.RDB$DEFAULT_SOURCE,
            TRIM({0}.RDB$RELATION_NAME),
            TRIM({0}.RDB$FIELD_NAME),
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            f.RDB$CHARACTER_LENGTH,
            TRIM(cs.RDB$CHARACTER_SET_NAME),
            COALESCE({0}.RDB$COLLATION_ID, f.RDB$COLLATION_ID, 0),
            TRIM(co.RDB$COLLATION_NAME)
        """;

    /// <summary>
    ///     Where the parameter columns start in a row of <see cref="FunctionQuery" /> or
    ///     <see cref="ProcedureQuery" />: after the source, the determinism flag, SQL SECURITY, the engine
    ///     version, and which list the row belongs to.
    /// </summary>
    private const int FirstParameterColumn = 5;

    private const string ParameterJoins = """
        LEFT JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = {0}.RDB$FIELD_SOURCE
        LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
        LEFT JOIN RDB$COLLATIONS co ON co.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
            AND co.RDB$COLLATION_ID = COALESCE({0}.RDB$COLLATION_ID, f.RDB$COLLATION_ID, 0)
        """;

    /// <summary>
    ///     One query for a function: a row per argument, the return value included, each carrying the
    ///     function's source and options. A function always has its return value, so it always has a row.
    ///     Unterminated, because Firebird runs one statement per command.
    /// </summary>
    /// <param name="nameParameter">The parameter bound to the catalog name, with its prefix.</param>
    /// <param name="readSqlSecurity">
    ///     Whether the server has <c>RDB$SQL_SECURITY</c>, which arrived in Firebird 4.
    /// </param>
    public static string FunctionQuery(string nameParameter, bool readSqlSecurity)
        => $"""
            SELECT
                fn.RDB$FUNCTION_SOURCE,
                COALESCE(fn.RDB$DETERMINISTIC_FLAG, 0),
                {sqlSecurityColumn("fn", readSqlSecurity)},
                rdb$get_context('SYSTEM', 'ENGINE_VERSION'),
                IIF(a.RDB$ARGUMENT_POSITION = fn.RDB$RETURN_ARGUMENT, 'R', 'I'),
            {string.Format(CultureInfo.InvariantCulture, ParameterColumns, "a", "ARGUMENT")}
            FROM RDB$FUNCTIONS fn
            LEFT JOIN RDB$FUNCTION_ARGUMENTS a
                ON a.RDB$FUNCTION_NAME = fn.RDB$FUNCTION_NAME AND a.RDB$PACKAGE_NAME IS NULL
            {string.Format(CultureInfo.InvariantCulture, ParameterJoins, "a")}
            WHERE fn.RDB$FUNCTION_NAME = {nameParameter} AND fn.RDB$PACKAGE_NAME IS NULL
            ORDER BY a.RDB$ARGUMENT_POSITION
            """;

    /// <summary>
    ///     One query for a procedure: a row per parameter, inputs then outputs, each carrying the
    ///     procedure's source and options -- or one row with no parameter when it has none.
    /// </summary>
    /// <inheritdoc cref="FunctionQuery" />
    public static string ProcedureQuery(string nameParameter, bool readSqlSecurity)
        => $"""
            SELECT
                p.RDB$PROCEDURE_SOURCE,
                0,
                {sqlSecurityColumn("p", readSqlSecurity)},
                rdb$get_context('SYSTEM', 'ENGINE_VERSION'),
                IIF(pp.RDB$PARAMETER_TYPE = 0, 'I', 'O'),
            {string.Format(CultureInfo.InvariantCulture, ParameterColumns, "pp", "PARAMETER")}
            FROM RDB$PROCEDURES p
            LEFT JOIN RDB$PROCEDURE_PARAMETERS pp
                ON pp.RDB$PROCEDURE_NAME = p.RDB$PROCEDURE_NAME AND pp.RDB$PACKAGE_NAME IS NULL
            {string.Format(CultureInfo.InvariantCulture, ParameterJoins, "pp")}
            WHERE p.RDB$PROCEDURE_NAME = {nameParameter} AND p.RDB$PACKAGE_NAME IS NULL
            ORDER BY pp.RDB$PARAMETER_TYPE, pp.RDB$PARAMETER_NUMBER
            """;

    /// <summary>
    ///     <c>RDB$SQL_SECURITY</c> arrived in Firebird 4, so it is named only when the builder knows it is
    ///     talking to Firebird 4 or later. Against anything else a routine's SQL SECURITY reads as unknown
    ///     and is not compared.
    /// </summary>
    public static bool ReadsSqlSecurity(DbCommandBuilder builder)
    {
        var version = builder is FirebirdDbCommandBuilder firebird
            ? firebird.ServerVersion
            : FirebirdServerVersion.Of(builder.Command.Connection);

        return version is { Major: >= 4 };
    }

    /// <summary>
    ///     <c>RDB$SQL_SECURITY</c> as the clause spells it, <c>NONE</c> when the routine does not say, or
    ///     null when the column was not read.
    /// </summary>
    private static string sqlSecurityColumn(string alias, bool read)
        => read
            ? $"CASE {alias}.RDB$SQL_SECURITY WHEN TRUE THEN 'DEFINER' WHEN FALSE THEN 'INVOKER' ELSE 'NONE' END"
            : "CAST(NULL AS VARCHAR(7))";

    /// <summary>
    ///     Read the routine <see cref="FunctionQuery" /> or <see cref="ProcedureQuery" /> selected, or null
    ///     when it does not exist.
    /// </summary>
    public static async Task<PsqlRoutine?> ReadAsync(DbDataReader reader, PsqlRoutineKind kind, string catalogName,
        CancellationToken ct)
    {
        PsqlRoutine? routine = null;

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            routine ??= new PsqlRoutine(kind, catalogName)
            {
                Body = text(reader, 0)?.Trim() ?? string.Empty,
                Deterministic = int32(reader, 1) == 1,
                SqlSecurity = text(reader, 2)?.Trim() is { } security && security != "NONE" ? security : null,
                SqlSecurityKnown = !await reader.IsDBNullAsync(2, ct).ConfigureAwait(false),
                Version = FirebirdServerVersion.TryParse(text(reader, 3))
            };

            if (await reader.IsDBNullAsync(FirstParameterColumn + 1, ct).ConfigureAwait(false))
            {
                // A procedure with no parameters at all: the one row the outer join still produces.
                continue;
            }

            var parameter = readParameter(reader, FirstParameterColumn);
            switch (text(reader, 4))
            {
                case "R":
                    routine.Returns = parameter with { Name = null };
                    break;
                case "O":
                    routine.Outputs.Add(parameter);
                    break;
                default:
                    routine.Inputs.Add(parameter);
                    break;
            }
        }

        if (routine != null)
        {
            routine.Statement = routine.render();
        }

        return routine;
    }

    /// <summary>
    ///     A parameter row. The type comes from the column or domain it names, when it names one --
    ///     <c>TYPE OF COLUMN</c> fills in the relation and field, and a domain is any field source outside
    ///     the <c>RDB$</c> names Firebird generates for an explicit type -- and otherwise from the field
    ///     Firebird made for it.
    /// </summary>
    private static PsqlParameter readParameter(DbDataReader reader, int first)
    {
        var name = text(reader, first);
        var fieldSource = text(reader, first + 1) ?? string.Empty;
        var mechanism = int32(reader, first + 2) ?? 0;
        var relation = text(reader, first + 5);
        var column = text(reader, first + 6);

        var kind = relation != null && column != null
            ? PsqlTypeKind.TypeOfColumn
            : !fieldSource.StartsWith("RDB$", StringComparison.Ordinal)
                ? mechanism == 1 ? PsqlTypeKind.TypeOfDomain : PsqlTypeKind.Domain
                : PsqlTypeKind.DataType;

        var dataType = kind == PsqlTypeKind.DataType
            ? FirebirdColumnType.FromCatalog(
                int32(reader, first + 7) ?? 0,
                int32(reader, first + 8),
                int32(reader, first + 9),
                int32(reader, first + 10),
                int32(reader, first + 11),
                text(reader, first + 12),
                null)
            : new FirebirdColumnType(fieldSource);

        return new PsqlParameter(
            name,
            kind,
            dataType,
            kind is PsqlTypeKind.Domain or PsqlTypeKind.TypeOfDomain ? fieldSource : null,
            kind == PsqlTypeKind.TypeOfColumn ? relation : null,
            kind == PsqlTypeKind.TypeOfColumn ? column : null,
            int32(reader, first + 3) == 1,
            int32(reader, first + 13) is > 0 ? text(reader, first + 14) : null,
            readDefault(text(reader, first + 4)));
    }

    /// <summary>
    ///     The catalog keeps a parameter's default as it was written, <c>= 1</c> or <c>DEFAULT 1</c>;
    ///     this is the expression alone. Unlike a column's, a parameter's <c>DEFAULT NULL</c> is a default:
    ///     it makes the parameter optional.
    /// </summary>
    private static string? readDefault(string? source)
    {
        if (source.IsEmpty())
        {
            return null;
        }

        var tokens = PsqlLexer.Tokenize(source!);
        if (tokens.Count == 0)
        {
            return null;
        }

        var first = tokens[0];
        return first.IsSymbol(source!, '=') || first.IsWord(source!, "DEFAULT")
            ? source![first.End..].Trim()
            : source!.Trim();
    }

    /// <summary>
    ///     How this routine, the model's, differs from <paramref name="actual" />, the catalog's; empty
    ///     when it does not.
    /// </summary>
    public List<string> DifferencesFrom(PsqlRoutine actual)
    {
        var differences = new List<string>();

        compareParameters("parameter", Inputs, actual.Inputs, differences);
        compareParameters("output parameter", Outputs, actual.Outputs, differences);

        if (Returns != null && actual.Returns != null && !Returns.IsSatisfiedBy(actual.Returns))
        {
            differences.Add($"returns {Returns.ToDeclaration()} rather than {actual.Returns.ToDeclaration()}");
        }

        if (Deterministic != actual.Deterministic)
        {
            differences.Add(Deterministic ? "is DETERMINISTIC" : "is not DETERMINISTIC");
        }

        if (actual.SqlSecurityKnown && !string.Equals(SqlSecurity, actual.SqlSecurity, StringComparison.OrdinalIgnoreCase))
        {
            differences.Add($"SQL SECURITY {SqlSecurity ?? "unstated"} rather than {actual.SqlSecurity ?? "unstated"}");
        }

        if (PsqlLexer.NormalizeSource(Body) != PsqlLexer.NormalizeSource(actual.Body))
        {
            differences.Add("the body differs");
        }

        return differences;
    }

    private static void compareParameters(string what, List<PsqlParameter> expected, List<PsqlParameter> actual,
        List<string> differences)
    {
        for (var i = 0; i < Math.Max(expected.Count, actual.Count); i++)
        {
            if (i >= actual.Count)
            {
                differences.Add($"{what} {expected[i].ToDeclaration()} is missing");
            }
            else if (i >= expected.Count)
            {
                differences.Add($"{what} {actual[i].ToDeclaration()} is extra");
            }
            else if (!expected[i].IsSatisfiedBy(actual[i]))
            {
                differences.Add($"{what} {i + 1} is {expected[i].ToDeclaration()} rather than {actual[i].ToDeclaration()}");
            }
        }
    }

    /// <summary>
    ///     The statement that recreates a routine read out of the catalog, names delimited so they come back
    ///     exactly.
    /// </summary>
    private string render()
    {
        var builder = new StringBuilder();
        builder.Append("CREATE OR ALTER ").Append(KindKeyword).Append(' ')
            .Append(FirebirdIdentifierRules.Instance.Delimit(CatalogName));

        if (Inputs.Count > 0)
        {
            builder.Append(" (").Append(Inputs.Select(x => x.ToDeclaration()).Join(", ")).Append(')');
        }

        if (Returns != null)
        {
            builder.Append(" RETURNS ").Append(Returns.ToDeclaration());
        }

        if (Outputs.Count > 0)
        {
            builder.Append(" RETURNS (").Append(Outputs.Select(x => x.ToDeclaration()).Join(", ")).Append(')');
        }

        if (Deterministic)
        {
            builder.Append(" DETERMINISTIC");
        }

        if (SqlSecurity != null)
        {
            builder.Append(" SQL SECURITY ").Append(SqlSecurity);
        }

        builder.Append(" AS").Append('\n').Append(Body);

        return builder.ToString();
    }

    private static string? text(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static int? int32(DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    /// <summary>
    ///     Walks the tokens of a routine's header.
    /// </summary>
    private sealed class HeaderReader
    {
        private readonly PsqlRoutineKind _kind;
        private readonly string _sql;
        private readonly List<PsqlToken> _tokens;
        private int _position;

        public HeaderReader(string sql, List<PsqlToken> tokens, PsqlRoutineKind kind)
        {
            _sql = sql;
            _tokens = tokens;
            _kind = kind;
        }

        private string KindKeyword => _kind == PsqlRoutineKind.Function ? "FUNCTION" : "PROCEDURE";

        public PsqlRoutine Read(FirebirdServerVersion? version)
        {
            skipVerb();

            var kindToken = expectWord(KindKeyword);
            var name = next();
            if (!name.IsIdentifier)
            {
                throw invalid($"a {KindKeyword.ToLowerInvariant()} name after {KindKeyword}");
            }

            var routine = new PsqlRoutine(_kind, PsqlLexer.CatalogIdentifier(_sql, name));

            if (peekSymbol('('))
            {
                routine.Inputs.AddRange(readParameterList(version, named: true));
            }

            if (peekWord("RETURNS"))
            {
                _position++;
                if (_kind == PsqlRoutineKind.Function)
                {
                    routine.Returns = readReturnType(version);
                }
                else
                {
                    if (!peekSymbol('('))
                    {
                        throw invalid("'(' after RETURNS");
                    }

                    routine.Outputs.AddRange(readParameterList(version, named: true));
                }
            }
            else if (_kind == PsqlRoutineKind.Function)
            {
                throw invalid("RETURNS");
            }

            readOptions(routine);

            if (peekWord("EXTERNAL"))
            {
                throw new NotSupportedException(
                    $"{KindKeyword} {routine.CatalogName} is external. Weasel models PSQL routines, whose body "
                    + "Firebird keeps as source; an external routine's body is in a UDR library it cannot compare.");
            }

            var @as = expectWord("AS");

            routine.Body = _sql[@as.End..].Trim();
            routine.Statement = $"CREATE OR ALTER {KindKeyword}{_sql[kindToken.End..]}";

            return routine;
        }

        private void skipVerb()
        {
            if (peekWord("CREATE"))
            {
                _position++;
                if (peekWord("OR"))
                {
                    _position++;
                    expectWord("ALTER");
                }
            }
            else if (peekWord("RECREATE") || peekWord("ALTER"))
            {
                _position++;
            }
            else
            {
                throw invalid($"CREATE [OR ALTER] {KindKeyword}");
            }
        }

        private void readOptions(PsqlRoutine routine)
        {
            while (true)
            {
                if (peekWord("DETERMINISTIC"))
                {
                    _position++;
                    routine.Deterministic = true;
                }
                else if (peekWord("SQL"))
                {
                    _position++;
                    expectWord("SECURITY");
                    var security = next();
                    if (!security.IsWord(_sql, "DEFINER") && !security.IsWord(_sql, "INVOKER"))
                    {
                        throw invalid("DEFINER or INVOKER after SQL SECURITY");
                    }

                    routine.SqlSecurity = security.Text(_sql).ToUpperInvariant();
                }
                else
                {
                    return;
                }
            }
        }

        /// <summary>
        ///     A parenthesised, comma-separated list of declarations, the cursor on its <c>(</c>.
        /// </summary>
        private List<PsqlParameter> readParameterList(FirebirdServerVersion? version, bool named)
        {
            var parameters = new List<PsqlParameter>();
            _position++;

            var start = _position;
            var depth = 0;

            while (true)
            {
                if (_position >= _tokens.Count)
                {
                    throw invalid("')' to close the parameter list");
                }

                var token = _tokens[_position];
                if (token.IsSymbol(_sql, '('))
                {
                    depth++;
                }
                else if (token.IsSymbol(_sql, ')') && depth > 0)
                {
                    depth--;
                }
                else if (depth == 0 && (token.IsSymbol(_sql, ',') || token.IsSymbol(_sql, ')')))
                {
                    if (_position > start)
                    {
                        parameters.Add(readDeclaration(start, _position, version, named));
                    }

                    _position++;
                    if (token.IsSymbol(_sql, ')'))
                    {
                        return parameters;
                    }

                    start = _position;
                    continue;
                }

                _position++;
            }
        }

        /// <summary>
        ///     A function's return type: a declaration without a name, running up to its options or
        ///     <c>AS</c>.
        /// </summary>
        private PsqlParameter readReturnType(FirebirdServerVersion? version)
        {
            var start = _position;
            var depth = 0;

            while (_position < _tokens.Count)
            {
                var token = _tokens[_position];
                if (token.IsSymbol(_sql, '('))
                {
                    depth++;
                }
                else if (token.IsSymbol(_sql, ')'))
                {
                    depth--;
                }
                else if (depth == 0 && (token.IsWord(_sql, "DETERMINISTIC") || token.IsWord(_sql, "AS")
                                        || token.IsWord(_sql, "EXTERNAL")
                                        || (token.IsWord(_sql, "SQL") && _position + 1 < _tokens.Count
                                                                     && _tokens[_position + 1].IsWord(_sql, "SECURITY"))))
                {
                    break;
                }

                _position++;
            }

            if (_position == start)
            {
                throw invalid("a type after RETURNS");
            }

            return readDeclaration(start, _position, version, named: false);
        }

        /// <summary>
        ///     <c>[name] type [NOT NULL] [COLLATE collation] [{= | DEFAULT} value]</c>, from token
        ///     <paramref name="start" /> up to but not including <paramref name="end" />.
        /// </summary>
        private PsqlParameter readDeclaration(int start, int end, FirebirdServerVersion? version, bool named)
        {
            var position = start;
            string? name = null;

            if (named)
            {
                if (!_tokens[position].IsIdentifier)
                {
                    throw invalid("a parameter name", position);
                }

                name = PsqlLexer.CatalogIdentifier(_sql, _tokens[position]);
                position++;
            }

            var typeStart = position;
            var depth = 0;
            while (position < end)
            {
                var token = _tokens[position];
                if (token.IsSymbol(_sql, '('))
                {
                    depth++;
                }
                else if (token.IsSymbol(_sql, ')'))
                {
                    depth--;
                }
                else if (depth == 0 && isClause(position, end))
                {
                    break;
                }

                position++;
            }

            if (position == typeStart)
            {
                throw invalid("a type", typeStart);
            }

            var typeEnd = position;

            var notNull = false;
            string? collation = null;
            string? defaultValue = null;

            while (position < end)
            {
                var token = _tokens[position];
                if (token.IsWord(_sql, "NOT") && position + 1 < end && _tokens[position + 1].IsWord(_sql, "NULL"))
                {
                    notNull = true;
                    position += 2;
                }
                else if (token.IsWord(_sql, "COLLATE") && position + 1 < end)
                {
                    collation = PsqlLexer.CatalogIdentifier(_sql, _tokens[position + 1]);
                    position += 2;
                }
                else if (token.IsWord(_sql, "DEFAULT") || token.IsSymbol(_sql, '='))
                {
                    defaultValue = _sql[token.End.._tokens[end - 1].End].Trim();
                    break;
                }
                else
                {
                    throw invalid("NOT NULL, COLLATE or DEFAULT", position);
                }
            }

            return readType(typeStart, typeEnd, version) with
            {
                Name = name,
                NotNull = notNull,
                Collation = collation,
                Default = defaultValue
            };
        }

        private bool isClause(int position, int end)
        {
            var token = _tokens[position];
            return (token.IsWord(_sql, "NOT") && position + 1 < end && _tokens[position + 1].IsWord(_sql, "NULL"))
                   || token.IsWord(_sql, "COLLATE")
                   || token.IsWord(_sql, "DEFAULT")
                   || token.IsSymbol(_sql, '=');
        }

        private PsqlParameter readType(int start, int end, FirebirdServerVersion? version)
        {
            var first = _tokens[start];

            if (first.IsWord(_sql, "TYPE") && end - start >= 3 && _tokens[start + 1].IsWord(_sql, "OF"))
            {
                if (_tokens[start + 2].IsWord(_sql, "COLUMN"))
                {
                    if (end - start != 6 || !_tokens[start + 4].IsSymbol(_sql, '.'))
                    {
                        throw invalid("TYPE OF COLUMN relation.column", start);
                    }

                    return parameter(PsqlTypeKind.TypeOfColumn) with
                    {
                        Relation = PsqlLexer.CatalogIdentifier(_sql, _tokens[start + 3]),
                        Column = PsqlLexer.CatalogIdentifier(_sql, _tokens[start + 5])
                    };
                }

                return parameter(PsqlTypeKind.TypeOfDomain) with
                {
                    Domain = PsqlLexer.CatalogIdentifier(_sql, _tokens[start + 2])
                };
            }

            if (end - start == 1 && first.Kind == PsqlTokenKind.DelimitedIdentifier)
            {
                return parameter(PsqlTypeKind.Domain) with { Domain = PsqlLexer.CatalogIdentifier(_sql, first) };
            }

            var type = FirebirdColumnType.Parse(_sql[first.Start.._tokens[end - 1].End], version);
            if (end - start == 1 && !DataTypeNames.Contains(type.Name))
            {
                return parameter(PsqlTypeKind.Domain) with { Domain = PsqlLexer.CatalogIdentifier(_sql, first) };
            }

            return parameter(PsqlTypeKind.DataType) with { DataType = type with { Collation = null } };
        }

        private static PsqlParameter parameter(PsqlTypeKind kind)
            => new(null, kind, new FirebirdColumnType(string.Empty), null, null, null, false, null, null);

        private PsqlToken next()
        {
            if (_position >= _tokens.Count)
            {
                throw invalid("more of the statement");
            }

            return _tokens[_position++];
        }

        private PsqlToken expectWord(string word)
        {
            if (!peekWord(word))
            {
                throw invalid(word);
            }

            return _tokens[_position++];
        }

        private bool peekWord(string word) => _position < _tokens.Count && _tokens[_position].IsWord(_sql, word);

        private bool peekSymbol(char symbol) => _position < _tokens.Count && _tokens[_position].IsSymbol(_sql, symbol);

        private ArgumentException invalid(string expected, int? at = null)
        {
            var position = at ?? _position;
            var found = position < _tokens.Count ? $"'{_tokens[position].Text(_sql)}'" : "the end of the statement";

            return new ArgumentException(
                $"Expected {expected} but found {found} in the PSQL {KindKeyword.ToLowerInvariant()} statement: {_sql}");
        }
    }
}
