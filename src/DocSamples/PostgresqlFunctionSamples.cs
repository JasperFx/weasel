using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Functions;

namespace DocSamples;

public class PostgresqlFunctionSamples
{
    public void create_function_from_sql()
    {
        #region sample_pg_create_function_from_sql
        var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
    SELECT amount * rate;
$$;
");
        #endregion
    }

    public void create_function_with_identifier()
    {
        #region sample_pg_create_function_with_identifier
        var function = new Function(
            new DbObjectName("public", "calculate_tax"),
            @"CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
    SELECT amount * rate;
$$;"
        );
        #endregion
    }

    public async Task function_delta_detection()
    {
        #region sample_pg_function_delta_detection
        var dataSource = new NpgsqlDataSourceBuilder("Host=localhost;Database=mydb").Build();
        var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
    SELECT amount * rate;
$$;
");

        await using var conn = dataSource.CreateConnection();
        await conn.OpenAsync();

        // Fetch the existing function from the database
        var existing = await function.FetchExistingAsync(conn);

        // Compute the delta
        var delta = await function.FindDeltaAsync(conn);
        // delta.Difference: None, Create, or Update
        #endregion
    }

    public void function_for_removal()
    {
        #region sample_pg_function_for_removal
        var removed = Function.ForRemoval("public.old_function");
        #endregion
    }

    public void function_generate_ddl()
    {
        #region sample_pg_function_generate_ddl
        var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
    SELECT amount * rate;
$$;
");

        var migrator = new PostgresqlMigrator();
        var writer = new StringWriter();

        // CREATE FUNCTION statement
        function.WriteCreateStatement(migrator, writer);

        // DROP FUNCTION statement
        function.WriteDropStatement(migrator, writer);
        #endregion
    }

    public void function_build_template()
    {
        #region sample_pg_function_build_template
        var function = Function.ForSql(@"
CREATE OR REPLACE FUNCTION public.calculate_tax(amount decimal, rate decimal)
RETURNS decimal
LANGUAGE sql
AS $$
    SELECT amount * rate;
$$;
");

        string result = function.BuildTemplate(
            "GRANT EXECUTE ON FUNCTION {SIGNATURE} TO app_user;");
        #endregion
    }
}
