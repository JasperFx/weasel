using System.Data;
using Microsoft.Data.SqlClient;
using Weasel.Core;

namespace Weasel.SqlServer;

public class CommandBuilder: CommandBuilderBase<SqlCommand, SqlParameter, SqlConnection, SqlTransaction, SqlDbType,
    SqlDataReader>
{
    public CommandBuilder(): this(new SqlCommand())
    {
    }

    public CommandBuilder(SqlCommand command): base(SqlServerProvider.Instance, ':', command)
    {
    }
}
