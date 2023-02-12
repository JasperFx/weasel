﻿using System.Data;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

public class SqlServerProviderTests
{
    [Fact]
    public void execute_to_db_type_as_int()
    {
        SqlServerProvider.Instance.ToParameterType(typeof(int)).ShouldBe(SqlDbType.Int);
        SqlServerProvider.Instance.ToParameterType(typeof(int?)).ShouldBe(SqlDbType.Int);
    }


    public class MappedTarget
    {
    }

    public class UnmappedTarget
    {
    }

    [Fact]
    public void canonicizesql_supports_tabs_as_whitespace()
    {
        var noTabsCanonized =
            "\r\nDECLARE\r\n  final_version uuid;\r\nBEGIN\r\nINSERT INTO table(\"data\", \"mt_dotnet_type\", \"id\", \"mt_version\", mt_last_modified) VALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp())\r\n  ON CONFLICT ON CONSTRAINT pk_table\r\n  DO UPDATE SET \"data\" = doc, \"mt_dotnet_type\" = docDotNetType, \"mt_version\" = docVersion, mt_last_modified = transaction_timestamp();\r\n\r\n  SELECT mt_version FROM table into final_version WHERE id = docId;\r\n  RETURN final_version;\r\nEND;\r\n"
                .CanonicizeSql();
        var tabsCanonized =
            "\r\nDECLARE\r\n\tfinal_version uuid;\r\nBEGIN\r\n\tINSERT INTO table(\"data\", \"mt_dotnet_type\", \"id\", \"mt_version\", mt_last_modified)\r\n\tVALUES (doc, docDotNetType, docId, docVersion, transaction_timestamp())\r\n\t\tON CONFLICT ON CONSTRAINT pk_table\r\n\t\t\tDO UPDATE SET \"data\" = doc, \"mt_dotnet_type\" = docDotNetType, \"mt_version\" = docVersion, mt_last_modified = transaction_timestamp();\r\n\r\n\tSELECT mt_version FROM table into final_version WHERE id = docId;\r\n\r\n\tRETURN final_version;\r\nEND;\r\n"
                .CanonicizeSql();
        noTabsCanonized.ShouldBe(tabsCanonized);
    }

    [Fact]
    public void replaces_multiple_spaces_with_new_string()
    {
        var inputString = "Darth        Maroon the   First";
        var expectedString = "Darth Maroon the First";
        inputString.ReplaceMultiSpace(" ").ShouldBe(expectedString);
    }
}
