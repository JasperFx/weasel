using System;
using System.Data;
using System.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests
{
    public class SqlServerProviderTests
    {
        [Fact]
        public void execute_to_db_type_as_int()
        {
            SqlServerProvider.Instance.ToParameterType(typeof(int)).ShouldBe(SqlDbType.Int);
            SqlServerProvider.Instance.ToParameterType(typeof(int?)).ShouldBe(SqlDbType.Int);
        }


        [Fact]
        public void execute_get_pg_type_default_mappings_resolve()
        {
            SqlServerProvider.Instance.GetDatabaseType(typeof(long), EnumStorage.AsString).ShouldBe("bigint");
            SqlServerProvider.Instance.GetDatabaseType(typeof(DateTime), EnumStorage.AsString).ShouldBe("timestamp without time zone");
        }




        public class MappedTarget { }
        public class UnmappedTarget { }

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

        [Fact]
        public void table_columns_should_match_raw_types()
        {
            var serialAsInt = new TableColumn("id", "serial");
            serialAsInt.ShouldBe(new TableColumn("id", "int"));

            var varchararrAsArray = new TableColumn("comments", "varchar[]");
            varchararrAsArray.ShouldBe(new TableColumn("comments", "array"));

            var charactervaryingAsArray = new TableColumn("comments", "character varying[]");
            charactervaryingAsArray.ShouldBe(new TableColumn("comments", "array"));

            var textarrayAsArray = new TableColumn("comments", "text[]");
            charactervaryingAsArray.ShouldBe(new TableColumn("comments", "array"));
        }

        [Theory]
        [InlineData("character varying", "varchar")]
        [InlineData("varchar", "varchar")]
        [InlineData("boolean", "boolean")]
        [InlineData("bool", "boolean")]
        [InlineData("integer", "int")]
        [InlineData("serial", "int")]
        [InlineData("integer[]", "int[]")]
        [InlineData("decimal", "decimal")]
        [InlineData("numeric", "decimal")]
        [InlineData("timestamp without time zone", "timestamp")]
        [InlineData("timestamp with time zone", "timestamptz")]
        [InlineData("array", "array")]
        [InlineData("character varying[]", "array")]
        [InlineData("varchar[]", "array")]
        [InlineData("text[]", "array")]
        public void convert_synonyms(string type, string synonym)
        {
            SqlServerProvider.Instance.ConvertSynonyms(type).ShouldBe(synonym);
        }
    }
}
