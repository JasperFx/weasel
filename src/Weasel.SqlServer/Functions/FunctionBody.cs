using Weasel.Core;

namespace Weasel.SqlServer.Functions;

public class FunctionBody
{
    public FunctionBody(DbObjectName identifier, string[] dropStatements, string body)
    {
        Identifier = identifier;
        DropStatements = dropStatements;
        Body = body;
    }

    public DbObjectName Identifier { get; set; }
    public string[] DropStatements { get; set; }
    public string Body { get; set; }
}
