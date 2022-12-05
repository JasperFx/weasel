using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Postgresql.Functions;

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

    public string ToOwnershipCommand(string owner)
    {
        return $"ALTER FUNCTION {Function.ParseSignature(Body)} OWNER TO \"{owner}\";";
    }

    public string BuildTemplate(string template)
    {
        return template
                .Replace(Migrator.SCHEMA, Identifier.Schema)
                .Replace(Migrator.FUNCTION, Identifier.Name)
                .Replace(Migrator.SIGNATURE, Function.ParseSignature(Body))
            ;
    }

    public void WriteTemplate(SqlTemplate template, StringWriter writer)
    {
        var text = template?.FunctionCreation;
        if (text.IsNotEmpty())
        {
            writer.WriteLine(BuildTemplate(text));
        }
    }
}
