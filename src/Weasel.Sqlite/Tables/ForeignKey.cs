using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Sqlite.Tables;

public class MisconfiguredForeignKeyException: Exception
{
    public MisconfiguredForeignKeyException(string? message): base(message)
    {
    }
}

public class ForeignKey: INamed
{
    private string[] _columnNames = null!;
    private string[] _linkedNames = null!;

    public ForeignKey(string name)
    {
        Name = name;
    }

    public string[] ColumnNames
    {
        get => _columnNames;
        set => _columnNames = value.OrderBy(x => x).ToArray();
    }

    public string[] LinkedNames
    {
        get => _linkedNames;
        set => _linkedNames = value.OrderBy(x => x).ToArray();
    }

    public DbObjectName LinkedTable { get; set; } = null!;

    public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;
    public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

    public string Name { get; set; }

    protected bool Equals(ForeignKey other)
    {
        return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) &&
               ColumnNames.SequenceEqual(other.ColumnNames, StringComparer.OrdinalIgnoreCase) &&
               LinkedNames.SequenceEqual(other.LinkedNames, StringComparer.OrdinalIgnoreCase) &&
               Equals(LinkedTable, other.LinkedTable) &&
               OnDelete == other.OnDelete && OnUpdate == other.OnUpdate;
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        if (!obj.GetType().CanBeCastTo<ForeignKey>())
        {
            return false;
        }

        return Equals((ForeignKey)obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = Name != null ? Name.ToLowerInvariant().GetHashCode() : 0;
            hashCode = (hashCode * 397) ^ (ColumnNames != null ? ColumnNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedNames != null ? LinkedNames.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (LinkedTable != null ? LinkedTable.GetHashCode() : 0);
            hashCode = (hashCode * 397) ^ (int)OnDelete;
            hashCode = (hashCode * 397) ^ (int)OnUpdate;
            return hashCode;
        }
    }

    /// <summary>
    ///     Read the DDL definition from the server
    /// </summary>
    /// <param name="definition"></param>
    public void Parse(string definition)
    {
        var open1 = definition.IndexOf('(');
        var closed1 = definition.IndexOf(')');

        ColumnNames = definition.Substring(open1 + 1, closed1 - open1 - 1).ToDelimitedArray(',');

        var open2 = definition.IndexOf('(', closed1);
        var closed2 = definition.IndexOf(')', open2);

        LinkedNames = definition.Substring(open2 + 1, closed2 - open2 - 1).ToDelimitedArray(',');

        var references = "REFERENCES";
        var tableStart = definition.IndexOf(references, StringComparison.OrdinalIgnoreCase) + references.Length;

        var tableName = definition.Substring(tableStart, open2 - tableStart).Trim();
        LinkedTable = DbObjectName.Parse(SqliteProvider.Instance, tableName);

        // Parse ON DELETE
        if (definition.ContainsIgnoreCase("ON DELETE CASCADE"))
        {
            OnDelete = CascadeAction.Cascade;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE SET NULL"))
        {
            OnDelete = CascadeAction.SetNull;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE SET DEFAULT"))
        {
            OnDelete = CascadeAction.SetDefault;
        }
        else if (definition.ContainsIgnoreCase("ON DELETE RESTRICT"))
        {
            OnDelete = CascadeAction.Restrict;
        }

        // Parse ON UPDATE
        if (definition.ContainsIgnoreCase("ON UPDATE CASCADE"))
        {
            OnUpdate = CascadeAction.Cascade;
        }
        else if (definition.ContainsIgnoreCase("ON UPDATE SET NULL"))
        {
            OnUpdate = CascadeAction.SetNull;
        }
        else if (definition.ContainsIgnoreCase("ON UPDATE SET DEFAULT"))
        {
            OnUpdate = CascadeAction.SetDefault;
        }
        else if (definition.ContainsIgnoreCase("ON UPDATE RESTRICT"))
        {
            OnUpdate = CascadeAction.Restrict;
        }
    }

    public string ToDDL(Table parent)
    {
        var writer = new StringWriter();
        WriteInlineDefinition(writer);
        return writer.ToString();
    }

    /// <summary>
    /// SQLite foreign keys are typically defined inline with CREATE TABLE
    /// This returns the FOREIGN KEY constraint for use in CREATE TABLE statement
    /// </summary>
    public void WriteInlineDefinition(TextWriter writer)
    {
        writer.Write($"CONSTRAINT {SchemaUtils.QuoteName(Name)} FOREIGN KEY ({ColumnNames.Select(SchemaUtils.QuoteName).Join(", ")})");
        writer.Write($" REFERENCES {LinkedTable.QualifiedName} ({LinkedNames.Select(SchemaUtils.QuoteName).Join(", ")})");

        if (OnDelete != CascadeAction.NoAction)
        {
            writer.WriteCascadeAction("ON DELETE", OnDelete);
        }

        if (OnUpdate != CascadeAction.NoAction)
        {
            writer.WriteCascadeAction("ON UPDATE", OnUpdate);
        }
    }

    /// <summary>
    /// SQLite does not support ALTER TABLE ADD CONSTRAINT for foreign keys.
    /// Foreign keys must be defined at table creation time.
    /// </summary>
    public void WriteAddStatement(Table parent, TextWriter writer)
    {
        throw new NotSupportedException(
            "SQLite does not support ALTER TABLE ADD CONSTRAINT for foreign keys. " +
            "Foreign keys must be defined when the table is created. " +
            $"Table '{parent.Identifier}' must be recreated to add foreign key '{Name}'.");
    }

    /// <summary>
    /// SQLite does not support ALTER TABLE DROP CONSTRAINT.
    /// The table must be recreated without the foreign key.
    /// </summary>
    public void WriteDropStatement(Table parent, TextWriter writer)
    {
        throw new NotSupportedException(
            "SQLite does not support ALTER TABLE DROP CONSTRAINT for foreign keys. " +
            $"Table '{parent.Identifier}' must be recreated to remove foreign key '{Name}'.");
    }

    public void LinkColumns(string columnName, string referencedName)
    {
        if (ColumnNames == null)
        {
            ColumnNames = new[] { columnName };
            LinkedNames = new[] { referencedName };
        }
        else
        {
            ColumnNames = ColumnNames.Append(columnName).ToArray();
            LinkedNames = LinkedNames.Append(referencedName).ToArray();
        }
    }

    public void ReadReferentialActions(string? onDelete, string? onUpdate = null)
    {
        if (onDelete != null)
        {
            OnDelete = SqliteProvider.ReadAction(onDelete);
        }

        if (onUpdate != null)
        {
            OnUpdate = SqliteProvider.ReadAction(onUpdate);
        }
    }
}
