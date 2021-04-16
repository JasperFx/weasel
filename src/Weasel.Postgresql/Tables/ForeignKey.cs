using System;
using System.Collections.Generic;
using System.Text;
using Baseline;

namespace Weasel.Postgresql.Tables
{
    public enum CascadeAction
    {
        SetNull,
        SetDefault,
        Restrict,
        NoAction,
        Cascade
    }

    public class MisconfiguredForeignKeyException : Exception
    {
        public MisconfiguredForeignKeyException(string? message) : base(message)
        {
        }
    }

    public class ForeignKey
    {
        public ForeignKey(string keyName)
        {
            KeyName = keyName;
        }

        public string KeyName { get; set; }
        public string[] ColumnNames { get; set; }
        public string[] LinkedNames { get; set; } 
        
        public DbObjectName LinkedTable { get; set; }

        public CascadeAction OnDelete { get; set; } = CascadeAction.NoAction;
        public CascadeAction OnUpdate { get; set; } = CascadeAction.NoAction;

        public string ToDDL(Table parent)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"ALTER TABLE {parent.Identifier}");
            sb.AppendLine($"ADD CONSTRAINT {KeyName} FOREIGN KEY({ColumnNames.Join(", ")})");
            sb.Append($"REFERENCES {LinkedTable}({LinkedNames.Join(", ")})");
            sb.WriteCascadeAction("ON DELETE", OnDelete);
            sb.WriteCascadeAction("ON UPDATE", OnUpdate);

            sb.Append(";");
            return sb.ToString();
        }
    }

    internal static class StringBuilderExtensions
    {
        public static void WriteCascadeAction(this StringBuilder sb, string prefix, CascadeAction action)
        {
            switch (action)
            {
                case CascadeAction.Cascade:
                    sb.AppendLine($"{prefix} CASCADE");
                    break;
                case CascadeAction.Restrict:
                    sb.AppendLine($"{prefix} RESTRICT");
                    break;
                case CascadeAction.NoAction:
                    return;
                case CascadeAction.SetDefault:
                    sb.AppendLine($"{prefix} SET DEFAULT");
                    break;
                case CascadeAction.SetNull:
                    sb.AppendLine($"{prefix} SET NULL");
                    break;
            }
        }
    }
}
