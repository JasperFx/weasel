using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Tables
{
    public partial class Table
    {
        internal async Task<TableDelta> FindDelta(NpgsqlConnection conn)
        {
            var actual = await FetchExisting(conn);
            return new TableDelta(this, actual);
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var existing = await readExisting(reader);
            return new TableDelta(this, existing);
        }

        public async Task<SchemaPatchDifference> CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate)
        {
            var existing = await readExisting(reader);
            if (existing == null)
            {
                WriteCreateStatement(patch.Rules, patch.UpWriter);
                patch.Rollbacks.Drop(this, Identifier);
            
                return SchemaPatchDifference.Create;
            }

            throw new NotImplementedException("Getting rid of this anyway");
            // var delta = new TableDelta(this, existing);
            // var difference = delta.determinePatchDifference();
            //
            // if (autoCreate == AutoCreate.None) return difference;
            //
            // switch (difference)
            // {
            //     case SchemaPatchDifference.None:
            //     case SchemaPatchDifference.Invalid:
            //         return difference;
            //     
            //     case SchemaPatchDifference.Update:
            //         switch (autoCreate)
            //         {
            //             case AutoCreate.All:
            //             case AutoCreate.CreateOrUpdate:
            //                 // write out all columns
            //                 // all index changes
            //                 // all foreign key changes
            //                 // all primary key changes
            //                 break;
            //         }
            //
            //         return SchemaPatchDifference.Update;
            // }  
            

            
            // if (delta.Extras.Any() || delta.Different.Any())
            // {
            //     if (autoCreate == AutoCreate.All)
            //     {
            //         delta.ForeignKeyMissing.Each(x => patch.Updates.Apply(this, x));
            //         delta.ForeignKeyRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            //         delta.ForeignKeyMissingRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            //
            //         Write(patch.Rules, patch.UpWriter);
            //
            //         return SchemaPatchDifference.Create;
            //     }
            //
            //     if (!delta.AlteredColumnTypes.Any())
            //     {
            //         return SchemaPatchDifference.Invalid;
            //     }
            // }
            //
            // if (!delta.Missing.All(x => x.CanAdd))
            // {
            //     return SchemaPatchDifference.Invalid;
            // }
            //
            // foreach (var missing in delta.Missing)
            // {
            //     patch.Updates.Apply(this, missing.AddColumnSql(this));
            //     patch.Rollbacks.RemoveColumn(this, Identifier, missing.Name);
            // }
            //
            // delta.AlteredColumnTypes.Each(x => patch.Updates.Apply(this, x));
            // delta.AlteredColumnTypeRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            //
            // delta.IndexChanges.Each(x => patch.Updates.Apply(this, x));
            // delta.IndexRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            //
            // delta.ForeignKeyChanges.Each(x => patch.Updates.Apply(this, x));
            // delta.ForeignKeyRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            // delta.ForeignKeyMissingRollbacks.Each(x => patch.Rollbacks.Apply(this, x));
            //
            return SchemaPatchDifference.Update;
        }

    }

}