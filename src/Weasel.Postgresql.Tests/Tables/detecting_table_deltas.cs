using Shouldly;
using JasperFx.Core.Reflection;
using Weasel.Core;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tests.Tables.Indexes;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables;

[Collection("deltas")]
public class detecting_table_deltas(): IndexDeltasDetectionContext("deltas")
{
    [Fact]
    public async Task detect_all_new_table()
    {
        var table = await theTable.FetchExistingAsync(theConnection);
        table.ShouldBeNull();

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task no_delta()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task missing_column()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<DateTimeOffset>("birth_day");
        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Columns.Missing.Single().Name.ShouldBe("birth_day");

        delta.Columns.Extras.Any().ShouldBeFalse();
        delta.Columns.Different.Any().ShouldBeFalse();

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task using_reserved_keywords_for_columns()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<string>("trim").AddIndex();
        theTable.AddColumn<string>("lower");
        theTable.AddColumn<string>("upper");

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task extra_column()
    {
        theTable.AddColumn<DateTime>("birth_day");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.RemoveColumn("birth_day");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Columns.Extras.Single().Name.ShouldBe("birth_day");

        delta.Columns.Missing.Any().ShouldBeFalse();
        delta.Columns.Different.Any().ShouldBeFalse();

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task when_a_table_drops_column_that_was_part_of_the_primary_key()
    {
        theTable.AddColumn<string>("tenant_id").AsPrimaryKey();
        await CreateSchemaObjectInDatabase(theTable);

        theTable.RemoveColumn("tenant_id");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedFact(MinimumVersion = "15.0")]
    public Task detect_new_index_with_distinct_nulls() =>
        detect_new_index(true);

    [Fact]
    public Task detect_new_index_without_distinct_nulls() =>
        detect_new_index(false);

    [Fact]
    public Task detect_new_index_with_concurrent_creation() =>
        detect_new_index(false, true);

    private async Task detect_new_index(bool withDistinctNulls, bool isConcurrent = false)
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsUnique = true;
            i.NullsNotDistinct = withDistinctNulls;
            i.IsConcurrent = isConcurrent;
        });

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Missing.Single()
            .Name.ShouldBe("idx_people_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedFact(MinimumVersion = "15.0")]
    public Task detect_matched_index_with_distinct_nulls() =>
        detect_matched_index(true);

    [Fact]
    public Task detect_matched_index_without_distinct_nulls() =>
        detect_matched_index(false);

    [Fact]
    public Task detect_matched_index_with_concurrent_creation() =>
        detect_matched_index(false, true);

    private async Task detect_matched_index(bool withDistinctNulls, bool isConcurrent = false)
    {
        theTable.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsUnique = true;
            i.NullsNotDistinct = withDistinctNulls;
            i.IsConcurrent = isConcurrent;
        });

        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();

        delta.Indexes.Matched.Single()
            .Name.ShouldBe("idx_people_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [PgVersionTargetedFact(MinimumVersion = "15.0")]
    public Task detect_different_index_with_distinct_nulls() =>
        detect_different_index(true);

    [Fact]
    public Task detect_different_index_without_distinct_nulls() =>
        detect_different_index(false);

    [Fact]
    public Task detect_different_index_with_concurrent_creation() =>
        detect_different_index(false, true);

    private async Task detect_different_index(bool withDistinctNulls, bool isConcurrent = false)
    {
        theTable.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsUnique = true;
            i.NullsNotDistinct = withDistinctNulls;
            i.IsConcurrent = isConcurrent;
        });

        await CreateSchemaObjectInDatabase(theTable);

        var indexDefinition = theTable.Indexes.Single().As<IndexDefinition>();
        indexDefinition
            .Method = IndexMethod.hash;
        indexDefinition.IsUnique = false;

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Different.Single()
            .Expected
            .Name.ShouldBe("idx_people_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }


    [PgVersionTargetedFact(MinimumVersion = "15.0")]
    public Task detect_extra_index_with_distinct_nulls() =>
        detect_extra_index(true);

    [Fact]
    public Task detect_extra_index_without_distinct_nulls() =>
        detect_extra_index(false);

    [Fact]
    public Task detect_extra_index_with_concurrent_creation() =>
        detect_extra_index(false, true);

    private async Task detect_extra_index(bool withDistinctNulls, bool isConcurrent = false)
    {
        theTable.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsUnique = true;
            i.NullsNotDistinct = withDistinctNulls;
            i.IsConcurrent = isConcurrent;
        });
        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Extras.Single().Name
            .ShouldBe("idx_people_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task ignore_new_index_that_is_ignored()
    {
        var existing = new detecting_table_deltas().theTable;
        existing.ModifyColumn("user_name").AddIndex(idx => idx.Name = "ignore_me");

        await CreateSchemaObjectInDatabase(existing);

        theTable.IgnoreIndex("ignore_me");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();

        delta.Difference.ShouldBe(SchemaPatchDifference.None);

        await AssertNoDeltasAfterPatching();
    }

    [PgVersionTargetedTheory(MaximumVersion = "13.0")]
    [MemberData(nameof(ConcurrentIndexTestData))]
    public Task matching_index_ddl_concurrent(string description, Action<Table> configure) =>
        matching_index_ddl(description, configure);

    [PgVersionTargetedTheory(MinimumVersion = "15.0")]
    [MemberData(nameof(UniqueIndexWithNullsNotDistinctTestData))]
    public Task matching_index_ddl_unique_with_null_not_distinct(string description, Action<Table> configure) =>
        matching_index_ddl(description, configure);

    [Theory]
    [MemberData(nameof(IndexTestData))]
    public Task matching_index_ddl_not_concurrent(string description, Action<Table> configure) =>
        matching_index_ddl(description, configure);

    private async Task matching_index_ddl(string description, Action<Table> configure)
    {
        configure(theTable);
        await CreateSchemaObjectInDatabase(theTable);

        var existing = await theTable.FetchExistingAsync(theConnection);

        // The index DDL should match what the database thinks it is in order to match

        var expected = existing!.Indexes.Single();
        var actual = theTable.Indexes.Single();

        expected.AssertMatches(actual, theTable);

        // And no deltas
        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Indexes.Matched.Count.ShouldBe(1);
    }

    public static IEnumerable<object[]> IndexTestData()
    {
        foreach (var (description, action) in IndexConfigs())
        {
            yield return new object[] { description, action };
        }
    }

    public static IEnumerable<object[]> ConcurrentIndexTestData()
    {
        foreach (var (description, action) in ConcurrentIndexConfigs())
        {
            yield return new object[] { description, action };
        }
    }

    public static IEnumerable<object[]> UniqueIndexWithNullsNotDistinctTestData()
    {
        foreach (var (description, action) in IndexConfigs())
        {
            yield return new object[] { description, action };
        }
    }

    private static IEnumerable<(string, Action<Table>)> UniqueIndexConfigsWithNullsNotDistinct()
    {
        yield return ("Simple jsonb property + unique + nulls not distinct", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Columns = new[] { "(data ->> 'Name')" };
            i.IsUnique = true;
            i.NullsNotDistinct = true;
        }));

        yield return ("Jsonb property with function + unique + nulls not distinct", t => t.ModifyColumn("data")
            .AddIndex(i =>
            {
                i.Columns = new[] { "lower(data ->> 'Name')" };
                i.IsUnique = true;
                i.NullsNotDistinct = true;
            }));
        yield return ("Jsonb property with function as expression + nulls not distinct", t => t.ModifyColumn("data")
            .AddIndex(i =>
            {
                i.Mask = "lower(?)";
                i.Columns = new[] { "(data ->> 'Name')" };
                i.IsUnique = true;
                i.NullsNotDistinct = true;
            }));

        yield return ("Jsonb property with cast + unique + nulls not distinct", t => t.ModifyColumn("data")
            .AddIndex(i =>
            {
                i.Columns = new[] { "CAST(data ->> 'SomeGuid' as uuid)" };
                i.IsUnique = true;
                i.NullsNotDistinct = true;
            }));

        yield return ("Jsonb property with multiple casts + unique + nulls not distinct", t => t.ModifyColumn("data")
            .AddIndex(i =>
            {
                i.Columns = new[] { "CAST(data ->> 'SomeGuid' as uuid)", "CAST(data ->> 'OtherGuid' as uuid)" };
                i.IsUnique = true;
                i.NullsNotDistinct = true;
            }));
    }

    private static IEnumerable<(string, Action<Table>)> IndexConfigs()
    {
        yield return ("Simple btree", t => t.ModifyColumn("user_name").AddIndex());
        yield return ("Simple btree with non-zero fill factor",
            t => t.ModifyColumn("user_name").AddIndex(i => i.FillFactor = 50));
        yield return ("Simple btree with expression",
            t => t.ModifyColumn("user_name").AddIndex(i => i.Mask = "(lower(?))"));
        yield return ("Simple btree with expression and predicate", t => t.ModifyColumn("user_name").AddIndex(i =>
        {
            i.Mask = "(lower(?))";
            i.Columns = new[] { "user_name" };
            i.Predicate = "id > 5";
        }));

        yield return ("Simple btree with expression and predicate", t => t.ModifyColumn("user_name").AddIndex(i =>
        {
            i.Mask = "(lower(?))";
            i.Columns = new[] { "user_name" };
            i.Predicate = "user_name is not null";
        }));

        yield return ("Simple btree + desc",
            t => t.ModifyColumn("user_name").AddIndex(i => i.SortOrder = SortOrder.Desc));
        yield return ("btree + unique", t => t.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true));

        yield return ("Simple brin", t => t.ModifyColumn("user_name").AddIndex(i => i.Method = IndexMethod.brin));
        yield return ("Simple gin", t => t.ModifyColumn("data").AddIndex(i => i.Method = IndexMethod.gin));
        yield return ("Simple gist",
            t => t.AddColumn("data2", "tsvector").AddIndex(i => i.Method = IndexMethod.gist));
        yield return ("Simple hash", t => t.ModifyColumn("user_name").AddIndex(i => i.Method = IndexMethod.hash));


        yield return ("Simple jsonb property",
            t => t.ModifyColumn("data").AddIndex(i => i.Columns = new[] { "(data ->> 'Name')" }));
        yield return ("Simple jsonb property + unique", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Columns = new[] { "(data ->> 'Name')" };
            i.IsUnique = true;
        }));

        yield return ("Jsonb property with function",
            t => t.ModifyColumn("data").AddIndex(i => i.Columns = new[] { "lower(data ->> 'Name')" }));
        yield return ("Jsonb property with function + unique", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Columns = new[] { "lower(data ->> 'Name')" };
            i.IsUnique = true;
        }));
        yield return ("Jsonb property with function as expression", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Mask = "lower(?)";
            i.Columns = new[] { "(data ->> 'Name')" };
            i.IsUnique = true;
        }));

        yield return ("Jsonb property with cast",
            t => t.ModifyColumn("data").AddIndex(i => i.Columns = new[] { "CAST(data ->> 'SomeGuid' as uuid)" }));
        yield return ("Jsonb property with cast + unique", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Columns = new[] { "CAST(data ->> 'SomeGuid' as uuid)" };
            i.IsUnique = true;
        }));

        yield return ("Jsonb property with multiple casts + unique", t => t.ModifyColumn("data").AddIndex(i =>
        {
            i.Columns = new[] { "CAST(data ->> 'SomeGuid' as uuid)", "CAST(data ->> 'OtherGuid' as uuid)" };
            i.IsUnique = true;
        }));

        yield return ("Jsonb property with fulll text search on multiple columns", t => t.ModifyColumn("data")
            .AddIndex(i =>
            {
                i.Columns = new[] { "to_tsvector('english',((data ->> 'FirstName') || ' ' || (data ->> 'LastName')))" };
                i.Method = IndexMethod.gin;
            }));
    }

    private static IEnumerable<(string, Action<Table>)> ConcurrentIndexConfigs()
    {
        yield return ("btree + concurrent", t => t.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsConcurrent = true;
        }));

        yield return ("btree + concurrent + unique", t => t.ModifyColumn("user_name").AddIndex(i =>
        {
            i.IsUnique = true;
            i.IsConcurrent = true;
        }));
    }

    [Fact]
    public async Task detect_all_new_foreign_key()
    {
        var states = new Table("deltas.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("deltas.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Missing.Single()
            .ShouldBeSameAs(table.ForeignKeys.Single());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detect_extra_foreign_key()
    {
        var states = new Table("deltas.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);


        var table = new Table("deltas.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        table.ForeignKeys.Clear();

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Extras.Single().Name
            .ShouldBe("fkey_people_state_id");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task match_foreign_key()
    {
        var states = new Table("deltas.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("deltas.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");


        await CreateSchemaObjectInDatabase(table);


        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();

        delta.ForeignKeys.Matched.Single().Name
            .ShouldBe("fkey_people_state_id");

        delta.Difference.ShouldBe(SchemaPatchDifference.None);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task match_foreign_key_with_inherited_foreign_key_class()
    {
        var table = new Table("deltas.foreign_key_test");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name");
        table.AddColumn<int?>("parent_id");

        // Marten uses custom class for foreign keys which is inherited from ForeignKey
        table.ForeignKeys.Add(new ForeignKeyTest("foreign_key_test_parent_id_fkey")
        {
            LinkedTable = table.Identifier, ColumnNames = new[] { "parent_id" }, LinkedNames = new[] { "id" }
        });

        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();
        delta.ForeignKeys.Matched.Single().Name.ShouldBe("foreign_key_test_parent_id_fkey");
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
        await AssertNoDeltasAfterPatching(table);
    }

    private class ForeignKeyTest: ForeignKey
    {
        public ForeignKeyTest(string name): base(name)
        {
        }
    }

    [Fact]
    public async Task different_foreign_key()
    {
        var states = new Table("deltas.states");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);


        var table = new Table("deltas.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        table.ForeignKeys.Single().OnDelete = CascadeAction.Cascade;

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Different.Single().Actual.Name
            .ShouldBe("fkey_people_state_id");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detect_primary_key_change()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<string>("tenant_id").AsPrimaryKey().DefaultValueByString("foo");
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching(theTable);
    }

    [Fact]
    public async Task detect_new_primary_key_change()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var table = new Table("deltas.states");
        table.AddColumn<string>("abbreviation");

        await CreateSchemaObjectInDatabase(table);

        table.ModifyColumn("abbreviation").AsPrimaryKey();

        var delta = await table.FindDeltaAsync(theConnection);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching(theTable);
    }

    [Fact]
    public async Task detect_new_primary_key_constraint_name_change()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.PrimaryKeyName = "pk_newchange";

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);

        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching(theTable);
    }

    [Fact]
    public async Task detect_primary_key_constraint_name_change_with_key_beyond_namedatalen_limit()
    {
        theTable.PrimaryKeyName =
            "pk_this_primary_key_exceeds_the_postgres_default_namedatalen_limit_of_sixtythree_chars";
        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task detect_new_primary_key_constraint_name_change_when_also_used_as_foreign_key()
    {
        var dudeTable = new Table("deltas.dudes");
        dudeTable.AddColumn<int>("id").AsPrimaryKey();
        dudeTable.AddColumn<string>("first_name");

        await CreateSchemaObjectInDatabase(dudeTable);

        var carTable = new Table("deltas.cars");
        carTable.AddColumn<int>("id").AsPrimaryKey();
        carTable.AddColumn<int>("dude_id").ForeignKeyTo(dudeTable, "id");

        await CreateSchemaObjectInDatabase(carTable);

        dudeTable.PrimaryKeyName = "pk_newchange";

        var delta = await dudeTable.FindDeltaAsync(theConnection);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);

        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching(dudeTable);
    }

    [Fact]
    public async Task equivalency_with_the_postgres_synonym_issue()
    {
        var table2 = new Table("deltas.people");
        table2.AddColumn<int>("id").AsPrimaryKey();
        table2.AddColumn("first_name", "character varying");
        table2.AddColumn("last_name", "character varying");
        table2.AddColumn("user_name", "character varying");
        table2.AddColumn("created_datetime", "timestamp without time zone");
        table2.AddColumn("created_datetime_offset", "timestamp with time zone");
        table2.AddColumn("data", "jsonb");

        await CreateSchemaObjectInDatabase(theTable);

        var delta = await table2.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task no_change_with_jsonb_path_ops()
    {
        var table2 = new Table("deltas.people");
        table2.AddColumn<int>("id").AsPrimaryKey();
        table2.AddColumn("first_name", "character varying");
        table2.AddColumn("last_name", "character varying");
        table2.AddColumn("user_name", "character varying");
        table2.AddColumn("created_datetime", "timestamp without time zone");
        table2.AddColumn("created_datetime_offset", "timestamp with time zone");
        table2.AddColumn("data", "jsonb").AddIndex(i => i.ToGinWithJsonbPathOps());

        await CreateSchemaObjectInDatabase(table2);

        var delta = await table2.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task patching_and_detecting_deltas_with_computed_indexes()
    {
        // CREATE INDEX idx_blobs_data ON deltas.blobs USING btree (data ->> 'Name');
        // CREATE INDEX mt_doc_testdocument_idx_name ON bugs.mt_doc_testdocument USING btree ( (data ->> 'Name'));

        var table = new Table("deltas.blobs");
        table.AddColumn<string>("key").AsPrimaryKey();
        table.AddColumn("data", "jsonb");

        await CreateSchemaObjectInDatabase(table);

        table.Indexes.Add(new IndexDefinition("idx_blobs_data_name") { Columns = new[] { "(data ->> 'Name')" } });

        await AssertNoDeltasAfterPatching(table);
    }
}
