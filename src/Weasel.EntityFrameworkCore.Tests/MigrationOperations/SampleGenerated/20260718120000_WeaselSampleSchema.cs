using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using System;

namespace Weasel.EntityFrameworkCore.Tests.MigrationOperations.SampleGenerated;

[DbContext(typeof(WeaselSampleDbContext))]
[Migration("20260718120000_WeaselSampleSchema")]
public partial class WeaselSampleSchema : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.EnsureSchema(name: "efgen");

        migrationBuilder.CreateSequence(
            name: "order_numbers",
            schema: "efgen",
            startValue: 1000L,
            incrementBy: 10);

        migrationBuilder.CreateTable(
            name: "customers",
            schema: "efgen",
            columns: table => new
            {
                id = table.Column<int>(type: "integer", nullable: false)
                    .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                name = table.Column<string>(type: "varchar", nullable: false, defaultValueSql: "'unknown'")
            },
            constraints: table =>
            {
                table.PrimaryKey("pkey_customers_id", x => x.id);
                table.CheckConstraint("ck_customers_name", "length(name) > 0");
            });

        migrationBuilder.CreateTable(
            name: "orders",
            schema: "efgen",
            columns: table => new
            {
                id = table.Column<Guid>(type: "uuid", nullable: false),
                customer_id = table.Column<int>(type: "integer", nullable: false),
                payload = table.Column<string>(type: "jsonb", nullable: true),
                status = table.Column<string>(type: "varchar", nullable: false, defaultValueSql: "'pending'")
            },
            constraints: table =>
            {
                table.PrimaryKey("pkey_orders_id", x => x.id);
                table.ForeignKey(
                    name: "fk_orders_customer",
                    column: x => x.customer_id,
                    principalSchema: "efgen",
                    principalTable: "customers",
                    principalColumn: "id",
                    onDelete: ReferentialAction.Cascade,
                    onUpdate: ReferentialAction.NoAction);
            });

        migrationBuilder.CreateIndex(
            name: "idx_orders_status",
            schema: "efgen",
            table: "orders",
            column: "status",
            filter: "status <> 'archived'");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "orders", schema: "efgen");

        migrationBuilder.DropTable(name: "customers", schema: "efgen");

        migrationBuilder.DropSequence(name: "order_numbers", schema: "efgen");
    }
}
