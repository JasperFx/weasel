using FirebirdSql.Data.FirebirdClient;
using Weasel.Core;
using Weasel.Firebird;
using Weasel.Firebird.Functions;
using Weasel.Firebird.Procedures;
using Weasel.Firebird.Triggers;

namespace DocSamples;

/// <summary>
///     Samples for the Firebird pages on functions, stored procedures and triggers.
/// </summary>
public class FirebirdPsqlSamples
{
    // functions.md samples

    public void firebird_define_function()
    {
        #region sample_firebird_define_function
        var function = new Function("fn_order_total", """
            CREATE FUNCTION fn_order_total (order_id INTEGER, discount NUMERIC(5, 2) = 0)
            RETURNS NUMERIC(18, 2)
            AS
                DECLARE VARIABLE total NUMERIC(18, 2);
            BEGIN
                SELECT SUM(amount) FROM order_lines WHERE order_id = :order_id INTO :total;
                RETURN total * (1 - discount);
            END
            """);
        #endregion
    }

    public async Task firebird_function_delta(string connectionString, Function function)
    {
        #region sample_firebird_function_delta
        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();

        var delta = (CreateOrAlterDelta)await function.FindDeltaAsync(conn);
        // delta.Differences names what changed: a parameter, the return type, the body
        #endregion
    }

    public void firebird_function_for_removal()
    {
        #region sample_firebird_function_for_removal
        var removed = Function.ForRemoval("fn_obsolete");
        #endregion
    }

    // procedures.md samples

    public void firebird_define_procedure()
    {
        #region sample_firebird_define_procedure
        var procedure = new StoredProcedure("sp_recent_orders", """
            CREATE PROCEDURE sp_recent_orders (since TIMESTAMP, max_rows INTEGER = 10)
            RETURNS (id INTEGER, placed_at TIMESTAMP)
            AS
            BEGIN
                FOR SELECT FIRST :max_rows id, placed_at FROM orders
                    WHERE placed_at >= :since ORDER BY placed_at DESC
                    INTO :id, :placed_at
                DO SUSPEND;
            END
            """);
        #endregion
    }

    public async Task firebird_procedure_fetch_existing(string connectionString, StoredProcedure procedure)
    {
        #region sample_firebird_procedure_fetch_existing
        await using var conn = new FbConnection(connectionString);
        await conn.OpenAsync();

        var existing = await procedure.FetchExistingAsync(conn);
        // existing.BodyText() is a CREATE OR ALTER PROCEDURE statement rebuilt from the catalog
        #endregion
    }

    // triggers.md samples

    public void firebird_define_trigger()
    {
        #region sample_firebird_define_trigger
        var trigger = new Trigger("trg_orders_stamp", "orders", """
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                IF (INSERTING) THEN NEW.created_at = CURRENT_TIMESTAMP;
            END
            """)
        {
            Timing = TriggerTiming.Before,
            Events = TriggerEvents.Insert | TriggerEvents.Update
        };
        // CREATE OR ALTER TRIGGER trg_orders_stamp FOR orders ACTIVE BEFORE INSERT OR UPDATE AS ...
        #endregion
    }

    public void firebird_trigger_on_view()
    {
        #region sample_firebird_trigger_on_view
        var trigger = new Trigger("trg_order_notes_insert", "order_notes", """
            BEGIN
                INSERT INTO orders (id, note) VALUES (NEW.id, NEW.note);
            END
            """)
        {
            Timing = TriggerTiming.Before,
            Events = TriggerEvents.Insert
        };
        #endregion
    }
}
