from pyspark.sql import Row
from pyspark.sql import functions as F

from PySpark.pipeline import clean_manufacturing, clean_events, clean_operators, build_fact_table


def test_build_fact_sets_flags_and_joins_operator(spark):
    # Manufacturing: one record inside maintenance window w/ breakdown reason, one outside
    mfg_rows = [
        Row(
            timestamp="2025-01-01 08:30:00",
            factory_id="F1",
            line_id="L1",
            shift="Day",
            order_id="O1",
            product_id="P1",
            operator_id="OP1",
            planned_qty=10,
            produced_qty=9,
            scrap_qty=1,
            defects_count=0,
            cycle_time_s=1.0,
            oee=0.9,
            availability=0.9,
            performance=0.95,
            quality=0.98,
            machine_state="Down",
            downtime_reason="Unplanned Breakdown",   # should trigger breakdown flag
            maintenance_type="",
            maintenance_due_date="2025-01-10",
            vibration_mm_s=1.0,
            temperature_c=25.0,
            pressure_bar=1.0,
            energy_kwh=1.0,
            workorder_status="Open",
        ),
        Row(
            timestamp="2025-01-01 12:00:00",
            factory_id="F1",
            line_id="L1",
            shift="Day",
            order_id="O2",
            product_id="P2",
            operator_id="OP1",
            planned_qty=10,
            produced_qty=10,
            scrap_qty=0,
            defects_count=0,
            cycle_time_s=1.0,
            oee=0.95,
            availability=0.95,
            performance=0.95,
            quality=1.0,
            machine_state="Run",
            downtime_reason="None",
            maintenance_type="",
            maintenance_due_date="2025-01-10",
            vibration_mm_s=1.0,
            temperature_c=25.0,
            pressure_bar=1.0,
            energy_kwh=1.0,
            workorder_status="Open",
        ),
    ]

    # Maintenance event: covers 08:00 -> 09:00
    events_rows = [
        Row(
            event_id="E1",
            factory_id="F1",
            line_id="L1",
            start_time="2025-01-01 08:00:00",
            end_time="2025-01-01 09:00:00",
            downtime_min=60,
            maintenance_type="Corrective",
            reason="Failure",
            outcome="Fixed",
            cost_eur=100.0,
        )
    ]

    # Operators roster
    ops_rows = [
        Row(
            operator_id="OP1",
            operator_name="Alice",
            operator_team="T1",
            operator_skill_level="Senior",
            operator_reliability_score=0.99,
            operator_hourly_rate_eur=50.0,
            factory_id="F1",
            primary_line="L1",
            primary_shift="Day",
        )
    ]

    df_mfg = clean_manufacturing(spark.createDataFrame(mfg_rows))
    df_events = clean_events(spark.createDataFrame(events_rows))
    df_ops = clean_operators(spark.createDataFrame(ops_rows))

    fact = build_fact_table(df_mfg, df_events, df_ops)

    rows = fact.select(
        "order_id",
        "in_maintenance_window",
        "is_unplanned_breakdown",
        "operator_name",
        "maintenance_event_id",
    ).orderBy("order_id").collect()

    # Order O1 is in window + breakdown
    assert rows[0]["order_id"] == "O1"
    assert rows[0]["in_maintenance_window"] == 1
    assert rows[0]["is_unplanned_breakdown"] == 1
    assert rows[0]["operator_name"] == "Alice"
    assert rows[0]["maintenance_event_id"] == "E1"

    # Order O2 is outside window + not breakdown
    assert rows[1]["order_id"] == "O2"
    assert rows[1]["in_maintenance_window"] == 0
    assert rows[1]["is_unplanned_breakdown"] == 0
    assert rows[1]["operator_name"] == "Alice"
    assert rows[1]["maintenance_event_id"] is None