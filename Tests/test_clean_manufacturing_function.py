import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from PySpark.pipeline import clean_manufacturing


def test_clean_manufacturing_filters_defaults_and_dedup(spark):
    rows = [
        # Valid row
        Row(
            timestamp="2025-01-01 08:00:00",
            factory_id="F1",
            line_id="L1",
            shift=" Day ",
            order_id="O1",
            product_id="P1",
            planned_qty=None,          # should default to 0
            produced_qty=10,
            scrap_qty=None,            # should default to 0
            defects_count=None,        # should default to 0
            energy_kwh=None,           # should default to 0.0
            maintenance_due_date="2025-01-10",
            machine_state=" RUN ",
            downtime_reason=" None ",
            maintenance_type=" Preventive ",
            workorder_status=" OPEN ",
        ),

        # Duplicate of the valid row (same natural grain)
        Row(
            timestamp="2025-01-01 08:00:00",
            factory_id="F1",
            line_id="L1",
            shift=" Day ",
            order_id="O1",
            product_id="P1",
            planned_qty=None,
            produced_qty=10,
            scrap_qty=None,
            defects_count=None,
            energy_kwh=None,
            maintenance_due_date="2025-01-10",
            machine_state=" RUN ",
            downtime_reason=" None ",
            maintenance_type=" Preventive ",
            workorder_status=" OPEN ",
        ),

        # Invalid: negative produced_qty -> filtered out
        Row(
            timestamp="2025-01-01 09:00:00",
            factory_id="F1",
            line_id="L1",
            shift="Night",
            order_id="O2",
            product_id="P2",
            planned_qty=5,
            produced_qty=-1,
            scrap_qty=0,
            defects_count=0,
            energy_kwh=1.2,
            maintenance_due_date="2025-01-11",
            machine_state="RUN",
            downtime_reason="",
            maintenance_type="",
            workorder_status="",
        ),

        # Invalid: null factory_id -> filtered out
        Row(
            timestamp="2025-01-01 10:00:00",
            factory_id=None,
            line_id="L1",
            shift="Day",
            order_id="O3",
            product_id="P3",
            planned_qty=1,
            produced_qty=1,
            scrap_qty=0,
            defects_count=0,
            energy_kwh=0.5,
            maintenance_due_date="2025-01-12",
            machine_state="RUN",
            downtime_reason="",
            maintenance_type="",
            workorder_status="",
        ),
    ]

    df = spark.createDataFrame(rows)
    out = clean_manufacturing(df)

    # Only the one valid row should remain (dedup removes the duplicate)
    assert out.count() == 1

    r = out.collect()[0]

    # Parsed timestamp/date columns exist and are not null
    assert r["timestamp_ts"] is not None
    assert r["maintenance_due_date_dt"] is not None

    # Defaults were applied
    assert r["planned_qty"] == 0
    assert r["scrap_qty"] == 0
    assert r["defects_count"] == 0
    assert abs(r["energy_kwh"] - 0.0) < 1e-9

    # Normalized columns exist (you normalize into *_norm)
    assert r["shift_norm"] is not None
    assert r["machine_state_norm"] is not None
