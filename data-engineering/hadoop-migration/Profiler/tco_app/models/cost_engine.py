"""Core TCO calculation engine.

Orchestrates: workload profiler data → SKU mapping → 7 cost categories → pricing → results.
Each run is stored in tco_runs + tco_run_details + tco_migration_timeline for reproducibility.
All functions receive explicit catalog/schema from the app UI.

Cost categories:
1. Hadoop on-prem (license, support, hardware, datacenter, admin)
2. Databricks DBU costs (ETL, Interactive, BI/SQL)
3. Cloud VM compute
4. Storage (tiered)
5. Databricks support
6. Databricks admin
7. Migration (3-year timeline)
"""

import uuid
from datetime import datetime
import pandas as pd
from utils.db_connector import execute_query, execute_statement, qualified_table
from models.pricing import create_pricing_snapshot, get_price_for_sku
from models.storage import estimate_storage_cost, estimate_tiered_storage_cost
from models.hadoop_costs import calculate_hadoop_costs
from models.vm_costs import calculate_vm_costs, get_dbsql_vm_cost
from models.migration_timeline import (
    calculate_migration_timeline, calculate_do_nothing, summarize_timeline,
)
from models.constants import (
    PERFORMANCE_GAINS, DBSQL_UTILIZATION, DEFAULT_ASSUMPTIONS, HOURS_PER_YEAR,
)


def get_workload_summary(catalog: str, schema: str) -> pd.DataFrame:
    """Get profiler workload summary by job_type, joined with SKU mapping.
    Uses the pre-aggregated workload_summary_by_type view."""
    mapping_tbl = qualified_table("tco_workload_sku_mapping", catalog, schema)
    summary_tbl = f"{catalog}.{schema}.workload_summary_by_type"

    return execute_query(f"""
        SELECT
            w.job_type,
            w.total_jobs AS total_apps,
            w.total_memory_gb_hours,
            w.total_memory_gb_hours AS total_vcore_hours,
            m.target_sku,
            m.target_sku_alt,
            m.compute_category
        FROM {summary_tbl} w
        LEFT JOIN {mapping_tbl} m ON w.job_type = m.job_type
        ORDER BY w.total_memory_gb_hours DESC
    """)


def get_assumptions(assumption_id: str, catalog: str, schema: str) -> dict:
    """Load an assumption set by ID."""
    tbl = qualified_table("tco_assumptions", catalog, schema)
    df = execute_query(f"SELECT * FROM {tbl} WHERE assumption_id = '{assumption_id}'")
    if df.empty:
        raise ValueError(f"Assumption set not found: {assumption_id}")
    row = df.iloc[0].to_dict()
    # Fill NULLs with defaults
    for k, v in DEFAULT_ASSUMPTIONS.items():
        if k in row and (row[k] is None or pd.isna(row[k])):
            row[k] = v
    return row


def list_assumptions(catalog: str, schema: str) -> pd.DataFrame:
    """List all saved assumption sets."""
    tbl = qualified_table("tco_assumptions", catalog, schema)
    return execute_query(f"""
        SELECT assumption_id, name, target_cloud, databricks_tier,
               use_serverless, discount_pct, created_at
        FROM {tbl}
        ORDER BY created_at DESC
    """)


def save_assumptions(assumptions: dict, catalog: str, schema: str) -> str:
    """Save a new assumption set. Returns assumption_id."""
    tbl = qualified_table("tco_assumptions", catalog, schema)
    assumption_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    # Build column list and values from all known fields
    base_cols = [
        "assumption_id", "name", "target_cloud", "databricks_tier",
        "use_serverless", "photon_enabled", "utilization_factor", "overhead_factor",
        "discount_pct", "vm_mem_gb", "hdfs_repl_factor", "delta_compression",
        "storage_cost_per_gb_month", "created_by", "created_at",
    ]
    v2_cols = [
        "hadoop_vendor_type", "hadoop_node_count", "hadoop_vcores_per_node",
        "hadoop_utilization_pct", "hadoop_license_per_node", "hadoop_license_discount",
        "hadoop_support_pct", "hadoop_hardware_per_node", "hadoop_datacenter_per_node",
        "hadoop_admin_count", "hadoop_admin_salary",
        "dev_test_uplift", "hyperthreading_factor", "photon_perf_gain",
        "etl_pct", "interactive_pct", "bisql_pct",
        "vm_discount_type", "worker_instance_type", "driver_instance_type",
        "dbsql_warehouse_size", "dbsql_type", "dbsql_utilization",
        "storage_discount_pct", "hot_storage_pct", "cold_storage_pct", "archive_storage_pct",
        "dbx_support_pct", "dbx_admin_overhead_pct",
        "migration_tshirt", "migration_custom_cost", "ecif_credit",
        "migration_duration_quarters",
    ]

    all_cols = base_cols + v2_cols
    values = {
        "assumption_id": assumption_id,
        "created_by": assumptions.get("created_by", "app"),
        "created_at": now,
    }
    values.update(assumptions)

    col_strs = []
    val_strs = []
    for col in all_cols:
        if col in values:
            col_strs.append(col)
            v = values[col]
            if v is None:
                val_strs.append("NULL")
            elif isinstance(v, bool):
                val_strs.append(str(v).lower())
            elif isinstance(v, (int, float)):
                val_strs.append(str(v))
            else:
                val_strs.append(f"'{v}'")

    execute_statement(f"""
        INSERT INTO {tbl} ({', '.join(col_strs)})
        VALUES ({', '.join(val_strs)})
    """)
    return assumption_id


def calculate_tco(
    assumption_id: str,
    catalog: str,
    schema: str,
    run_name: str = "",
    hadoop_cost_annual: float | None = None,
) -> dict:
    """Run a full TCO calculation across all 7 cost categories.

    Steps:
    1. Load assumptions
    2. Snapshot current DBU prices
    3. Aggregate profiler workloads by job_type
    4. Calculate Hadoop on-prem costs
    5. Split workloads into ETL/Interactive/BI streams
    6. Apply stream-specific modifiers and estimate DBUs
    7. Calculate VM costs
    8. Calculate tiered storage
    9. Calculate Databricks support + admin
    10. Calculate migration timeline
    11. Write tco_runs + tco_run_details + tco_migration_timeline
    """
    assumptions = get_assumptions(assumption_id, catalog, schema)
    cloud = assumptions["target_cloud"]
    discount_pct = float(assumptions.get("discount_pct", 0) or 0)
    util_factor = float(assumptions.get("utilization_factor", 0.9) or 0.9)
    overhead_factor = float(assumptions.get("overhead_factor", 1.1) or 1.1)

    # Step 2: Snapshot pricing
    snapshot_id = create_pricing_snapshot(cloud, catalog, schema)

    # Step 3: Get workload summary
    workloads = get_workload_summary(catalog, schema)

    # Step 4: Hadoop on-prem costs
    hadoop_costs = calculate_hadoop_costs(assumptions)
    hadoop_annual = hadoop_costs["total"]
    # Override with user-provided Hadoop cost if given
    if hadoop_cost_annual and hadoop_cost_annual > 0:
        hadoop_annual = hadoop_cost_annual

    # Step 5-6: Split workloads by compute_category and estimate DBUs
    run_id = str(uuid.uuid4())
    details = []
    stream_dbu_costs = {"etl": 0.0, "interactive": 0.0, "bisql": 0.0}
    total_compute_cost = 0.0

    # Compute modifiers
    photon_enabled = assumptions.get("photon_enabled", True)
    engine = "photon" if photon_enabled else "classic"
    perf_gain = float(assumptions.get("photon_perf_gain") or PERFORMANCE_GAINS.get(engine, 0.3))
    dev_test_uplift = float(assumptions.get("dev_test_uplift", 0) or 0)
    ht_factor = float(assumptions.get("hyperthreading_factor", 2.0) or 2.0)

    for _, row in workloads.iterrows():
        sku = row["target_sku"]
        if pd.isna(sku) or not sku:
            sku = "PREMIUM_ALL_PURPOSE_COMPUTE"

        # Use serverless SKU if toggled
        if assumptions.get("use_serverless") and pd.notna(row.get("target_sku_alt")):
            sku = row["target_sku_alt"]

        mem_gb_hours = float(row["total_memory_gb_hours"] or 0)
        compute_cat = row.get("compute_category", "jobs")

        # Apply stream-specific modifiers
        # Spreadsheet formula: vCPUs × utilization × (1 + dev_test) × HT_factor × (1 - perf_gain)
        estimated_dbus = (
            mem_gb_hours * util_factor * overhead_factor
            * (1 + dev_test_uplift) * (1 - perf_gain)
        )

        price_info = get_price_for_sku(snapshot_id, sku, cloud, catalog, schema)
        list_price = price_info["list_price"]
        effective_price = list_price * (1 - discount_pct / 100)
        annual_cost = estimated_dbus * effective_price

        # Map compute_category to stream
        stream = _category_to_stream(compute_cat)
        stream_dbu_costs[stream] = stream_dbu_costs.get(stream, 0) + annual_cost

        total_compute_cost += annual_cost

        details.append({
            "run_id": run_id,
            "job_type": row["job_type"],
            "target_sku": sku,
            "total_apps": int(row["total_apps"]),
            "total_memory_gb_hours": mem_gb_hours,
            "total_vcore_hours": float(row["total_vcore_hours"] or 0),
            "estimated_dbu_hours": round(estimated_dbus, 2),
            "dbu_list_price": list_price,
            "dbu_effective_price": effective_price,
            "estimated_cost": round(annual_cost, 2),
            "hadoop_equivalent_cost": round(mem_gb_hours * 0.25, 2),
        })

    # Step 7: VM costs
    # Estimate cluster counts from workload profile
    total_vcores = sum(float(d["total_vcore_hours"] or 0) for d in details)
    etl_pct = float(assumptions.get("etl_pct", 40) or 40) / 100
    interactive_pct = float(assumptions.get("interactive_pct", 30) or 30) / 100
    bisql_pct = float(assumptions.get("bisql_pct", 30) or 30) / 100

    worker_type = assumptions.get("worker_instance_type", "m6id.2xlarge")
    # Get vCPUs per worker from lookup
    from models.vm_costs import get_instance_price
    worker_info = get_instance_price(worker_type, cloud, catalog, schema)
    vcpus_per_worker = int(worker_info.get("vcpus", 8) or 8)

    # Calculate clusters per stream
    def _stream_clusters(pct, hours_per_day=24):
        stream_vcores = total_vcores * pct * (1 + dev_test_uplift) / ht_factor * (1 - perf_gain)
        workers = max(1, int(stream_vcores / vcpus_per_worker / hours_per_day / 365))
        clusters = max(1, workers // 4)  # ~4 workers per cluster
        return {"clusters": clusters, "workers_per_cluster": min(workers, 4), "hours_per_day": hours_per_day}

    stream_config = {
        "etl": _stream_clusters(etl_pct, 24),
        "interactive": _stream_clusters(interactive_pct, 12),
    }

    vm_result = calculate_vm_costs(assumptions, stream_config, catalog, schema)
    vm_cost_annual = vm_result["total"]

    # BI/SQL VM cost from DBSQL lookup (serverless = $0 VM)
    dbsql_type = assumptions.get("dbsql_type", "pro")
    bisql_vm_cost = 0.0
    if dbsql_type != "serverless":
        dbsql_size = assumptions.get("dbsql_warehouse_size", "Medium")
        dbsql_info = get_dbsql_vm_cost(dbsql_size, cloud, catalog, schema)
        bisql_vm_cost = float(dbsql_info.get("vm_cost_per_hour", 0) or 0) * HOURS_PER_YEAR
    vm_cost_annual += bisql_vm_cost

    # Step 8: Storage
    storage_result = estimate_tiered_storage_cost(
        catalog, schema, cloud,
        hdfs_repl_factor=int(assumptions.get("hdfs_repl_factor", 3) or 3),
        delta_compression=float(assumptions.get("delta_compression", 0.5) or 0.5),
        hot_pct=float(assumptions.get("hot_storage_pct", 70) or 70),
        cold_pct=float(assumptions.get("cold_storage_pct", 20) or 20),
        archive_pct=float(assumptions.get("archive_storage_pct", 10) or 10),
        storage_discount_pct=float(assumptions.get("storage_discount_pct", 0) or 0),
    )
    total_storage_annual = storage_result["annual_cost"]

    # Step 9: Support + Admin
    dbx_support_pct = float(assumptions.get("dbx_support_pct", 25) or 25) / 100
    dbx_support_cost = total_compute_cost * dbx_support_pct

    dbx_admin_pct = float(assumptions.get("dbx_admin_overhead_pct", 30) or 30) / 100
    dbx_admin_cost = hadoop_costs["admin_cost"] * dbx_admin_pct

    # Total Databricks annual
    total_dbx_annual = (
        total_compute_cost + vm_cost_annual + total_storage_annual
        + dbx_support_cost + dbx_admin_cost
    )

    # Savings
    savings_pct = None
    if hadoop_annual and hadoop_annual > 0:
        savings_pct = (1 - total_dbx_annual / hadoop_annual) * 100

    # Step 10: Migration timeline
    timeline = calculate_migration_timeline(assumptions, hadoop_annual, total_dbx_annual)
    do_nothing = calculate_do_nothing(hadoop_annual)
    timeline_summary = summarize_timeline(timeline, do_nothing)

    # Step 11: Write results
    _write_run(
        run_id, run_name, assumption_id, snapshot_id, catalog, schema,
        hadoop_costs=hadoop_costs,
        stream_dbu_costs=stream_dbu_costs,
        vm_cost=vm_cost_annual,
        storage_cost=total_storage_annual,
        dbx_support=dbx_support_cost,
        dbx_admin=dbx_admin_cost,
        total_dbx=total_dbx_annual,
        hadoop_annual=hadoop_annual,
        savings_pct=savings_pct,
        timeline_summary=timeline_summary,
    )
    _write_run_details(details, catalog, schema)
    _write_migration_timeline(run_id, timeline, catalog, schema)

    return {
        "run_id": run_id,
        "run_name": run_name,
        "snapshot_id": snapshot_id,
        "assumption_id": assumption_id,
        # Hadoop breakdown
        "hadoop_costs": hadoop_costs,
        "hadoop_annual": round(hadoop_annual, 2),
        # Databricks breakdown
        "stream_dbu_costs": {k: round(v, 2) for k, v in stream_dbu_costs.items()},
        "total_compute_cost_annual": round(total_compute_cost, 2),
        "vm_cost_annual": round(vm_cost_annual, 2),
        "total_storage_cost_annual": round(total_storage_annual, 2),
        "dbx_support_cost": round(dbx_support_cost, 2),
        "dbx_admin_cost": round(dbx_admin_cost, 2),
        "total_dbx_annual": round(total_dbx_annual, 2),
        "total_cost_annual": round(total_dbx_annual, 2),
        "savings_pct": round(savings_pct, 1) if savings_pct else None,
        "workload_count": len(details),
        "details": details,
        "storage": storage_result,
        "vm": vm_result,
        # Migration
        "timeline": timeline,
        "timeline_summary": timeline_summary,
        "do_nothing": do_nothing,
    }


def _category_to_stream(compute_category: str) -> str:
    """Map compute_category to cost stream."""
    if compute_category in ("sql", "serverless_sql"):
        return "bisql"
    elif compute_category in ("all_purpose",):
        return "interactive"
    else:
        return "etl"


def _write_run(run_id, run_name, assumption_id, snapshot_id,
               catalog, schema, *, hadoop_costs, stream_dbu_costs,
               vm_cost, storage_cost, dbx_support, dbx_admin,
               total_dbx, hadoop_annual, savings_pct, timeline_summary):
    """Insert a row into tco_runs with full cost breakdown."""
    tbl = qualified_table("tco_runs", catalog, schema)
    now = datetime.utcnow().isoformat()

    def _v(val):
        return "NULL" if val is None else str(val)

    execute_statement(f"""
        INSERT INTO {tbl}
        (run_id, run_name, assumption_id, snapshot_id,
         profiler_catalog, profiler_schema,
         total_hadoop_cost_annual, total_databricks_cost_annual,
         total_storage_cost_annual, total_cost_annual, savings_pct,
         hadoop_license_cost, hadoop_support_cost, hadoop_hardware_cost,
         hadoop_datacenter_cost, hadoop_admin_cost,
         dbx_etl_dbu_cost, dbx_interactive_dbu_cost, dbx_bisql_dbu_cost,
         dbx_vm_cost, dbx_support_cost, dbx_admin_cost,
         migration_cost_total, three_year_hadoop_total, three_year_databricks_total,
         three_year_savings,
         created_by, created_at)
        VALUES (
            '{run_id}', '{run_name}', '{assumption_id}', '{snapshot_id}',
            '{catalog}', '{schema}',
            {_v(hadoop_annual)}, {_v(total_dbx)},
            {_v(storage_cost)}, {_v(total_dbx)}, {_v(savings_pct)},
            {hadoop_costs['license_cost']}, {hadoop_costs['support_cost']},
            {hadoop_costs['hardware_cost']}, {hadoop_costs['datacenter_cost']},
            {hadoop_costs['admin_cost']},
            {stream_dbu_costs.get('etl', 0)}, {stream_dbu_costs.get('interactive', 0)},
            {stream_dbu_costs.get('bisql', 0)},
            {vm_cost}, {dbx_support}, {dbx_admin},
            {timeline_summary.get('three_year_migration_portion', 0)},
            {timeline_summary.get('do_nothing_total', 0)},
            {timeline_summary.get('three_year_total', 0)},
            {timeline_summary.get('net_savings', 0)},
            'app', '{now}'
        )
    """)


def _write_run_details(details: list[dict], catalog: str, schema: str):
    """Insert rows into tco_run_details."""
    tbl = qualified_table("tco_run_details", catalog, schema)
    for d in details:
        execute_statement(f"""
            INSERT INTO {tbl}
            (run_id, job_type, target_sku, total_apps,
             total_memory_gb_hours, total_vcore_hours,
             estimated_dbu_hours, dbu_list_price, dbu_effective_price,
             estimated_cost, hadoop_equivalent_cost)
            VALUES (
                '{d["run_id"]}', '{d["job_type"]}', '{d["target_sku"]}',
                {d["total_apps"]}, {d["total_memory_gb_hours"]},
                {d["total_vcore_hours"]}, {d["estimated_dbu_hours"]},
                {d["dbu_list_price"]}, {d["dbu_effective_price"]},
                {d["estimated_cost"]}, {d["hadoop_equivalent_cost"]}
            )
        """)


def _write_migration_timeline(run_id: str, timeline: list[dict],
                              catalog: str, schema: str):
    """Insert rows into tco_migration_timeline."""
    tbl = qualified_table("tco_migration_timeline", catalog, schema)
    for q in timeline:
        execute_statement(f"""
            INSERT INTO {tbl}
            (run_id, quarter, quarter_label, migration_pct,
             hadoop_cost, databricks_cost, migration_cost, total_cost,
             can_turn_off_hadoop)
            VALUES (
                '{run_id}', {q["quarter"]}, '{q["quarter_label"]}',
                {q["migration_pct"]}, {q["hadoop_cost"]}, {q["databricks_cost"]},
                {q["migration_cost"]}, {q["total_cost"]},
                {str(q["can_turn_off_hadoop"]).lower()}
            )
        """)


def get_run(run_id: str, catalog: str, schema: str) -> dict:
    """Load a completed run with its details."""
    runs_tbl = qualified_table("tco_runs", catalog, schema)
    details_tbl = qualified_table("tco_run_details", catalog, schema)

    run_df = execute_query(f"SELECT * FROM {runs_tbl} WHERE run_id = '{run_id}'")
    details_df = execute_query(
        f"SELECT * FROM {details_tbl} WHERE run_id = '{run_id}' ORDER BY estimated_cost DESC"
    )

    if run_df.empty:
        raise ValueError(f"Run not found: {run_id}")

    result = run_df.iloc[0].to_dict()
    result["details"] = details_df.to_dict("records")

    # Load migration timeline if available
    try:
        timeline_tbl = qualified_table("tco_migration_timeline", catalog, schema)
        tl_df = execute_query(
            f"SELECT * FROM {timeline_tbl} WHERE run_id = '{run_id}' ORDER BY quarter"
        )
        result["timeline"] = tl_df.to_dict("records") if not tl_df.empty else []
    except Exception:
        result["timeline"] = []

    return result


def list_runs(catalog: str, schema: str) -> pd.DataFrame:
    """List all TCO runs."""
    tbl = qualified_table("tco_runs", catalog, schema)
    return execute_query(f"""
        SELECT run_id, run_name, total_cost_annual, savings_pct, created_at
        FROM {tbl}
        ORDER BY created_at DESC
    """)
