"""3-year migration timeline model.

Generates a 12-quarter migration ramp with parallel Hadoop+Databricks costs,
migration services cost, and a "Do Nothing" baseline comparison.
"""

from models.constants import MIGRATION_TSHIRT


def calculate_migration_timeline(
    assumptions: dict,
    hadoop_annual: float,
    databricks_annual: float,
) -> list[dict]:
    """Generate 12-quarter migration timeline.

    Args:
        assumptions: Full assumption dict with migration_* fields.
        hadoop_annual: Total annual Hadoop cost.
        databricks_annual: Total annual Databricks cost.

    Returns:
        List of 12 quarter dicts.
    """
    duration = int(assumptions.get("migration_duration_quarters", 8) or 8)
    duration = max(1, min(12, duration))

    # Migration cost
    tshirt = assumptions.get("migration_tshirt", "medium")
    if tshirt == "custom":
        migration_total = float(assumptions.get("migration_custom_cost", 0) or 0)
    else:
        migration_total = MIGRATION_TSHIRT.get(tshirt, 850_000)

    ecif_credit = float(assumptions.get("ecif_credit", 0) or 0)
    migration_total = max(0, migration_total - ecif_credit)

    migration_per_quarter = migration_total / duration if duration > 0 else 0
    hadoop_quarterly = hadoop_annual / 4
    databricks_quarterly = databricks_annual / 4

    timeline = []
    for q in range(1, 13):
        year = (q - 1) // 4 + 1
        qtr = (q - 1) % 4 + 1
        label = f"Q{qtr} Y{year}"

        # Linear migration ramp
        if q <= duration:
            migration_pct = q / duration
        else:
            migration_pct = 1.0

        # During migration: pay both Hadoop (declining) and Databricks (growing)
        h_cost = hadoop_quarterly * (1 - migration_pct)
        d_cost = databricks_quarterly * migration_pct
        m_cost = migration_per_quarter if q <= duration else 0

        # Can turn off Hadoop when migration is complete enough
        # Spreadsheet uses 43.75% threshold (7/16)
        can_turn_off = migration_pct >= 1.0

        timeline.append({
            "quarter": q,
            "quarter_label": label,
            "migration_pct": round(migration_pct, 4),
            "hadoop_cost": round(h_cost, 2),
            "databricks_cost": round(d_cost, 2),
            "migration_cost": round(m_cost, 2),
            "total_cost": round(h_cost + d_cost + m_cost, 2),
            "can_turn_off_hadoop": can_turn_off,
        })

    return timeline


def calculate_do_nothing(hadoop_annual: float) -> dict:
    """Calculate 3-year "Do Nothing" (stay on Hadoop) cost.

    Returns summary with quarterly and 3-year totals.
    """
    quarterly = hadoop_annual / 4
    three_year = hadoop_annual * 3

    return {
        "annual_cost": round(hadoop_annual, 2),
        "quarterly_cost": round(quarterly, 2),
        "three_year_total": round(three_year, 2),
    }


def summarize_timeline(timeline: list[dict], do_nothing: dict) -> dict:
    """Compute 3-year summary from timeline vs Do Nothing.

    Returns dict with totals, savings, and payback quarter.
    """
    three_year_total = sum(q["total_cost"] for q in timeline)
    three_year_hadoop = sum(q["hadoop_cost"] for q in timeline)
    three_year_databricks = sum(q["databricks_cost"] for q in timeline)
    three_year_migration = sum(q["migration_cost"] for q in timeline)

    do_nothing_total = do_nothing["three_year_total"]
    net_savings = do_nothing_total - three_year_total

    # Find payback quarter: when cumulative migration cost is offset by savings
    cumulative_cost = 0
    do_nothing_cumulative = 0
    payback_quarter = None
    for q in timeline:
        cumulative_cost += q["total_cost"]
        do_nothing_cumulative += do_nothing["quarterly_cost"]
        if payback_quarter is None and cumulative_cost < do_nothing_cumulative:
            payback_quarter = q["quarter"]

    return {
        "three_year_total": round(three_year_total, 2),
        "three_year_hadoop_portion": round(three_year_hadoop, 2),
        "three_year_databricks_portion": round(three_year_databricks, 2),
        "three_year_migration_portion": round(three_year_migration, 2),
        "do_nothing_total": round(do_nothing_total, 2),
        "net_savings": round(net_savings, 2),
        "savings_pct": round(net_savings / do_nothing_total * 100, 1) if do_nothing_total > 0 else 0,
        "payback_quarter": payback_quarter,
    }
