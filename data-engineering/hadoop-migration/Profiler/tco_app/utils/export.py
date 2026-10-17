"""Export utilities for TCO results — CSV and PDF."""

import io
import pandas as pd


def run_to_csv(run: dict) -> str:
    """Convert a TCO run result to CSV string."""
    details = run.get("details", [])
    df = pd.DataFrame(details)

    # Add summary row
    summary = pd.DataFrame([{
        "job_type": "TOTAL",
        "total_apps": df["total_apps"].sum() if "total_apps" in df else 0,
        "total_memory_gb_hours": df["total_memory_gb_hours"].sum() if "total_memory_gb_hours" in df else 0,
        "estimated_dbu_hours": df["estimated_dbu_hours"].sum() if "estimated_dbu_hours" in df else 0,
        "estimated_cost": df["estimated_cost"].sum() if "estimated_cost" in df else 0,
        "hadoop_equivalent_cost": df["hadoop_equivalent_cost"].sum() if "hadoop_equivalent_cost" in df else 0,
    }])
    df = pd.concat([df, summary], ignore_index=True)

    buf = io.StringIO()
    buf.write(f"# TCO Run: {run.get('run_name', 'Untitled')}\n")
    buf.write(f"# Run ID: {run.get('run_id', '')}\n")
    buf.write(f"# Total Annual Cost: ${run.get('total_cost_annual', 0):,.2f}\n")
    buf.write(f"# Storage Annual Cost: ${run.get('total_storage_cost_annual', 0):,.2f}\n\n")
    df.to_csv(buf, index=False)
    return buf.getvalue()


def comparison_to_csv(runs: list[dict]) -> str:
    """Convert multiple TCO runs to a comparison CSV."""
    all_rows = []
    for r in runs:
        name = r.get("run_name", "Untitled")
        for d in r.get("details", []):
            d["run_name"] = name
            d["total_cost_annual"] = r.get("total_cost_annual", 0)
            all_rows.append(d)

    df = pd.DataFrame(all_rows)
    return df.to_csv(index=False)
