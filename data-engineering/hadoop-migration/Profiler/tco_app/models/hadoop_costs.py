"""Hadoop on-premises cost model.

Calculates 5 sub-costs from user-input assumptions:
- License, Support, Hardware, Data Center, Admin
"""


def calculate_hadoop_costs(assumptions: dict) -> dict:
    """Calculate annual Hadoop on-prem costs from assumption inputs.

    Args:
        assumptions: Dict with hadoop_* fields from tco_assumptions.

    Returns:
        Dict with per-category costs and total.
    """
    vendor_type = assumptions.get("hadoop_vendor_type", "Licensed")
    nodes = int(assumptions.get("hadoop_node_count", 0) or 0)
    license_per_node = float(assumptions.get("hadoop_license_per_node", 0) or 0)
    license_discount = float(assumptions.get("hadoop_license_discount", 0) or 0) / 100
    support_pct = float(assumptions.get("hadoop_support_pct", 0) or 0) / 100
    hw_per_node = float(assumptions.get("hadoop_hardware_per_node", 0) or 0)
    dc_per_node = float(assumptions.get("hadoop_datacenter_per_node", 0) or 0)
    admin_count = int(assumptions.get("hadoop_admin_count", 0) or 0)
    admin_salary = float(assumptions.get("hadoop_admin_salary", 0) or 0)

    # License cost: $0 for open-source
    if vendor_type == "Open Source":
        license_cost = 0.0
    else:
        license_cost = nodes * license_per_node * (1 - license_discount)

    support_cost = license_cost * support_pct
    hardware_cost = nodes * hw_per_node
    datacenter_cost = nodes * dc_per_node
    admin_cost = admin_count * admin_salary

    total = license_cost + support_cost + hardware_cost + datacenter_cost + admin_cost

    return {
        "license_cost": round(license_cost, 2),
        "support_cost": round(support_cost, 2),
        "hardware_cost": round(hardware_cost, 2),
        "datacenter_cost": round(datacenter_cost, 2),
        "admin_cost": round(admin_cost, 2),
        "total": round(total, 2),
        "node_count": nodes,
        "vendor_type": vendor_type,
    }
