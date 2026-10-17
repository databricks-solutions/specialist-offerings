// Hadoop on-prem cost model — faithful port of the Dash app's models/hadoop_costs.py.
// Reproduces the Visa sheet's Hadoop total to the dollar given matching inputs.

import type { Assumptions, HadoopCosts } from '../../shared/tco-types';
import { withDefaults } from './constants';

const round2 = (n: number) => Math.round(n * 100) / 100;

export function calculateHadoopCosts(assumptions: Assumptions): HadoopCosts {
  const a = withDefaults(assumptions);

  const vendorType = a.hadoop_vendor_type;
  const nodes = Math.trunc(a.hadoop_node_count || 0);
  const licensePerNode = a.hadoop_license_per_node || 0;
  const licenseDiscount = (a.hadoop_license_discount || 0) / 100;
  const supportPct = (a.hadoop_support_pct || 0) / 100;
  const hwPerNode = a.hadoop_hardware_per_node || 0;
  const dcPerNode = a.hadoop_datacenter_per_node || 0;
  const adminCount = Math.trunc(a.hadoop_admin_count || 0);
  const adminSalary = a.hadoop_admin_salary || 0;

  // License is $0 for open source.
  const licenseCost = vendorType === 'Open Source' ? 0 : nodes * licensePerNode * (1 - licenseDiscount);
  const supportCost = licenseCost * supportPct;
  const hardwareCost = nodes * hwPerNode;
  const datacenterCost = nodes * dcPerNode;
  const adminCost = adminCount * adminSalary;

  const total = licenseCost + supportCost + hardwareCost + datacenterCost + adminCost;

  return {
    license_cost: round2(licenseCost),
    support_cost: round2(supportCost),
    hardware_cost: round2(hardwareCost),
    datacenter_cost: round2(datacenterCost),
    admin_cost: round2(adminCost),
    total: round2(total),
    node_count: nodes,
    vendor_type: vendorType,
  };
}
