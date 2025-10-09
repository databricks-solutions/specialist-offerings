
"""
Databricks Security Detection Alert Generator
Reads rules.yml and generates SQL alerts dynamically
"""

import yaml
import os
from typing import Dict, Any
from pathlib import Path

def load_rules(rules_file: str = None) -> Dict[str, Any]:
    """Load alert rules from YAML file"""
    if rules_file is None:
        # Get the script's directory and resolve paths relative to project root
        script_dir = Path(__file__).parent
        project_root = script_dir.parent.parent  # Go up from utils/ to src/ to project root
        rules_file = project_root / "rules.yml"
    
    with open(rules_file, 'r') as f:
        return yaml.safe_load(f)

def generate_alert_sql(alert_config: Dict[str, Any], global_config: Dict[str, Any], time_intervals: Dict[str, int]) -> str:
    """Generate SQL for a single alert"""
    
    # Use the complete query_template as query_text (including format_string wrapper)
    query_text = alert_config['query_template']
    
    # Get the time interval for this specific alert
    alert_time_interval = alert_config.get('time_interval', 'default')
    
    # Substitute the time_interval placeholder with the actual hours value
    if alert_time_interval in time_intervals:
        hours = time_intervals[alert_time_interval]
        query_text = query_text.replace("{time_interval}", str(hours))
    else:
        # Fallback to default if specified interval doesn't exist
        hours = time_intervals.get('default', 168)
        query_text = query_text.replace("{time_interval}", str(hours))
        print(f"⚠️  Warning: time_interval '{alert_time_interval}' not found for alert '{alert_config['display_name']}'. Using default ({hours} hours).")

    print(query_text)
    
    sql = f"""
-- {alert_config['description']}
SELECT create_alert(
    display_name => '{alert_config['display_name']}',
    query_text => format_string('{query_text}', :catalog, :schema),
    warehouse_id => {global_config['warehouse_id']},
    comparison_operator => '{alert_config['comparison_operator']}',
    threshold_value => {alert_config['threshold_value']},
    empty_result_state => '{alert_config.get('empty_result_state', 'UNKNOWN')}',
    user_email => {global_config['user_email']},
    notify_on_ok => {str(global_config['notify_on_ok']).lower()},
    retrigger_seconds => {global_config['retrigger_seconds']},
    cron_schedule => '{alert_config.get('cron_schedule', global_config.get('cron_schedule', '0 0 10 1/7 * ?'))}',
    timezone_id => '{global_config['timezone_id']}',
    pause_status => '{global_config['pause_status']}'
    ) as alert;
"""
    
    return sql

def generate_all_alerts_sql(rules: Dict[str, Any]) -> str:
    """Generate SQL for all alerts"""
    
    global_config = rules['global']
    alerts = rules['alerts']
    time_intervals = rules.get('time_intervals', {})
    
    sql_parts = [
        "-- Databricks notebook source",
        "-- MAGIC %md",
        "# Databricks Security Detection Alerts \n",
        "generated from rules.yml",
        "",
        "-- COMMAND ----------",
        "use catalog identifier(:catalog);",
        "use schema identifier(:schema);",
        "-- COMMAND ----------",
        ""
    ]
    
    for alert_name, alert_config in alerts.items():
        sql_parts.append(generate_alert_sql(alert_config, global_config, time_intervals))
        sql_parts.append("")
        sql_parts.append("-- COMMAND ----------")
        sql_parts.append("")
    
    return "\n".join(sql_parts)

def main():
    """Main function to generate alerts"""
    try:
        # Load rules
        rules = load_rules()
        
        # Generate SQL
        sql_content = generate_all_alerts_sql(rules)
        
        # Get script directory and resolve output path relative to project root
        script_dir = Path(__file__).parent
        project_root = script_dir.parent.parent
        output_file = project_root / "src"/ "create_alerts.sql"
        
        # Ensure the output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write(sql_content)
        
        print(f"✅ Generated alerts SQL: {output_file}")
        print(f"📊 Total alerts generated: {len(rules['alerts'])}")
        
        # Print alert summary with time intervals
        print("\n📋 Alert Summary:")
        time_intervals = rules.get('time_intervals', {})
        for alert_name, config in rules['alerts'].items():
            interval_name = config.get('time_interval', 'default')
            hours = time_intervals.get(interval_name, time_intervals.get('default', 168))
            print(f"  • {config['display_name']}: {config['description']}")
            print(f"    Threshold: {config['comparison_operator']} {config['threshold_value']} | Time: {interval_name} ({hours}h)")
        
    except FileNotFoundError as e:
        print(f"❌ Error: rules.yml not found. Please ensure the file exists at: {e.filename}")
    except yaml.YAMLError as e:
        print(f"❌ Error parsing YAML: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()