import json

from h2mi.cli import main


def test_version(capsys):
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip()


def test_config_check_json(examples, capsys):
    rc = main(["--json", "config", "check", "--env", str(examples / "env.yaml"),
               "--table", str(examples / "tables" / "payments.txn_daily.yaml")])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["tables"][0]["target"].endswith(".payments.txn_daily")


def test_ddl_plan_offline(examples, capsys):
    rc = main(["ddl", "plan", "--env", str(examples / "env.yaml"),
               "--table", str(examples / "tables" / "payments.txn_daily.yaml"),
               "--columns", "txn_id:bigint,account_id:bigint,amount:decimal(14,2),qty:tinyint,txn_date:string"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "CLUSTER BY (`txn_date`, `txn_id`)" in out and "__h2mi_stg_txn_daily" in out
    assert "-- [AUTO_MAP] qty tinyint" in out and "GRANT" in out


def test_ddl_plan_blocks_on_redesign(examples, capsys):
    rc = main(["ddl", "plan", "--env", str(examples / "env.yaml"),
               "--table", str(examples / "tables" / "payments.txn_daily.yaml"),
               "--columns", "txn_id:bigint,u:uniontype<int,string>"])
    assert rc == 4 and "NOT loadable" in capsys.readouterr().out


def test_not_implemented_commands_exit_1(capsys):
    assert main(["sync", "--env", "x.yaml"]) == 1
    assert "not implemented" in capsys.readouterr().err
