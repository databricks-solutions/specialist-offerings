"""Generate synthetic home-insurance policies + claims JSON files with injected DQ issues,
then upload them to /Volumes/alexn/sdp_demo/files/{policies,claims}/.

Uses Python stdlib only (no faker/mimesis dependency).

Run locally:
    python3 setup/03_generate_raw_data.py
"""

from __future__ import annotations

import argparse
import io
import json
import random
import sys
from datetime import date, timedelta

from databricks.sdk import WorkspaceClient

CATALOG = "alexn"
SCHEMA = "sdp_demo"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/files"

PERILS_OK = ["fire", "flood", "wind", "theft", "liability", "water_damage", "other"]
COVERAGES_OK = ["basic", "standard", "premium"]
POLICY_STATUSES_OK = ["active", "lapsed", "cancelled", "pending"]
CLAIM_STATUSES_OK = ["open", "approved", "denied", "paid", "closed"]

FIRST_NAMES = [
    "Aiden", "Olivia", "Liam", "Emma", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "Logan", "Mia", "Lucas", "Charlotte", "Jackson", "Amelia", "Levi",
    "Harper", "James", "Evelyn", "Benjamin", "Abigail", "Henry", "Emily", "Alexander",
    "Elizabeth", "Sebastian", "Sofia", "Daniel", "Madison", "Matthew", "Avery",
    "Samuel", "Ella", "David", "Scarlett", "Joseph", "Grace", "Carter", "Chloe",
    "Owen", "Victoria", "Wyatt", "Riley", "John", "Aria", "Jack", "Lily", "Luke",
    "Aubrey", "Jayden", "Zoey", "Dylan", "Penelope", "Grayson", "Lillian", "Isaac",
    "Addison", "Gabriel", "Layla", "Julian", "Natalie", "Mateo", "Camila", "Anthony",
    "Hannah", "Jaxon", "Brooklyn",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts",
]
STREETS = [
    "Maple", "Oak", "Pine", "Cedar", "Elm", "Walnut", "Chestnut", "Birch", "Spruce",
    "Willow", "Sycamore", "Magnolia", "Cypress", "Aspen", "Hickory", "Juniper",
    "Locust", "Poplar", "Linden", "Dogwood",
]
SUFFIXES = ["St", "Ave", "Rd", "Blvd", "Ln", "Dr", "Ct", "Way", "Pl", "Ter"]
CITIES = [
    "Springfield", "Riverside", "Franklin", "Greenville", "Bristol", "Clinton",
    "Fairview", "Salem", "Madison", "Georgetown", "Arlington", "Burlington",
    "Manchester", "Oxford", "Kingston", "Newport", "Auburn", "Dover", "Milford",
]
STATES = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "NJ", "VA"]
DESCRIPTIONS = [
    "Tree fell on roof during storm.",
    "Kitchen fire caused minor smoke damage.",
    "Burst pipe in basement flooded utility room.",
    "Theft of electronics from living room.",
    "Hail damage to siding and gutters.",
    "Lightning strike caused electrical damage.",
    "Slip and fall on icy walkway, guest injured.",
    "Roof leak during heavy rain, ceiling damage.",
    "Vandalism to garage door overnight.",
    "Mold discovered after slow leak under sink.",
    "Wind damage to fence and patio cover.",
    "Smoke damage from neighboring unit fire.",
]


def _name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _email_for(name: str, i: int) -> str:
    user = name.lower().replace(" ", ".")
    return f"{user}{i}@example.com"


def _address() -> str:
    return f"{random.randint(100, 9999)} {random.choice(STREETS)} {random.choice(SUFFIXES)}, " \
           f"{random.choice(CITIES)}, {random.choice(STATES)}"


def _zip() -> str:
    return f"{random.randint(10000, 99999)}"


def _rand_past_date(years_back: int = 5) -> date:
    today = date.today()
    return today - timedelta(days=random.randint(1, 365 * years_back))


def _maybe(prob: float) -> bool:
    return random.random() < prob


def generate_policies(n: int) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        name = _name()
        email = _email_for(name, i)
        zipc = _zip()
        coverage = random.choice(COVERAGES_OK)
        premium = round(random.uniform(300, 2500), 2)
        eff = _rand_past_date(3)
        exp = eff + timedelta(days=365)
        status = random.choice(POLICY_STATUSES_OK)

        # Inject realistic DQ issues
        if _maybe(0.03):
            email = None
        elif _maybe(0.02):
            email = f"not-an-email-{i}"
        if _maybe(0.02):
            zipc = "ABC12"
        if _maybe(0.01):
            coverage = "gold"
        if _maybe(0.01):
            eff = date.today() + timedelta(days=random.randint(1, 60))
        if _maybe(0.01):
            premium = -round(random.uniform(50, 500), 2)
        if _maybe(0.01):
            status = "expired"

        rows.append({
            "policy_id": 100_000 + i,
            "holder_name": name,
            "holder_email": email,
            "property_address": _address(),
            "property_zip": zipc,
            "coverage_type": coverage,
            "premium_amount": premium,
            "effective_date": eff.isoformat(),
            "expiration_date": exp.isoformat(),
            "status": status,
        })
    return rows


def generate_claims(n: int, policy_ids: list[int]) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        policy_id = random.choice(policy_ids)
        cdate = _rand_past_date(2)
        amount = round(random.uniform(100, 25_000), 2)
        peril = random.choice(PERILS_OK)
        status = random.choice(CLAIM_STATUSES_OK)
        desc = random.choice(DESCRIPTIONS)

        # Inject DQ issues
        if _maybe(0.02):
            amount = -round(random.uniform(10, 500), 2)
        if _maybe(0.01):
            cdate = date.today() + timedelta(days=random.randint(1, 30))
        if _maybe(0.01):
            peril = None
        elif _maybe(0.01):
            peril = "earthquake"
        if _maybe(0.01):
            status = "archived"

        rows.append({
            "claim_id": 900_000 + i,
            "policy_id": policy_id,
            "claim_date": cdate.isoformat(),
            "claim_amount": amount,
            "peril_type": peril,
            "description": desc,
            "status": status,
        })
    return rows


def _to_jsonl_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    for r in rows:
        buf.write(json.dumps(r))
        buf.write("\n")
    return buf.getvalue().encode("utf-8")


def upload(w: WorkspaceClient, volume_path: str, payload: bytes) -> None:
    w.files.upload(file_path=volume_path, contents=io.BytesIO(payload), overwrite=True)
    print(f"  -> uploaded {volume_path} ({len(payload):,} bytes)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-policies", type=int, default=3_000)
    parser.add_argument("--n-claims", type=int, default=15_000)
    parser.add_argument("--n-files", type=int, default=3,
                        help="Split each entity across N files (simulate landing batches)")
    parser.add_argument("--suffix", default="",
                        help="Filename suffix (e.g. '_batch2') for additional landings")
    parser.add_argument("--profile", default="DEFAULT")
    args = parser.parse_args()

    random.seed(42 if not args.suffix else 7777)
    w = WorkspaceClient(profile=args.profile)
    print(f"Authenticated as {w.current_user.me().user_name}")

    print(f"\nGenerating {args.n_policies} policies...")
    policies = generate_policies(args.n_policies)
    policy_ids = [p["policy_id"] for p in policies]

    print(f"Generating {args.n_claims} claims...")
    claims = generate_claims(args.n_claims, policy_ids)

    suf = args.suffix
    print(f"\nUploading to {VOLUME_ROOT}/ ...")
    chunk = max(1, len(policies) // args.n_files)
    for i in range(args.n_files):
        part = policies[i * chunk: (i + 1) * chunk] if i < args.n_files - 1 else policies[i * chunk:]
        upload(w, f"{VOLUME_ROOT}/policies/policies{suf}_part{i:02d}.json", _to_jsonl_bytes(part))

    chunk = max(1, len(claims) // args.n_files)
    for i in range(args.n_files):
        part = claims[i * chunk: (i + 1) * chunk] if i < args.n_files - 1 else claims[i * chunk:]
        upload(w, f"{VOLUME_ROOT}/claims/claims{suf}_part{i:02d}.json", _to_jsonl_bytes(part))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
