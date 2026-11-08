# =============================================================================
# File        : make_demo_dataset.py
# Module      : scripts
# Owner       : Amine
# Description : Builds a short, mixed demo file (data/parquet/test_demo.parquet)
#               from the four test days: a sample of benign flows plus up to
#               N flows of every attack family, shuffled together after a calm
#               benign start, so a ~90 s replay shows normal traffic and every
#               attack family in Grafana.
# Input       : data/parquet/test_{tuesday,wednesday,thursday,friday}.parquet
# Output      : data/parquet/test_demo.parquet
# Usage       : py scripts/make_demo_dataset.py
# =============================================================================

import argparse
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_DIR = BASE_DIR / "data" / "parquet"
DAYS = ["test_tuesday.parquet", "test_wednesday.parquet",
        "test_thursday.parquet", "test_friday.parquet"]


def main():
    parser = argparse.ArgumentParser(description="Build the FlowGuard demo dataset")
    parser.add_argument("--per-attack", type=int, default=150, help="Max flows per attack family")
    parser.add_argument("--benign", type=int, default=2500, help="Benign flows in total")
    parser.add_argument("--calm", type=int, default=300, help="Benign flows at the very start")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    attacks, benign = [], []
    for day in DAYS:
        df = pd.read_parquet(PARQUET_DIR / day)
        is_benign = df["Label"] == "BENIGN"
        benign.append(df[is_benign].sample(n=min(args.benign // len(DAYS), int(is_benign.sum())),
                                        random_state=args.seed))
        for label, group in df[~is_benign].groupby("Label"):
            attacks.append(group.sample(n=min(args.per_attack, len(group)), random_state=args.seed))
        print(f"  {day}: {len(df):,} flows read")

    attacks = pd.concat(attacks)
    # A family present on several days keeps at most --per-attack flows in total
    attacks = attacks.groupby("Label", group_keys=False).apply(
        lambda g: g.sample(n=min(args.per_attack, len(g)), random_state=args.seed))
    benign = pd.concat(benign).sample(frac=1, random_state=args.seed)

    calm = benign.iloc[:args.calm]
    mixed = pd.concat([benign.iloc[args.calm:], attacks]).sample(frac=1, random_state=args.seed)
    demo = pd.concat([calm, mixed], ignore_index=True)

    out = PARQUET_DIR / "test_demo.parquet"
    demo.to_parquet(out, index=False)
    print(f"\nDemo file written: {out} ({len(demo):,} flows)")
    print(demo["Label"].value_counts().to_string())


if __name__ == "__main__":
    main()