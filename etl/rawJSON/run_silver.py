"""
run_silver.py — run the Silver extractor over bronze JSON and write results to a
SEPARATE folder (keeps bronze inputs and silver outputs apart).

Reads bronze JSON records (from eml_to_json.py or the live pipeline), runs
cleanJson.extract_receipt on each, and writes one silver JSON per input into the
output folder.

CLI:
    python -m etl.rawJSON.run_silver etl/rawJSON/testData/json -o etl/rawJSON/testData/silver
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:                                             # works as `python -m etl.rawJSON.run_silver`
    from etl.rawJSON.cleanJson import extract_receipt
except ImportError:                              # ...and when run from inside the folder
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from cleanJson import extract_receipt


def process_file(path: Path, out_dir: Path) -> tuple[Path, dict]:
    record = json.loads(path.read_text(encoding="utf-8"))
    result = extract_receipt(record)
    out_path = out_dir / f"{path.stem}.json"
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path, result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Run Silver extraction over bronze JSON → separate folder.")
    ap.add_argument("input", help="bronze JSON file OR directory")
    ap.add_argument("-o", "--output-dir", required=True, help="folder to write silver results into")
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if in_path.is_dir():
        files = sorted(in_path.glob("*.json"))
    elif in_path.is_file():
        files = [in_path]
    else:
        print(f"Not found: {in_path}", file=sys.stderr)
        return 1
    if not files:
        print(f"No .json files in {in_path}", file=sys.stderr)
        return 1

    failures = 0
    for f in files:
        try:
            out, res = process_file(f, out_dir)
            print(f"ok: {f.name} -> {out}  "
                  f"(vendor={res['vendor']}, total={res['total_amount']}, {res['extraction_status']})")
        except Exception as e:
            failures += 1
            print(f"FAILED: {f.name}: {e}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
