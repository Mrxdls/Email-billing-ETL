"""
eml_to_json.py — convert raw .eml files into the bronze JSON record shape.

Produces the EXACT same record GmailSync._save() writes to S3 (same keys, same
order), so .eml files (manual exports, test fixtures in ./testData) are a true
stand-in for real pipeline data and flow through the SAME Silver extractor
(cleanJson.extract_receipt) unchanged.

Output record (identical to the bronze S3 payload):
    { id, message_id, account_id, user_id, sender, receiver, subject, date,
      body_html, body_text, label_ids, snippet }

CLI:
    # one file → JSON beside it
    python -m etl.rawJSON.eml_to_json "etl/rawJSON/testData/Rapido Invoice (1).eml"
    # a whole directory → an output dir
    python -m etl.rawJSON.eml_to_json etl/rawJSON/testData -o etl/rawJSON/testData/json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from email import policy
from email.parser import BytesParser
from pathlib import Path


def _bodies(msg) -> tuple[str | None, str | None]:
    """Return (html, text) bodies via the modern email API, with a walk fallback."""
    html = text = None

    # Preferred: policy.default's structured body accessors.
    try:
        html_part = msg.get_body(preferencelist=("html",))
        if html_part is not None:
            html = html_part.get_content()
        text_part = msg.get_body(preferencelist=("plain",))
        if text_part is not None:
            text = text_part.get_content()
    except Exception:
        pass

    # Fallback: walk parts (covers odd/malformed multipart trees).
    if html is None or text is None:
        for part in msg.walk():
            if part.get_content_maintype() != "text" or part.is_attachment():
                continue
            ctype = part.get_content_type()
            try:
                content = part.get_content()
            except Exception:
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                content = payload.decode(part.get_content_charset() or "utf-8", "replace")
            if ctype == "text/html" and html is None:
                html = content
            elif ctype == "text/plain" and text is None:
                text = content

    return html, text


def _snippet(text: str | None, html: str | None, limit: int = 200) -> str:
    """Short preview from the text body, falling back to stripped HTML."""
    body = text or (re.sub(r"<[^>]+>", " ", html) if html else "")
    return " ".join(body.split())[:limit]


def eml_to_record(raw: bytes, *, account_id=None, user_id=None) -> dict:
    """
    Parse raw .eml bytes into the bronze JSON record dict — IDENTICAL in keys
    and order to the payload GmailSync._save() writes to S3.
    """
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    html, text = _bodies(msg)
    return {
        "id": str(uuid.uuid4()),                 # mirrors the bronze row id / S3 name
        "message_id": (msg.get("Message-ID") or "").strip().strip("<>") or None,
        "account_id": account_id,
        "user_id": user_id,
        "sender": msg.get("From"),
        "receiver": msg.get("To"),
        "subject": msg.get("Subject"),
        "date": msg.get("Date"),
        "body_html": html,
        "body_text": text,
        "label_ids": [],
        "snippet": _snippet(text, html),
    }


def convert_file(path: Path, out_dir: Path | None) -> Path:
    """Convert one .eml file to a .json file; returns the output path."""
    record = eml_to_record(path.read_bytes())
    if not record["message_id"]:
        record["message_id"] = path.stem        # .eml without a Message-ID header
    out_path = (out_dir or path.parent) / f"{path.stem}.json"
    out_path.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Convert .eml file(s) to bronze JSON records.")
    ap.add_argument("input", help="a .eml file OR a directory containing .eml files")
    ap.add_argument("-o", "--output-dir", help="where to write .json (default: beside the input)")
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    out_dir = Path(args.output_dir) if args.output_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    if in_path.is_dir():
        files = sorted(in_path.glob("*.eml"))
        if not files:
            print(f"No .eml files in {in_path}", file=sys.stderr)
            return 1
    elif in_path.is_file():
        files = [in_path]
    else:
        print(f"Not found: {in_path}", file=sys.stderr)
        return 1

    failures = 0
    for f in files:
        try:
            out = convert_file(f, out_dir)
            print(f"ok: {f.name} -> {out}")
        except Exception as e:
            failures += 1
            print(f"FAILED: {f.name}: {e}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
