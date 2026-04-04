import argparse
import csv
from pathlib import Path


MINIMAL_PDF = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >>
endobj
trailer
<< /Root 1 0 R >>
%%EOF
"""


def parse_args():
    parser = argparse.ArgumentParser(description="Generate dummy PDF files from exported file names")
    parser.add_argument("--names-file", required=True, help="Text or CSV file containing file names")
    parser.add_argument("--output-dir", required=True, help="Directory where PDFs should be created")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of files to generate")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files if they already exist",
    )
    return parser.parse_args()


def read_names(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return list(_read_names_from_csv(path))
    return list(_read_names_from_text(path))


def _read_names_from_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames:
            file_name_column = _pick_file_name_column(reader.fieldnames)
            if file_name_column:
                for row in reader:
                    value = (row.get(file_name_column) or "").strip()
                    if value:
                        yield value
                return

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            value = (row[0] or "").strip()
            if value and value.lower() != "file_name":
                yield value


def _pick_file_name_column(fieldnames):
    lowered = {name.lower(): name for name in fieldnames if name}
    for candidate in ("file_name", "filename", "file"):
        if candidate in lowered:
            return lowered[candidate]
    return fieldnames[0] if fieldnames else None


def _read_names_from_text(path: Path):
    with path.open("r", encoding="utf-8-sig") as handle:
        for line in handle:
            value = line.strip()
            if value:
                yield value


def main():
    args = parse_args()
    names_path = Path(args.names_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    names = read_names(names_path)
    created = 0
    skipped = 0
    seen = set()

    for file_name in names:
        if args.limit and created >= args.limit:
            break
        if file_name in seen:
            skipped += 1
            continue
        seen.add(file_name)

        destination = output_dir / file_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and not args.overwrite:
            skipped += 1
            continue

        destination.write_bytes(MINIMAL_PDF)
        created += 1

    print("generated=%s skipped=%s output_dir=%s" % (created, skipped, output_dir))


if __name__ == "__main__":
    main()
