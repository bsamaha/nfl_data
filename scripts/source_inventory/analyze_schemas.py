"""Summarize schemas.json: seasons, rows, column counts and the seasons where the column set or types change.

Local only; reads OUT_DIR/schemas.json written by probe_schemas.py.

Usage: python analyze_schemas.py OUT_DIR [label,label,...]
"""
import json
import re
import sys
from pathlib import Path

schemas_by_dataset = json.loads((Path(sys.argv[1]) / "schemas.json").read_text())
selected_labels = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else None
if selected_labels and selected_labels - set(schemas_by_dataset):
    unknown_labels = ", ".join(sorted(selected_labels - set(schemas_by_dataset)))
    available_labels = ", ".join(schemas_by_dataset)
    sys.exit(f"unknown label: {unknown_labels}. Labels in schemas.json: {available_labels}")


def format_preview(items, limit=14):
    item_list = list(items)
    return ", ".join(item_list[:limit]) + (f" …(+{len(item_list) - limit})" if len(item_list) > limit else "")


for dataset_label, records in schemas_by_dataset.items():
    if selected_labels and dataset_label not in selected_labels:
        continue
    # group variants (e.g. pfr pass/rush/rec/def, ngs passing/rushing/receiving) by file stem without the year
    records_by_stem: dict[str, list[dict]] = {}
    for record in records:
        stem = re.sub(r"_?(19|20)\d\d", "", record["file"]).replace(".parquet", "")
        records_by_stem.setdefault(stem, []).append(record)
    for stem, stem_records in records_by_stem.items():
        # probe_schemas.py writes a record with "error" in place of rows and columns when a footer read fails
        failed_files = sorted(record["file"] for record in stem_records if "error" in record)
        failed_seasons = {record["season"] for record in stem_records if "error" in record}
        valid_records = [record for record in stem_records if "error" not in record]
        if not valid_records:
            print(f"\n## {dataset_label} :: {stem}  no file was read; failed: {', '.join(failed_files)}")
            continue
        valid_records.sort(key=lambda record: (record["season"] or 0))
        seasons = [record["season"] for record in valid_records if record["season"]]
        row_counts = [record["rows"] for record in valid_records]
        print(
            f"\n## {dataset_label} :: {stem}  files={len(valid_records)}"
            + (f" seasons={seasons[0]}-{seasons[-1]}" if seasons else "")
            + f" rows(min/max/last)={min(row_counts)}/{max(row_counts)}/{row_counts[-1]}"
            + f" cols(first/last)={len(valid_records[0]['columns'])}/{len(valid_records[-1]['columns'])}"
            + f" last_update={max(record['updated_at'] for record in valid_records)[:16]}"
        )
        if failed_files:
            print("   FAILED to read, left out of this summary:", ", ".join(failed_files))
        if seasons:
            missing_seasons = sorted(set(range(seasons[0], seasons[-1] + 1)) - set(seasons) - failed_seasons)
            if missing_seasons:
                print("   MISSING seasons:", missing_seasons)
        previous_columns = None
        for record in valid_records:
            current_columns = dict(record["columns"])
            if previous_columns is not None:
                added_columns = [col for col in current_columns if col not in previous_columns]
                removed_columns = [col for col in previous_columns if col not in current_columns]
                retyped_columns = [
                    f"{col}:{previous_columns[col]}->{current_columns[col]}"
                    for col in current_columns
                    if col in previous_columns and previous_columns[col] != current_columns[col]
                ]
                if added_columns or removed_columns or retyped_columns:
                    print(
                        f"   {record['season'] or record['file']}: rows={record['rows']}"
                        + (f" | +{len(added_columns)}: {format_preview(added_columns)}" if added_columns else "")
                        + (f" | -{len(removed_columns)}: {format_preview(removed_columns)}" if removed_columns else "")
                        + (f" | type {len(retyped_columns)}: {format_preview(retyped_columns, 8)}" if retyped_columns else "")
                    )
            previous_columns = current_columns
        latest_record = valid_records[-1]
        print("   latest columns:", ", ".join(f"{col_name}:{col_type}" for col_name, col_type in latest_record["columns"]))
        if latest_record.get("kv"):
            print("   file metadata keys:", list(latest_record["kv"].items())[:4])
