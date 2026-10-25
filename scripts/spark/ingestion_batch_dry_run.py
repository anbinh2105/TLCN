from __future__ import annotations

from ingestion.batch.ingest_to_bronze import (
    SOURCES,
    discover_files,
    calculate_sha256,
    load_successful_checksums,
)


def main():
    print("=" * 70)
    print("INGESTION BATCH DRY-RUN")
    print("=" * 70)

    successful_checksums = load_successful_checksums()

    total_files = 0
    already_ingested = 0
    pending_files = 0

    for source in SOURCES:
        files = discover_files(source)

        print(f"\nSOURCE: {source}")
        print(f"Files discovered: {len(files)}")

        for file_path in files:
            checksum = calculate_sha256(file_path)

            if checksum in successful_checksums:
                status = "ALREADY_INGESTED"
                already_ingested += 1
            else:
                status = "PENDING"
                pending_files += 1

            total_files += 1

            print(f"  [{status}] {file_path.name}")

    print("\n" + "=" * 70)
    print("INGESTION DRY-RUN SUMMARY")
    print("=" * 70)
    print(f"Total files       : {total_files}")
    print(f"Already ingested  : {already_ingested}")
    print(f"Pending ingestion : {pending_files}")

    if total_files == 0:
        raise RuntimeError("No Raw files discovered")

    print("No MinIO upload or metadata write was executed.")


if __name__ == "__main__":
    main()
