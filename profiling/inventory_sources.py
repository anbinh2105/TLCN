from pathlib import Path
from collections import defaultdict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"

SOURCES = [
    "weather",
    "rainfall",
    "water_level",
    "river_discharge",
    "tide",
]


def format_size(size_bytes):
    mb = size_bytes / (1024 * 1024)

    if mb >= 1:
        return f"{mb:.2f} MB"

    kb = size_bytes / 1024
    return f"{kb:.2f} KB"


def main():

    print("=" * 100)
    print("DATA SOURCE INVENTORY")
    print("=" * 100)

    total_files = 0
    total_size = 0

    for source in SOURCES:

        source_dir = RAW_ROOT / source

        print()
        print("=" * 100)
        print(f"SOURCE: {source}")
        print("=" * 100)

        if not source_dir.exists():
            print("Directory does not exist:")
            print(source_dir)
            continue

        files = sorted(
            [
                path
                for path in source_dir.rglob("*")
                if path.is_file()
            ]
        )

        print(f"Directory : {source_dir}")
        print(f"Files     : {len(files)}")

        extension_count = defaultdict(int)
        source_size = 0

        for file_path in files:

            extension = file_path.suffix.lower()

            extension_count[extension] += 1

            size = file_path.stat().st_size
            source_size += size

            relative_path = file_path.relative_to(
                PROJECT_ROOT
            )

            print(
                f"  {relative_path} "
                f"| {extension} "
                f"| {format_size(size)}"
            )

        print()
        print("Formats:")

        for extension, count in sorted(
            extension_count.items()
        ):
            print(
                f"  {extension}: {count}"
            )

        print(
            f"Total source size: "
            f"{format_size(source_size)}"
        )

        total_files += len(files)
        total_size += source_size

    print()
    print("=" * 100)
    print("GLOBAL SUMMARY")
    print("=" * 100)

    print(f"Total files: {total_files}")
    print(
        f"Total size : {format_size(total_size)}"
    )


if __name__ == "__main__":
    main()