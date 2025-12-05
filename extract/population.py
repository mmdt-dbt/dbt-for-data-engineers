# ...existing code...
import argparse
from pathlib import Path
import pandas as pd


def load_all_sheets(xlsx_path: Path) -> dict:
    """Load all sheets from an Excel file as a dict of DataFrames."""
    return pd.read_excel(xlsx_path, sheet_name=None, header=None)


def get_population_data(region: str, township: str, df: pd.DataFrame) -> pd.DataFrame:
    """Extract population data from the given DataFrame."""

    print(f"Input Dataframe \n", df)
    result_df = pd.DataFrame(
        [
            {
                "region": region,
                "township": township,
                "total_population": df.iloc[2, 0],
                "male_population": df.iloc[2, 1],
                "female_population": df.iloc[2, 2],
            }
        ]
    )

    print(f"Result Dataframe \n", result_df)
    return result_df


def main():
    data_dir = Path.cwd() / "population_excel_files"
    if not data_dir.exists():
        raise SystemExit(f"Data directory not found: {data_dir.resolve()}")

    excel_files = sorted([p for p in data_dir.glob("*.xls*") if p.is_file()])
    if not excel_files:
        print(f"No Excel files found in {data_dir.resolve()}")
        return

    out_root = Path.cwd() / "output"
    population_df = pd.DataFrame()

    for xlsx in excel_files:
        print(f"\nProcessing {xlsx.name} ...")
        sheets = load_all_sheets(xlsx)

        # Skip the first sheet (preserve original sheet order)
        sheet_items = list(sheets.items())[1:]
        if not sheet_items:
            print(f"No sheets to process after skipping first sheet in {xlsx.name}")
            continue

        print(f"Loaded {len(sheets)} sheets from {xlsx}")

        for name, df in sheets.items():
            print(f"\n")
            print(f"- Sheet: {name!r}, rows: {df.shape[0]}, cols: {df.shape[1]}")

            if df.shape[0] == 0:
                print(f"  Skipping empty sheet: {name!r}")
                continue

            if (
                name.startswith("List")
                or name.startswith("list")
                or name.startswith("Lest")
                or name.startswith("Lint")
                or name.startswith("Link")
            ):
                print(f"  Skipping list sheet: {name!r}")
                continue

            df = df.iloc[2:].reset_index(drop=True)
            df = df.head(3).reset_index(drop=True)

            if df.shape[1] > 2:
                df = df.iloc[:, 2:].reset_index(drop=True)

            print(df)

            region = xlsx.stem
            township = name

            print(f" Region: {region}, Township: {township} ")

            township_df = get_population_data(region, township, df)
            population_df = pd.concat([population_df, township_df], ignore_index=True)

    population_df.to_csv("output/population_data.csv", index=False)


if __name__ == "__main__":
    main()
