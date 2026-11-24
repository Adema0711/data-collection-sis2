"""Summarize and plot the cleaned population data."""
import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from cleaner import CLEANED_CSV, END_YEAR, START_YEAR
from config import BASE_DIR

ANALYSIS_DIR = BASE_DIR / "analysis"


def save_plot(ax, path, title, xlabel, ylabel):
    ax.set(title=title, xlabel=xlabel, ylabel=ylabel)
    ax.grid(axis="y", alpha=0.23)
    ax.figure.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(ax.figure)


def run_analysis(source=CLEANED_CSV, output=ANALYSIS_DIR):
    output = Path(output)
    data = pd.read_csv(source)
    if data.empty:
        raise ValueError("The cleaned CSV has no rows to analyze.")
    # --- Dataset overview: dimensions, data types, descriptive statistics ---
    print(f"Dataset shape: {data.shape[0]} rows x {data.shape[1]} columns")
    print("Data types:")
    print(data.dtypes.to_string())
    descriptive = data[["year", "population"]].describe().round(2)
    print("Descriptive statistics:")
    print(descriptive.to_string())

    # --- Categorical statistics ---
    categorical = pd.concat({
        column: data[column].value_counts().rename("records")
        for column in ["region", "income_level"]
    }).rename_axis(["column", "value"]).reset_index()
    print("Categorical statistics:")
    print(categorical.to_string(index=False))

    # --- Grouped analysis by region and income level (latest year) ---
    by_group = (
        data[data["year"].eq(END_YEAR)]
        .groupby(["region", "income_level"])["population"]
        .agg(countries="count", total="sum", mean="mean")
        .reset_index()
    )
    print(f"Population by region and income level, {END_YEAR}:")
    print(by_group.to_string(index=False))

    trends = data.pivot_table(index="year", columns="country_code", values="population", aggfunc="sum")
    endpoints = trends.reindex([START_YEAR, END_YEAR]).T.dropna()
    if endpoints.empty:
        raise ValueError("No country has population data for both endpoint years.")
    growth = (
        np.divide(endpoints[END_YEAR], endpoints[START_YEAR]).sub(1).mul(100)
        .sort_values().rename("growth_percent")
    )
    names = data.drop_duplicates("country_code").set_index("country_code")["country_name"]
    latest = data.loc[data["year"].eq(END_YEAR)].sort_values("population")
    summary = data.groupby(["country_code", "country_name"])["population"].agg(
        years="count", minimum="min", maximum="max", mean="mean",
        median="median", standard_deviation="std",
    )
    recent_kazakhstan = data.loc[
        data["country_code"].eq("KAZ") & data["year"].between(2015, END_YEAR)
    ]

    output.mkdir(parents=True, exist_ok=True)
    summary.round(2).to_csv(output / "population_statistics.csv")
    descriptive.to_csv(output / "descriptive_statistics.csv")
    categorical.to_csv(output / "categorical_statistics.csv", index=False)
    by_group.to_csv(output / "population_by_region_income.csv", index=False)
    trends.to_csv(output / "population_by_year.csv")
    recent_kazakhstan.to_csv(output / "kazakhstan_2015_2024.csv", index=False)
    latest[["country_code", "country_name", "population"]].iloc[::-1].to_csv(
        output / "population_ranking.csv", index=False
    )
    growth.sort_values(ascending=False).round(2).to_csv(output / "population_growth.csv")

    ax = trends.div(1e6).plot(figsize=(10, 5), linewidth=2, logy=True)
    ax.legend(title="Country code", bbox_to_anchor=(1.02, 1), loc="upper left")
    save_plot(ax, output / "01_population_trends.png",
              "Population over time", "Year", "Population (millions, log scale)")

    colors = np.where(growth < 0, "#b84a4a", "#277a69")
    ax = growth.rename(index=names).plot.barh(figsize=(9, 5), color=colors)
    ax.axvline(0, color="#444444", linewidth=0.8)
    save_plot(ax, output / "02_population_growth.png",
              f"Population change, {START_YEAR}–{END_YEAR}", "Change (%)", "")

    ax = latest.set_index("country_name")["population"].div(1e6).plot.barh(
        figsize=(9, 5), color="#386ba3"
    )
    save_plot(ax, output / "03_population_ranking.png",
              f"Population ranking, {END_YEAR}", "Population (millions)", "")

    leader = latest.iloc[-1]
    smallest = latest.iloc[0]
    top_growth, bottom_growth = growth.index[-1], growth.index[0]
    shrinking = [names[c] for c in growth.index if growth[c] < 0]
    big_region = by_group.groupby("region")["total"].sum().sort_values()
    findings = [
        "# Population analysis",
        f"Analyzed {len(data)} records across {len(names)} countries "
        f"({START_YEAR}-{END_YEAR}). Growth could be calculated for {len(growth)} countries "
        f"with both endpoint years.",
        "## Chart 1: Population over time (01_population_trends.png)",
        f"Each line is one country. A log scale is used because populations differ by almost "
        f"two orders of magnitude, so on a linear axis small countries would look flat. "
        f"In {END_YEAR} the sample ranged from {smallest['population']:,} ({smallest['country_name']}) "
        f"to {leader['population']:,} ({leader['country_name']}). Parallel lines mean similar "
        f"relative growth; a flat or falling line means stagnation or decline.",
        "## Chart 2: Population change (02_population_growth.png)",
        f"Bars show the percentage change between {START_YEAR} and {END_YEAR}. "
        f"{names[top_growth]} grew the most ({growth.iloc[-1]:+.2f}%) and {names[bottom_growth]} "
        f"the least ({growth.iloc[0]:+.2f}%). "
        + (f"Countries with a decline: {', '.join(shrinking)}." if shrinking
           else "No country in the sample lost population."),
        f"## Chart 3: Ranking in {END_YEAR} (03_population_ranking.png)",
        f"{leader['country_name']} ranked first with {leader['population']:,} people. "
        f"The ranking shows absolute size, while chart 2 shows speed of change: "
        f"a large country can grow slowly in percent but still add the most people.",
        "## Grouped analysis",
        f"By region, the largest total population in {END_YEAR} is in {big_region.index[-1]} "
        f"({big_region.iloc[-1]:,}). Details are in population_by_region_income.csv, "
        f"descriptive_statistics.csv and categorical_statistics.csv.",
        "## Conclusions",
        "Population sizes in the sample are very uneven, growth rates differ strongly between "
        "countries, and absolute size does not predict growth speed.",
    ]
    if "KAZ" in endpoints.index:
        start, end = endpoints.loc["KAZ", [START_YEAR, END_YEAR]]
        findings.append(f"Kazakhstan went from {start:,.0f} to {end:,.0f} people "
                        f"({growth.loc['KAZ']:+.2f}%).")
    (output / "findings.md").write_text("\n\n".join(findings) + "\n", encoding="utf-8")
    print(f"Analysis saved to {output}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=CLEANED_CSV)
    parser.add_argument("--output-dir", type=Path, default=ANALYSIS_DIR)
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error(f"{args.input} is missing. Run cleaner.py first.")
    run_analysis(args.input, args.output_dir)