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
    trends = data.pivot(index="year", columns="country_code", values="population")
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
    top_growth, bottom_growth = growth.index[-1], growth.index[0]
    findings = [
        "# Population analysis",
        f"Analyzed {len(data)} records across {len(names)} countries. "
        f"Growth could be calculated for {len(growth)} countries with both endpoint years.",
        f"1. **Population over time.** The trend chart uses a log scale because country "
        f"populations differ greatly in size. In {END_YEAR}, the sample ranged from "
        f"{latest.iloc[0]['population']:,} ({latest.iloc[0]['country_name']}) "
        f"to {leader['population']:,} ({leader['country_name']}).",
        f"2. **Percentage change.** {names[top_growth]} recorded the largest change "
        f"({growth.iloc[-1]:+.2f}%), and {names[bottom_growth]} recorded "
        f"the smallest ({growth.iloc[0]:+.2f}%).",
        f"3. **Ranking in {END_YEAR}.** {leader['country_name']} ranked first "
        f"among the sampled countries, with {leader['population']:,} people.",
    ]
    if "KAZ" in endpoints.index:
        start, end = endpoints.loc["KAZ", [START_YEAR, END_YEAR]]
        findings[2] += (f" Kazakhstan went from {start:,.0f} to {end:,.0f} people "
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
