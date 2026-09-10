"""Rigenera la figura dai metadati versionati; non esegue esperimenti."""
from pathlib import Path
import csv
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
DATA = HERE.parents[1] / "dataset" / "output"
FIGURES = HERE / "figures"
FIGURES.mkdir(exist_ok=True)


def records(name):
    with (DATA / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


k_rows = records("edge_k_metrics.csv")
shards = records("replay_shards_summary.csv")
shards.sort(key=lambda row: int(row["edge_id"].split("-")[1]))
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 8,
    "axes.labelsize": 8, "axes.titlesize": 9,
    "xtick.labelsize": 7, "ytick.labelsize": 7,
    "pdf.fonttype": 42, "axes.spines.top": False,
    "axes.spines.right": False,
})
fig, axes = plt.subplots(1, 2, figsize=(7.05, 2.08), layout="constrained")
ks = [int(row["k"]) for row in k_rows]
scores = [float(row["silhouette"]) for row in k_rows]
best = max(range(len(scores)), key=scores.__getitem__)
axes[0].plot(ks, scores, marker="o", markersize=3.5, color="#215e86")
axes[0].scatter([ks[best]], [scores[best]], color="#b86427", s=30, zorder=3)
axes[0].annotate("k = 13; S = 0,521", (ks[best], scores[best]),
                 xytext=(7.2, 0.527), fontsize=8,
                 arrowprops={"arrowstyle": "-", "color": "#777777"})
axes[0].set(xlabel="Numero di cluster k", ylabel="Silhouette Score",
            xticks=ks, ylim=(0.465, 0.537), title="(a) Selezione della topologia")
axes[0].grid(axis="y", color="#dddddd", linewidth=0.5)
axes[1].bar(range(13), [int(row["row_count"])/1000 for row in shards],
            color="#215e86", width=0.7)
axes[1].set(xlabel="Identificativo Edge", ylabel="Osservazioni (migliaia)",
            xticks=range(13), ylim=(0, 250), title="(b) Workload sperimentale")
axes[1].grid(axis="y", color="#dddddd", linewidth=0.5)
axes[1].set_axisbelow(True)
fig.savefig(FIGURES / "workload.pdf", metadata={"Title": "Topologia e workload"})
plt.close(fig)
print(FIGURES / "workload.pdf")
