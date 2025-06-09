# %%
import numpy as np
import polars as pd
import pandas
import altair as alt
import pathlib
import json
from typing import Tuple
alt.renderers.enable("png")

# %%
output_dir = pathlib.Path.cwd().parent / "target" / "criterion" / "dyn_paper"
output_files = output_dir.glob("dyn_paper_*/**/new/estimates.json")
output_files = list(output_files)
output_files

# %%
def run_type(path: pathlib.Path) -> Tuple[str, str]:
    percent = path.parent.parent.parent.name.split("_")[-1]
    return (
        path.parent.parent.name,
        "direct" if percent == "direct" else f"{percent}%"
    )

# %%
run_type(output_files[-1])

# %%
output_files[-1].read_text()

# %%
out = []
for output_file in output_files:
    data = json.loads(output_file.read_text())["mean"]["point_estimate"]
    out.append((*run_type(output_file), data))

out

# %%
df = pd.from_records(out, {"n": pd.Int32, "percentage": pd.String, "time": pd.Duration("ns")}, orient="row")
df
# %%
df1 = df.select(n=pd.col("n"), percentage=pd.col("percentage"), time = pd.col("time").dt.total_milliseconds())
df1

# %%
holder = []
grouper = df1.to_pandas().groupby('percentage')
for a, b in grouper:
    holder.append(b[b.n==b.n.max()])

df2 = pandas.concat(holder)
df2 = pd.DataFrame(df2)
df2
# %%
list(sorted(set(df1["n"].to_list())))

# %%
c1 = df1.plot.line(x="n:Q", y="time", color=alt.Color("percentage", legend=None))

labels = alt.Chart(df1).mark_text(align='left', size=15, dx=4).encode(
    alt.X('n:Q', aggregate='max'),
    alt.Y('time', aggregate={'argmax': "n"}),
    alt.Text('percentage'),
).properties(width=800)
ticks = list(sorted(set(df1["n"].to_list())))
outplt = alt.layer(
    c1, labels
).encode(
    x=alt.X(title="# Events", axis=alt.Axis(values=ticks), scale=alt.Scale(domain=[0, ticks[-1] + 6000])),
    y=alt.Y(title="Time (ms)", scale=alt.Scale(domain=[0, 360]))
).configure_axis(
    labelFontSize=15,
    titleFontSize=16
)
outplt.save(
    "output.pdf"
)
outplt
