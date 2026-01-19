import plotly.express as px
import polars as pl
from lakeview._core import get_table_history_py


def main() -> None:
    histories = get_table_history_py(".", True, None)
    records = []
    for history in histories:
        records.extend(history.to_dict())

    df = pl.DataFrame(records, infer_schema_length=None)

    df = df.with_columns(pl.from_epoch("timestamp", time_unit="ms").alias("timestamp"))
    fig = px.line(
        df,
        x="version",
        y="total_rows",
        color="table_path",
        facet_col="table_path",
        facet_col_wrap=1,
        template="ggplot2",
    )

    fig.update_xaxes(matches=None, showticklabels=True)
    fig.update_yaxes(matches=None)

    fig.show()


if __name__ == "__main__":
    main()
