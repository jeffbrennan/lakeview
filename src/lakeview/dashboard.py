from genericpath import commonprefix
import polars as pl
import plotly.express as px
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
from lakeview._core import get_table_history_py
from pathlib import Path
from os.path import commonpath

PALETTE = {
    "primary": "#e85740",
    "secondary": "#d3869b",
    "tertiary": "#8ec07c",
    "background": "#1d2021",
    "surface": "#282828",
    "text": "#ebdbb2",
    "muted": "#928374",
    "red": "#fb4934",
    "green": "#b8bb26",
    "yellow": "#fabd2f",
    "blue": "#83a598",
}

COLOR_SEQUENCE = [
    PALETTE["primary"],
    PALETTE["secondary"],
    PALETTE["tertiary"],
    PALETTE["blue"],
    PALETTE["yellow"],
    PALETTE["red"],
    PALETTE["green"],
]


def format_bytes(bytes_value) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(bytes_value)
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"


def load_data():
    histories = get_table_history_py(".", True, None)
    records = []
    for history in histories:
        records.extend(history.to_dict())

    df = pl.DataFrame(records, infer_schema_length=None)
    df = df.with_columns(pl.from_epoch("timestamp", time_unit="ms").alias("timestamp"))

    if not df.is_empty():
        paths = df["table_path"].unique().to_list()
        if len(paths) > 1:
            common_prefix = commonpath(paths)
            if not common_prefix.endswith("/"):
                common_prefix += "/"
            if not common_prefix.startswith("./"):
                common_prefix = "./" + common_prefix


            df = df.with_columns(
                pl.col("table_path")
                .str.strip_prefix(common_prefix)
                .alias("table_name")
            )
        else:
            df = df.with_columns(
                pl.col("table_path").str.split("/").list.last().alias("table_name")
            )

    return df


def create_summary_stats(df):
    summary = df.group_by("table_path").agg(
        [
            pl.col("version").max().alias("Version"),
            pl.col("total_rows").last().alias("Records"),
            pl.col("total_bytes").last().alias("Size"),
            pl.col("timestamp").max().alias("Last Updated"),
        ]
    )

    summary_pd = summary.to_pandas()
    summary_pd["Records"] = summary_pd["Records"].apply(lambda x: f"{x:,}")
    summary_pd["Size"] = summary_pd["Size"].apply(format_bytes)
    summary_pd["Last Updated"] = summary_pd["Last Updated"].dt.strftime("%a, %b %d, %Y %H:%m %p")

    return summary_pd


def create_record_fig(df):
    num_tables = df["table_name"].n_unique()
    fig = px.line(
        df,
        x="version",
        y="total_rows",
        color="table_name",
        facet_col="table_name",
        facet_col_wrap=1,
        title="Records",
        template="simple_white",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        font_color=PALETTE["text"],
        title_font_size=20,
        title_font_color=PALETTE["primary"],
        showlegend=False,
        height=300 * num_tables,
    )
    fig.update_xaxes(matches=None, showticklabels=True, gridcolor=PALETTE["muted"])
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"], title_text="")
    fig.for_each_annotation(lambda a: a.update(
        text=a.text.split("=")[-1],
        font=dict(size=16, weight=700)
    ))
    return fig


def create_size_fig(df):
    num_tables = df["table_name"].n_unique()
    df_mb = df.with_columns((pl.col("total_bytes") / (1024 * 1024)).alias("total_mb"))
    fig = px.line(
        df_mb,
        x="version",
        y="total_mb",
        color="table_name",
        facet_col="table_name",
        facet_col_wrap=1,
        title="Size (MB)",
        template="simple_white",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        font_color=PALETTE["text"],
        title_font_size=20,
        title_font_color=PALETTE["primary"],
        showlegend=False,
        height=300 * num_tables,
    )
    fig.update_xaxes(matches=None, showticklabels=True, gridcolor=PALETTE["muted"])
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"], title_text="")
    fig.for_each_annotation(lambda a: a.update(
        text=a.text.split("=")[-1],
        font=dict(size=16, weight=700)
    ))
    return fig


def create_operation_breakdown(df):
    operation_counts = df.group_by(["table_name", "operation"]).agg(
        pl.len().alias("count")
    )
    fig = px.bar(
        operation_counts,
        x="table_name",
        y="count",
        color="operation",
        title="Operations by Table",
        barmode="stack",
        template="simple_white",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        font_color=PALETTE["text"],
        title_font_size=20,
        title_font_color=PALETTE["primary"],
    )
    fig.update_xaxes(tickangle=45, gridcolor=PALETTE["muted"])
    fig.update_yaxes(gridcolor=PALETTE["muted"])
    return fig


def create_file_churn_fig(df):
    num_tables = df["table_name"].n_unique()
    df_churn = df.with_columns(
        [pl.col("files_added").fill_null(0), pl.col("files_removed").fill_null(0)]
    )

    fig = px.bar(
        df_churn,
        x="version",
        y=["files_added", "files_removed"],
        facet_col="table_name",
        facet_col_wrap=1,
        title="File Churn",
        barmode="group",
        template="simple_white",
        color_discrete_sequence=[PALETTE["secondary"], PALETTE["tertiary"]],
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        font_color=PALETTE["text"],
        title_font_size=20,
        title_font_color=PALETTE["primary"],
        height=300 * num_tables,
    )
    fig.update_xaxes(matches=None, showticklabels=True, gridcolor=PALETTE["muted"])
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"])
    fig.for_each_annotation(lambda a: a.update(
        text=a.text.split("=")[-1],
        font=dict(size=16, weight=700)
    ))
    return fig


def create_activity_timeline(df):
    activity = df.group_by(
        [pl.col("timestamp").dt.date().alias("date"), "table_name"]
    ).agg(pl.len().alias("operations"))

    fig = px.bar(
        activity,
        x="date",
        y="operations",
        color="table_name",
        title="Daily Operations",
        barmode="stack",
        template="simple_white",
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        font_color=PALETTE["text"],
        title_font_size=20,
        title_font_color=PALETTE["primary"],
    )
    fig.update_xaxes(gridcolor=PALETTE["muted"])
    fig.update_yaxes(gridcolor=PALETTE["muted"])
    return fig


def create_summary_table(summary_df):
    return dbc.Table.from_dataframe(  # pyright: ignore[reportAttributeAccessIssue]
        summary_df,
        striped=True,
        bordered=True,
        hover=True,
        responsive=True,
        className="mt-3",
        style={"background-color": PALETTE["surface"], "color": PALETTE["text"]},
    )


ASSETS_PATH = Path(__file__).parent / "assets"

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder=str(ASSETS_PATH),
)

df = load_data()
summary = create_summary_stats(df)

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H1(
                            "lakeview",
                            className="mb-4 mt-4",
                            style={"color": PALETTE["primary"], "font-weight": "700"},
                        ),
                    ]
                )
            ]
        ),
        dbc.Row(
            [dbc.Col([create_summary_table(summary)])],
            className="mb-4",
            style={
                "background-color": PALETTE["surface"],
                "border-radius": "12px",
                "padding": "20px",
            },
        ),
        dbc.Tabs(
            [
                dbc.Tab(
                    label="Trends",
                    tab_style={"background-color": PALETTE["background"]},
                    active_tab_style={
                        "background-color": PALETTE["surface"],
                        "border-top": f"3px solid {PALETTE['primary']}",
                    },
                    children=[
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(figure=create_record_fig(df))], width=6
                                ),
                                dbc.Col(
                                    [dcc.Graph(figure=create_size_fig(df))], width=6
                                ),
                            ],
                            className="mt-3",
                        )
                    ],
                ),
                dbc.Tab(
                    label="Operations",
                    tab_style={"background-color": PALETTE["background"]},
                    active_tab_style={
                        "background-color": PALETTE["surface"],
                        "border-top": f"3px solid {PALETTE['primary']}",
                    },
                    children=[
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(figure=create_operation_breakdown(df))],
                                    width=12,
                                )
                            ],
                            className="mt-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(figure=create_file_churn_fig(df))],
                                    width=12,
                                )
                            ],
                            className="mt-3",
                        ),
                    ],
                ),
                dbc.Tab(
                    label="Timeline",
                    tab_style={"background-color": PALETTE["background"]},
                    active_tab_style={
                        "background-color": PALETTE["surface"],
                        "border-top": f"3px solid {PALETTE['primary']}",
                    },
                    children=[
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(figure=create_activity_timeline(df))],
                                    width=12,
                                )
                            ],
                            className="mt-3",
                        )
                    ],
                ),
            ],
            style={"margin-bottom": "2rem"},
        ),
    ],
    fluid=True,
)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
