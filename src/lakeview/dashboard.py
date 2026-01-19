from os.path import commonpath
from pathlib import Path

import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import ALL, Dash, Input, Output, State, dcc, html

from lakeview._core import get_table_history_py

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


def create_empty_figure():
    """Create an empty figure with just the background color"""
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=PALETTE["background"],
        plot_bgcolor=PALETTE["surface"],
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        margin=dict(l=0, r=0, t=0, b=0),
    )
    return fig


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


def deserialize_dataframe(df_dicts):
    if not df_dicts:
        return pl.DataFrame()

    df = pl.from_dicts(df_dicts)

    if "timestamp" in df.columns:
        df = df.with_columns(
            pl.col("timestamp").str.to_datetime().cast(pl.Datetime("ms"))
        )

    return df


def get_all_table_info():
    all_histories = get_table_history_py("./tests/data/delta", True, None)

    all_table_paths = []
    for history in all_histories:
        if history.to_dict():
            table_path = history.to_dict()[0]["table_path"]
            all_table_paths.append(table_path)

    all_table_names = []
    if len(all_table_paths) > 1:
        common_prefix = commonpath(all_table_paths)
        if not common_prefix.endswith("/"):
            common_prefix += "/"
        if not common_prefix.startswith("./"):
            common_prefix = "./" + common_prefix
        all_table_names = [
            path.replace(common_prefix, "", 1) for path in all_table_paths
        ]
    else:
        all_table_names = [path.split("/")[-1] for path in all_table_paths]

    return all_histories, all_table_names, all_table_paths


def load_specific_tables(table_names_to_load):
    """Load data for specific tables by their formatted names"""
    all_histories, all_table_names, all_table_paths = get_all_table_info()

    name_to_path = dict(zip(all_table_names, all_table_paths))

    records = []
    for table_name in table_names_to_load:
        if table_name in name_to_path:
            target_path = name_to_path[table_name]
            for history in all_histories:
                if (
                    history.to_dict()
                    and history.to_dict()[0]["table_path"] == target_path
                ):
                    records.extend(history.to_dict())
                    break

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records, infer_schema_length=None)
    df = df.with_columns(
        pl.from_epoch("timestamp", time_unit="ms")
        .cast(pl.Datetime("ms"))
        .alias("timestamp")
    )

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
                pl.col("table_path")
                .str.split("/")
                .list.last()
                .alias("table_name")
            )

    return df


def load_data(max_tables=5):
    """Load initial data for the first N tables"""
    all_histories, all_table_names, _ = get_all_table_info()

    records = []
    for history in all_histories[:max_tables]:
        records.extend(history.to_dict())

    df = pl.DataFrame(records, infer_schema_length=None)
    df = df.with_columns(
        pl.from_epoch("timestamp", time_unit="ms")
        .cast(pl.Datetime("ms"))
        .alias("timestamp")
    )

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
                pl.col("table_path")
                .str.split("/")
                .list.last()
                .alias("table_name")
            )

    return df, all_table_names


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
    summary_pd["Last Updated"] = summary_pd["Last Updated"].dt.strftime(
        "%a, %b %d, %Y %H:%m %p"
    )

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
    fig.update_xaxes(
        matches=None, showticklabels=True, gridcolor=PALETTE["muted"]
    )
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"], title_text="")
    fig.for_each_annotation(
        lambda a: a.update(
            text=a.text.split("=")[-1], font=dict(size=16, weight=700)
        )
    )
    return fig


def create_size_fig(df):
    num_tables = df["table_name"].n_unique()
    df_mb = df.with_columns(
        (pl.col("total_bytes") / (1024 * 1024)).alias("total_mb")
    )
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
    fig.update_xaxes(
        matches=None, showticklabels=True, gridcolor=PALETTE["muted"]
    )
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"], title_text="")
    fig.for_each_annotation(
        lambda a: a.update(
            text=a.text.split("=")[-1], font=dict(size=16, weight=700)
        )
    )
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
        [
            pl.col("files_added").fill_null(0),
            pl.col("files_removed").fill_null(0),
        ]
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
    fig.update_xaxes(
        matches=None, showticklabels=True, gridcolor=PALETTE["muted"]
    )
    fig.update_yaxes(matches=None, gridcolor=PALETTE["muted"])
    fig.for_each_annotation(
        lambda a: a.update(
            text=a.text.split("=")[-1], font=dict(size=16, weight=700)
        )
    )
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
        style={
            "background-color": PALETTE["surface"],
            "color": PALETTE["text"],
        },
    )


ASSETS_PATH = Path(__file__).parent / "assets"

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder=str(ASSETS_PATH),
)

df, all_tables = load_data(max_tables=5)
summary = create_summary_stats(df)

all_tables = sorted(all_tables)
loaded_tables = sorted(df["table_name"].unique().to_list())

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H1(
                            "lakeview",
                            className="mb-4 mt-4",
                            style={
                                "color": PALETTE["primary"],
                                "font-weight": "700",
                            },
                        ),
                    ],
                    width=6,
                ),
                dbc.Col(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        dbc.DropdownMenu(
                                            [
                                                dbc.Input(
                                                    id="table-search",
                                                    placeholder="Search tables...",
                                                    type="text",
                                                    style={
                                                        "margin": "5px 10px",
                                                        "width": "calc(100% - 20px)",
                                                        "background-color": PALETTE[
                                                            "background"
                                                        ],
                                                        "color": PALETTE[
                                                            "text"
                                                        ],
                                                        "border-color": PALETTE[
                                                            "muted"
                                                        ],
                                                    },
                                                ),
                                                html.Hr(
                                                    style={
                                                        "margin": "5px 0",
                                                        "border-color": PALETTE[
                                                            "muted"
                                                        ],
                                                    }
                                                ),
                                                html.Div(
                                                    id="table-checklist-container",
                                                    style={
                                                        "max-height": "300px",
                                                        "overflow-y": "auto",
                                                        "padding": "5px",
                                                    },
                                                ),
                                            ],
                                            id="table-dropdown",
                                            label=f"{len(loaded_tables)} tables selected",
                                            style={
                                                "width": "300px",
                                                "background-color": PALETTE[
                                                    "surface"
                                                ],
                                                "border-color": PALETTE["text"],
                                            },
                                            toggle_style={
                                                "background-color": PALETTE[
                                                    "surface"
                                                ],
                                                "color": PALETTE["text"],
                                                "border-color": PALETTE["text"],
                                                "min-height": "45px",
                                                "width": "300px",
                                                "text-align": "left",
                                                "padding": "10px 15px",
                                            },
                                            className="custom-table-dropdown",
                                        ),
                                        dcc.Store(
                                            id="table-filter",
                                            data=loaded_tables,
                                        ),
                                        dcc.Store(
                                            id="dataframe-store",
                                            data=df.to_dicts(),
                                        ),
                                    ],
                                    style={"display": "inline-block"},
                                ),
                            ],
                            style={
                                "display": "flex",
                                "align-items": "center",
                                "justify-content": "flex-end",
                                "height": "100%",
                                "padding-top": "20px",
                            },
                        )
                    ],
                    width=6,
                ),
            ],
            style={"margin-bottom": "20px"},
        ),
        html.Hr(
            style={"border-color": PALETTE["muted"], "margin-bottom": "30px"}
        ),
        dbc.Row(
            [dbc.Col([html.Div(id="summary-table")])],
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
                                    [dcc.Graph(id="record-chart")], width=6
                                ),
                                dbc.Col([dcc.Graph(id="size-chart")], width=6),
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
                                    [dcc.Graph(id="operations-chart")],
                                    width=12,
                                )
                            ],
                            className="mt-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [dcc.Graph(id="file-churn-chart")],
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
                                    [dcc.Graph(id="timeline-chart")],
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


@app.callback(
    Output("dataframe-store", "data"),
    Input("table-filter", "data"),
    State("dataframe-store", "data"),
)
def load_selected_tables(selected_tables, current_df_dicts):
    """Dynamically load data for selected tables"""
    if not selected_tables:
        return current_df_dicts

    current_df = deserialize_dataframe(current_df_dicts)

    loaded_tables_list = []
    if not current_df.is_empty() and "table_name" in current_df.columns:
        loaded_tables_list = current_df["table_name"].unique().to_list()

    tables_to_load = [t for t in selected_tables if t not in loaded_tables_list]

    if tables_to_load:
        new_df = load_specific_tables(tables_to_load)

        if not new_df.is_empty():
            if current_df.is_empty():
                current_df = new_df
            else:
                current_df = pl.concat([current_df, new_df], how="diagonal")

    return current_df.to_dicts()


@app.callback(
    Output("table-checklist-container", "children"),
    Input("table-search", "value"),
    Input("table-filter", "data"),
)
def update_checklist(search_value, selected_tables):
    """Populate the checklist with filtered tables"""
    filtered_tables = all_tables
    if search_value:
        filtered_tables = [
            t for t in all_tables if search_value.lower() in t.lower()
        ]

    checklist_items = []
    for table in filtered_tables:
        is_checked = table in selected_tables
        checklist_items.append(
            dbc.Checkbox(
                id={"type": "table-checkbox", "index": table},
                label=table,
                value=is_checked,
                style={
                    "color": PALETTE["text"],
                    "padding": "5px 10px",
                    "cursor": "pointer",
                },
                className="custom-checkbox",
            )
        )

    if not checklist_items:
        return html.Div(
            "No tables found",
            style={
                "color": PALETTE["muted"],
                "padding": "10px",
                "text-align": "center",
            },
        )

    return checklist_items


@app.callback(
    Output("table-filter", "data"),
    Input({"type": "table-checkbox", "index": ALL}, "value"),
    State({"type": "table-checkbox", "index": ALL}, "id"),
    State("table-filter", "data"),
    State("table-search", "value"),
)
def update_selected_tables(
    checkbox_values, checkbox_ids, current_selection, search_value
):
    """Update the store based on checkbox selections, preserving hidden selections"""
    if not checkbox_ids:
        return loaded_tables

    visible_tables = all_tables
    if search_value:
        visible_tables = [
            t for t in all_tables if search_value.lower() in t.lower()
        ]

    visible_selected = {
        checkbox_ids[i]["index"]
        for i, is_checked in enumerate(checkbox_values)
        if is_checked
    }

    hidden_tables = set(all_tables) - set(visible_tables)

    hidden_selected = {t for t in current_selection if t in hidden_tables}

    all_selected = list(visible_selected | hidden_selected)

    return all_selected if all_selected else loaded_tables


@app.callback(Output("table-dropdown", "label"), Input("table-filter", "data"))
def update_dropdown_label(selected_tables):
    """Update the dropdown button label with selection count"""
    if not selected_tables:
        return "No tables selected"
    count = len(selected_tables)
    return f"{count} table{'s' if count != 1 else ''} selected"


@app.callback(
    Output("summary-table", "children"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_summary_table(selected_tables, df_dicts):
    if not df_dicts or not selected_tables:
        return html.Div()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    summary = create_summary_stats(filtered_df)
    return create_summary_table(summary)


@app.callback(
    Output("record-chart", "figure"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_record_chart(selected_tables, df_dicts):
    if not selected_tables or not df_dicts:
        return create_empty_figure()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    return create_record_fig(filtered_df)


@app.callback(
    Output("size-chart", "figure"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_size_chart(selected_tables, df_dicts):
    if not selected_tables or not df_dicts:
        return create_empty_figure()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    return create_size_fig(filtered_df)


@app.callback(
    Output("operations-chart", "figure"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_operations_chart(selected_tables, df_dicts):
    if not selected_tables or not df_dicts:
        return create_empty_figure()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    return create_operation_breakdown(filtered_df)


@app.callback(
    Output("file-churn-chart", "figure"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_file_churn_chart(selected_tables, df_dicts):
    if not selected_tables or not df_dicts:
        return create_empty_figure()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    return create_file_churn_fig(filtered_df)


@app.callback(
    Output("timeline-chart", "figure"),
    Input("table-filter", "data"),
    Input("dataframe-store", "data"),
)
def update_timeline_chart(selected_tables, df_dicts):
    if not selected_tables or not df_dicts:
        return create_empty_figure()
    current_df = deserialize_dataframe(df_dicts)
    filtered_df = current_df.filter(pl.col("table_name").is_in(selected_tables))
    return create_activity_timeline(filtered_df)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
