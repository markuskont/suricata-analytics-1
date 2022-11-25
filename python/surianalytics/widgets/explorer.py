"""
Reusable widgets
"""

from ipywidgets.widgets.interaction import display
from ..connectors import RESTSciriusConnector
from ..datamining import min_max_scaling

import ipywidgets as widgets

import pandas as pd
import numpy as np

import networkx as nx
import hvplot.networkx as hvnx
import holoviews as hv

import pickle
import os
import re


CORE_COLUMNS = ["timestamp",
                "flow_id",
                "event_type",
                "proto",
                "app_proto",
                "src_ip",
                "flow.src_ip",
                "src_port",
                "dest_ip",
                "flow.dest_ip",
                "dest_port",
                "direction",
                "payload_printable",
                "metadata.flowbits"]

DEFAULT_COLUMNS = ["timestamp",
                   "flow_id",
                   "event_type",
                   "app_proto",
                   "src_ip",
                   "src_port",
                   "dest_ip",
                   "dest_port"]

TIME_COLS = ["timestamp", "@timestamp", "http.date", "flow.start", "flow.end"]

GRAPH_RESOLUTIONS = [
    "480x360",
    "960x540",
    "1280x720",
    "1600x900",
    "1920x1080",
    "2048x1080",
    "2540x1440",
    "3840x2160"
]


class Explorer(object):

    def __init__(self, c=RESTSciriusConnector()) -> None:
        # Data connector to backend
        self._connector = c

        # Outputs
        self._output_query_feedback = widgets.Output()
        self._output_eve_explorer = widgets.Output()
        self._output_eve_agg = widgets.Output()
        self._output_uniq = widgets.Output()
        self._output_graph = widgets.Output()
        self._output_graph_feedback = widgets.Output()

        self._output_debug = widgets.Output()

        # Containers
        self.data = pd.DataFrame()
        self.data_filtered = pd.DataFrame()
        self.data_aggregate = pd.DataFrame()
        self.data_uniq = pd.DataFrame()

        self.data_graph = nx.Graph()

        self._pickle_q_time = "./params.pkl"
        if os.path.exists(self._pickle_q_time):
            params = pickle.load(open(self._pickle_q_time, "rb"), encoding="bytes")
            self._connector.set_query_timeframe(params["time"][0], params["time"][1])

        self._register_shared_widgets()
        self._register_search_area()
        self._register_eve_explorer()
        self._register_eve_aggregator()
        self._register_uniq()
        self._register_graph()
        self._register_tabs()

    def _register_shared_widgets(self) -> None:
        self._select_agg_col = widgets.Dropdown(description="Group by")

    def _register_search_area(self) -> None:
        self._picker_date_from = widgets.DatetimePicker(description="From", value=self._connector.from_date)
        self._picker_date_to = widgets.DatetimePicker(description="To", value=self._connector.to_date)

        self._interactive_time_pick = widgets.interactive(self._connector.set_query_timeframe,
                                                          from_date=self._picker_date_from,
                                                          to_date=self._picker_date_to)

        # self._slider_time_minutes = widgets.IntSlider(description="Minutes", min=0, max=60, value=15, continuous_update=False)
        # self._slider_time_hours = widgets.IntSlider(description="Hours", min=0, max=24, value=0, continuous_update=False)

        # self._interactive_time_range = widgets.interactive(self._connector.set_query_delta,
        #                                                    minutes=self._slider_time_minutes,
        #                                                    hours=self._slider_time_hours)

        self._slider_page_size = widgets.IntSlider(description="Document count",
                                                   min=1000,
                                                   max=10000)

        self._text_query = widgets.Text(description="Query")
        self._box_query_params = widgets.VBox([self._interactive_time_pick,
                                               # self._interactive_time_range,
                                               self._text_query,
                                               self._slider_page_size])

        self._button_download_eve = widgets.Button(description="Download EVE")
        self._button_download_eve.on_click(self._download_eve)

        self._box_search_area = widgets.VBox([self._box_query_params,
                                              self._button_download_eve])

        self._selection_eve_explore_columns = widgets.SelectMultiple(description="Columns", rows=20)
        self._selection_eve_explore_sort = widgets.SelectMultiple(description="Sort", rows=20)

    def _register_eve_explorer(self) -> None:
        self._slider_show_eve = widgets.IntSlider(min=10, max=1000, continuous_update=False)

        self._find_filtered_columns = widgets.Dropdown(description="Field")
        self._find_filtered_value = widgets.Text(description="Value")

        self._find_filtered_event_type = widgets.Dropdown(description="Event Type")
        self._find_filtered_flow_id = widgets.Combobox(description="Flow ID")

        self._interactive_explore_eve = widgets.interactive(
            self._display_eve_show,
            limit=self._slider_show_eve,
            columns=self._selection_eve_explore_columns,
            sort=self._selection_eve_explore_sort,
            filter_field=self._find_filtered_columns,
            filter_value=self._find_filtered_value,
            filter_event_type=self._find_filtered_event_type,
            filter_flow_id=self._find_filtered_flow_id,
        )

        self._box_filter_fields = widgets.VBox([self._find_filtered_columns,
                                                self._find_filtered_value])
        self._box_filter_fields = widgets.HBox([self._box_filter_fields,
                                                widgets.VBox([self._find_filtered_event_type,
                                                              self._find_filtered_flow_id])])

        self._box_eve_explorer = widgets.VBox([self._box_filter_fields,
                                               self._slider_show_eve,
                                               widgets.HBox([self._selection_eve_explore_columns,
                                                             self._selection_eve_explore_sort])])

        self._box_eve_explorer = widgets.HBox([self._box_search_area,
                                               self._box_eve_explorer,
                                               self._output_query_feedback])

        self._box_eve_explorer = widgets.VBox([self._box_eve_explorer,
                                               self._output_eve_explorer,
                                               self._output_debug])

    def _register_eve_aggregator(self) -> None:
        self._button_eve_agg = widgets.Button(description="Aggregate EVE")
        self._interactive_aggregate_eve = widgets.interactive(self._display_eve_agg,
                                                              limit=widgets.IntSlider(min=10, max=1000, continuous_update=False),
                                                              groupby=self._select_agg_col)

        self._box_eve_agg = widgets.HBox([self._box_search_area,
                                          self._interactive_aggregate_eve])

        self._box_eve_agg = widgets.VBox([self._box_eve_agg,
                                          self._output_eve_agg,
                                          self._output_debug])

    def _register_uniq(self) -> None:
        options = self._connector.get_unique_fields()
        self._dropdown_select_field = widgets.Combobox(description="Select field",
                                                       options=options)

        self._tickbox_sort_counts = widgets.Checkbox(description="Sort by count", value=False)
        self._tickbox_show_simple = widgets.Checkbox(description="Show only simple values", value=False)

        self._slider_show_uniq = widgets.IntSlider(min=10, max=1000, continuous_update=False, description="Limit display")

        self._interactive_display_uniq = widgets.interactive(self._display_uniq,
                                                             limit=self._slider_show_uniq,
                                                             show_simple=self._tickbox_show_simple,
                                                             sort_by_count=self._tickbox_sort_counts)

        self._button_download_uniq = widgets.Button(description="Pull uniq")
        self._button_download_uniq.on_click(self._download_uniq)

        self._box_uniq = widgets.VBox([self._dropdown_select_field,
                                       self._interactive_display_uniq,
                                       self._button_download_uniq])

        self._box_uniq = widgets.HBox([self._box_search_area,
                                       self._box_uniq])

        self._box_uniq = widgets.VBox([self._box_uniq,
                                       self._output_uniq])

    def _register_graph(self) -> None:
        options = self._connector.get_unique_fields()
        options = col_cleanup(options)

        self._dropdown_graph_node_src = widgets.Dropdown(description="Node src", options=options, value="event_type")
        self._dropdown_graph_node_dst = widgets.Dropdown(description="Node dst", options=options, value="flow_id")

        self._slider_graph_node_agg_src = widgets.IntSlider(min=10, max=200, value=100, continuous_update=False)
        self._slider_graph_node_agg_dst = widgets.IntSlider(min=10, max=200, value=100, continuous_update=False)

        self._button_graph_download = widgets.Button(description="Pull graph data")
        self._button_graph_download.on_click(self._download_graph)

        self._dropdown_graph_rez = widgets.Dropdown(
            description="Graph size",
            options=GRAPH_RESOLUTIONS,
            value=GRAPH_RESOLUTIONS[1]
        )

        self._button_graph_draw = widgets.Button(description="Draw graph")
        self._button_graph_draw.on_click(self._display_graph)

        self._box_graph = widgets.VBox([widgets.HBox([self._dropdown_graph_node_src,
                                                      self._slider_graph_node_agg_src]),
                                        widgets.HBox([self._dropdown_graph_node_dst,
                                                      self._slider_graph_node_agg_dst]),
                                        self._button_graph_download,
                                        self._dropdown_graph_rez])

        self._box_graph = widgets.HBox([self._box_search_area,
                                        self._box_graph])

        self._box_graph = widgets.VBox([self._box_graph,
                                        self._output_graph,
                                        self._output_graph_feedback])

    def _register_tabs(self) -> None:
        boxes = [
            (self._box_eve_explorer, "Expore"),
            (self._box_eve_agg, "Aggregate"),
            (self._box_uniq, "Uniq"),
            (self._box_graph, "Graph")
        ]

        self._tabs = widgets.Tab(children=[b[0] for b in boxes])

        for i, item in enumerate(boxes):
            self._tabs.set_title(i, item[1])

    def _download_eve(self, args: None) -> None:
        self._connector.set_page_size(self._slider_page_size.value)
        self._output_query_feedback.clear_output()
        with self._output_query_feedback:
            try:
                self.data = self._connector.get_events_df(qfilter=self._text_query.value)
            except ConnectionError:
                print("unable to connect to %s" % self._connector.endpoint)

            self.data = reorder_columns(self.data)
            self.data = df_parse_time_colums(self.data)

            self._set_selection_options(self._selection_eve_explore_columns)
            self._set_selection_options(self._selection_eve_explore_sort)

            dump = {
                "time": (self._connector.from_date, self._connector.to_date)
            }
            pickle.dump(dump, open(self._pickle_q_time, "wb"))

        self._display_aggregate_event_types()
        display_df(self.data, self._output_eve_explorer)

        # initial display update and widget population when user has not interacted yet
        # empty dropdowns / boxes and noisy display otherwise

        # FIXME - linting errors from type definitions
        self._display_eve_show(
            limit=self._slider_show_eve.value,
            columns=self._selection_eve_explore_columns.value,
            sort=self._selection_eve_explore_sort.value,
            filter_field=self._find_filtered_columns.value,
            filter_value=self._find_filtered_value.value,
            filter_event_type=self._find_filtered_event_type.value,
            filter_flow_id=self._find_filtered_flow_id.value,
        )

    def _download_uniq(self, args: None) -> None:
        self._output_query_feedback.clear_output()
        with self._output_query_feedback:
            try:
                values = self._connector.get_eve_unique_values(counts="yes",
                                                               field=self._dropdown_select_field.value,
                                                               qfilter=self._text_query.value)
                self.data_uniq = pd.DataFrame(values)

            except ConnectionError:
                print("unable to connect to %s" % self._connector.endpoint)

        self.data_uniq = pd.DataFrame(self.data_uniq)
        self._display_uniq(self._slider_show_uniq.value,
                           checkbox_verify(self._tickbox_show_simple),
                           checkbox_verify(self._tickbox_sort_counts))

    def _download_graph(self, args: None) -> None:
        self._output_graph_feedback.clear_output()
        with self._output_graph_feedback:
            print("calling scirius at %s" % self._connector.endpoint)
            kwargs = {
                "col_src": self._dropdown_graph_node_src.value,
                "col_dest": self._dropdown_graph_node_dst.value,
                "size_src": self._slider_graph_node_agg_src.value,
                "size_dest": self._slider_graph_node_agg_dst.value,
                "qfilter": self._text_query.value
            }
            self.data_graph = self._connector.get_eve_fields_graph_nx(**kwargs)

            # drop empty nodes (and connected edges)
            # means missing eve field, no connection can be made
            if "" in list(self.data_graph.nodes()):
                self.data_graph.remove_node("")

            nx_add_scaled_doc_count(self.data_graph)
            nx_degree_scale(self.data_graph)

            display("call done, got %d nodes and %d edges" %
                    (len(self.data_graph.nodes()), len(self.data_graph.edges())))

    def _display_uniq(self, limit: int, show_simple: bool, sort_by_count: bool) -> None:
        if self.data_uniq.empty:
            return

        pd.set_option('display.max_rows', limit)
        pd.set_option('display.min_rows', limit)

        if sort_by_count:
            self.data_uniq = self.data_uniq.sort_values(by="doc_count", ascending=False)
        else:
            self.data_uniq = self.data_uniq.sort_values(by="key")

        if show_simple is True:
            display_list_simple(list(self.data_uniq.key), self._output_uniq)
        else:
            display_df(self.data_uniq, self._output_uniq)

    def _display_graph(self, args) -> None:
        self._output_graph.clear_output()
        with self._output_graph:
            pass

    def _display_aggregate_event_types(self):
        self._output_query_feedback.clear_output()
        with self._output_query_feedback:
            if "event_type" not in self._data_column_values():
                print("no event_type column to aggregate")
            else:
                df_agg = (
                    self
                    .data
                    .groupby("event_type")
                    .agg({"event_type": ["count"]})
                )
                if df_agg is not None:
                    df_agg = df_agg.reset_index()
                    df_agg.columns = ["event_type", "event_count"]
                display(df_agg)

    def _display_eve_show(self,
                          limit: int,
                          columns: tuple,
                          sort: tuple,
                          filter_field: str,
                          filter_value: str,
                          filter_event_type: str,
                          filter_flow_id: str):
        pd.set_option('display.max_rows', limit)
        pd.set_option('display.min_rows', limit)

        cols = list(columns)
        if len(cols) == 0:
            cols = [c for c in DEFAULT_COLUMNS if c in self._data_column_values()]
            self._selection_eve_explore_columns.value = cols

        self._output_eve_explorer.clear_output()
        with self._output_eve_explorer:
            sort_cols = []
            if len(sort) > 0:
                sort_cols = list(sort)
            elif len(sort) == 0 and "timestamp" in self._data_column_values():
                sort_cols = ["timestamp"]
                self._selection_eve_explore_sort.value = sort_cols

        self.data_filtered = (
            self
            .data[cols]
            .sort_values(by=sort_cols)
        )
        self._output_eve_explorer.clear_output()
        with self._output_eve_explorer:
            self.data_filtered = df_filter_value(self.data_filtered, filter_field, filter_value)
            self.data_filtered = df_filter_value(self.data_filtered, "event_type", filter_event_type)
            self.data_filtered = df_filter_value(self.data_filtered, "flow_id", filter_flow_id)

        self._find_filtered_columns.options = self._filtered_column_values()

        update_values(self._find_filtered_event_type, self.data, "event_type")
        update_values(self._find_filtered_flow_id, self.data_filtered, "flow_id")

        self._select_agg_col.options = self._filtered_column_values()

        display_df(self.data_filtered, self._output_eve_explorer)

    def _display_eve_agg(self, limit: int, groupby: str) -> None:
        pd.set_option('display.max_rows', limit)
        pd.set_option('display.min_rows', limit)

        df = self.data if self.data_filtered.empty else self.data_filtered

        if groupby in ("", None):
            return

        self.data_aggregate = (
            df
            .fillna("")
            .dropna(axis=1, how="all")
            .groupby(by=groupby)
            .agg({
                item: ["min", "max"] if item in TIME_COLS
                else ["unique", "nunique"]
                for item in list(df.columns.values) if item != groupby
            })
        )
        if self.data_aggregate is not None and not self.data_aggregate.empty:
            self.data_aggregate = self.data_aggregate.reset_index()

        if isinstance(self.data_aggregate, pd.DataFrame):
            display_df(self.data_aggregate, self._output_eve_agg)

    def _set_selection_options(self, selection: widgets.SelectMultiple) -> None:
        if isinstance(selection.options, tuple) and len(selection.options) == 0:
            selection.options = self._data_column_values()

    def _data_column_values(self) -> list:
        return [] if self.data is None else list(self.data.columns.values)

    def _filtered_column_values(self) -> list:
        return list(self.data_filtered.columns.values)

    def display(self) -> None:
        display(self._tabs)


def display_df(data: pd.DataFrame | pd.Series, output: widgets.Output):
    output.clear_output()
    with output:
        display(data.dropna(how="all", axis=1))


def display_list_simple(data: list | pd.Series, output: widgets.Output) -> None:
    output.clear_output()
    with output:
        print("\n".join(data))


def reorder_columns(df: pd.DataFrame, core_columns=CORE_COLUMNS) -> pd.DataFrame:
    cols = list(df.columns.values)
    core_cols = [c for c in core_columns if c in cols]

    cols = sorted([c for c in cols if c not in core_cols])
    cols = core_cols + cols
    return df[cols]


def df_existing_columns(df: pd.DataFrame, columns=CORE_COLUMNS) -> list:
    return [c for c in columns if c in list(df.columns.values)]


def df_parse_time_colums(df: pd.DataFrame, columns=TIME_COLS) -> pd.DataFrame:
    for ts in columns:
        if ts in list(df.columns.values):
            df[ts] = pd.to_datetime(df[ts], errors="coerce")
    return df


def df_filter_value(df: pd.DataFrame, col: str, value: str) -> pd.DataFrame:
    if col in ("", None) or value == ("", None):
        return df
    if col not in list(df.columns.values):
        return df

    df = df.loc[pd.notna(df[col])]

    series = df[col]
    if series.dtype == "object":
        df = df.loc[series.str.contains(value, flags=re.IGNORECASE)]
    elif series.dtype == "int64":
        df = df.loc[series == int(value)]
    elif series.dtype == "float64":
        df = df.loc[series.astype(int) == int(value)]

    return df


def update_values(w: widgets.Dropdown | widgets.Combobox,
                  df: pd.DataFrame,
                  field: str) -> None:
    if field not in list(df.columns.values):
        return
    w.options = [""] + [str(v) for v in list(df[field].dropna().unique())]


def checkbox_verify(c: widgets.Checkbox, default=False) -> bool:
    return c.value if isinstance(c.value, bool) else default


def col_cleanup(c: list[str]) -> list:
    return [i for i in c if i not in TIME_COLS and not i.startswith("@")]


def nx_add_scaled_doc_count(g: nx.Graph):
    doc_counts = [attr["doc_count"] for (_, _, attr) in g.edges(data=True)]

    doc_counts = np.log2(doc_counts)
    doc_counts = min_max_scaling(pd.Series(doc_counts))

    # add scaled doc counts to edges to serve as weights
    for i, (_, _, attr) in enumerate(g.edges(data=True)):
        if attr is not None:
            attr["scaled_doc_count"] = doc_counts[i]


def nx_degree_scale(g: nx.Graph) -> pd.Series | pd.DataFrame:
    degree = [g.degree(n) for n in g.nodes()]
    return min_max_scaling(pd.Series(degree))
