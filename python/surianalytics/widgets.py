"""
Reusable widgets
"""

from ipywidgets.widgets.interaction import display
from .connectors import RESTSciriusConnector

import ipywidgets as widgets
import pandas as pd

import pickle
import os


DEFAULT_COLUMNS = ["timestamp",
                   "host",
                   "community_id",
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
                   "direction"]

TIME_COLS = ["timestamp", "@timestamp", "http.date"]


class EveHunter(object):
    data: pd.DataFrame

    def __init__(self, c=RESTSciriusConnector()) -> None:
        # Data connector to backend
        self._connector = c

        # Outputs
        self._output_query_feedback = widgets.Output()
        self._output_eve_explorer = widgets.Output()

        self._pickle_q_time = "./params.pkl"
        if os.path.exists(self._pickle_q_time):
            params = pickle.load(open(self._pickle_q_time, "rb"), encoding="bytes")
            self._connector.set_query_timeframe(params["time"][0], params["time"][1])

        self._register_shared_query_params()
        self._register_eve_explorer()
        self._register_search_area()
        self._register_tabs()

    def _download_eve(self, args: None) -> None:
        self._output_query_feedback.clear_output()
        with self._output_query_feedback:
            try:
                self.data = self._connector.get_events_df(qfilter=self._text_query.value)
            except ConnectionError:
                print("unable to connect to %s" % self._connector.endpoint)

            self.data = reorder_columns(self.data)
            self.data = df_parse_time_colums(self.data)

            self._set_selection_values(self._selection_eve_explore_columns)
            self._set_selection_values(self._selection_eve_explore_sort)

            dump = {
                "time": (self._connector.from_date, self._connector.to_date)
            }
            pickle.dump(dump, open(self._pickle_q_time, "wb"))

        self._display_aggregate_event_types()
        display_df(self.data, self._output_eve_explorer)

    def _register_shared_query_params(self) -> None:
        self._interactive_time_pick = widgets.interactive(self._connector.set_query_timeframe,
                                                          from_date=widgets.DatetimePicker(description="From",
                                                                                           value=self._connector.from_date),
                                                          to_date=widgets.DatetimePicker(description="To",
                                                                                         value=self._connector.to_date))

        self._slider_page_size = widgets.IntSlider(description="Document count",
                                                   min=1000,
                                                   max=10000)

        self._text_query = widgets.Text(description="Query")
        self._box_query_params = widgets.VBox([self._interactive_time_pick,
                                               self._text_query,
                                               self._slider_page_size])

    def _register_search_area(self) -> None:
        self._button_download_eve = widgets.Button(description="Download EVE")
        self._button_download_eve.on_click(self._download_eve)

        self._box_search_area = widgets.VBox([self._box_query_params,
                                              self._button_download_eve])

        self._box_search_area = widgets.HBox([self._box_search_area,
                                              self._output_query_feedback])

    def _register_eve_explorer(self) -> None:
        self._slider_show_eve = widgets.IntSlider(min=10, max=1000)
        self._selection_eve_explore_columns = widgets.SelectMultiple(description="Columns", rows=20)
        self._selection_eve_explore_sort = widgets.SelectMultiple(description="Sort", rows=20)

        self._interactive_explore_eve = widgets.interactive(
            self._display_show_eve,
            limit=self._slider_show_eve,
            columns=self._selection_eve_explore_columns,
            sort=self._selection_eve_explore_sort,
        )

        self._box_eve_explorer = widgets.VBox([self._slider_show_eve,
                                               widgets.HBox([self._selection_eve_explore_columns,
                                                             self._selection_eve_explore_sort])])

        self._box_eve_explorer = widgets.HBox([self._box_query_params,
                                               self._box_eve_explorer,
                                               self._output_query_feedback])

        self._box_eve_explorer = widgets.VBox([self._box_eve_explorer,
                                               self._output_eve_explorer])

    def _register_tabs(self) -> None:
        boxes = [
            (self._box_search_area, "Query Events"),
            (self._box_eve_explorer, "Expore EVE"),
        ]

        self._tabs = widgets.Tab(children=[b[0] for b in boxes])

        for i, item in enumerate(boxes):
            self._tabs.set_title(i, item[1])

    def _display_aggregate_event_types(self):
        self._output_query_feedback.clear_output()
        with self._output_query_feedback:
            if "event_type" not in list(self.data.columns.values):
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

    def _display_show_eve(self, limit: int, columns: tuple, sort: tuple):
        pd.set_option('display.max_rows', limit)
        pd.set_option('display.min_rows', limit)

        cols = list(columns)
        if len(cols) == 0:
            cols = [c for c in DEFAULT_COLUMNS if c in list(self.data.columns.values)]
            self._selection_eve_explore_columns.value = cols

        sort_cols = ["timestamp"] if len(sort) == 0 else list(sort)

        display_df(self.data[cols].sort_values(by=sort_cols), self._output_eve_explorer)

    def _set_selection_values(self, selection: widgets.SelectMultiple) -> None:
        if isinstance(selection.value, tuple) and len(selection.value) == 0:
            selection.options = list(self.data.columns.values)

    def display(self) -> None:
        display(self._tabs)


def display_df(data: pd.DataFrame, output: widgets.Output):
    output.clear_output()
    with output:
        display(data.dropna(how="all", axis=1))


def reorder_columns(df: pd.DataFrame, core_columns=DEFAULT_COLUMNS) -> pd.DataFrame:
    cols = list(df.columns.values)
    core_cols = [c for c in core_columns if c in cols]

    cols = sorted([c for c in cols if c not in core_cols])
    cols = core_cols + cols
    return df[cols]


def df_existing_columns(df: pd.DataFrame, columns=DEFAULT_COLUMNS) -> list:
    return [c for c in columns if c in list(df.columns.values)]


def df_parse_time_colums(df: pd.DataFrame, columns=TIME_COLS) -> pd.DataFrame:
    for ts in columns:
        if ts in list(df.columns.values):
            df[ts] = pd.to_datetime(df[ts], errors="coerce")
    return df
