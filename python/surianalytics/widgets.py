"""
Reusable widgets
"""

from ipywidgets.widgets.interaction import display
from .connectors import RESTSciriusConnector

import ipywidgets as widgets
import pandas as pd

import pickle
import os


class EveHunter(object):
    data: pd.DataFrame

    def __init__(self, c=RESTSciriusConnector()) -> None:
        # Data connector to backend
        self._connector = c

        # Outputs
        self._output_debug = widgets.Output()
        self._output_data = widgets.Output()

        self._pickle_q_time = "./params.pkl"
        if os.path.exists(self._pickle_q_time):
            params = pickle.load(open(self._pickle_q_time, "rb"), encoding="bytes")
            self._connector.set_query_timeframe(params["time"][0], params["time"][1])

        self._register_search_area()
        self._register_eve_explorer()
        self._register_tabs()

    def display(self) -> None:
        display(self._tabs)

    def _download_eve(self, args: None) -> None:
        self._output_debug.clear_output()
        with self._output_debug:
            try:
                self.data = self._connector.get_events_df(qfilter=self._text_query.value)
            except ConnectionError:
                print("unable to connect to %s" % self._connector.endpoint)
            dump = {
                "time": (self._connector.from_date, self._connector.to_date)
            }
            pickle.dump(dump, open(self._pickle_q_time, "wb"))

        self._display_aggregate_event_types()
        self._display_raw_data()

    def _register_search_area(self) -> None:
        self._interactive_time_pick = widgets.interactive(self._connector.set_query_timeframe,
                                                          from_date=widgets.DatetimePicker(description="From",
                                                                                           value=self._connector.from_date),
                                                          to_date=widgets.DatetimePicker(description="To",
                                                                                         value=self._connector.to_date))

        self._slider_page_size = widgets.IntSlider(description="Document count",
                                                   min=1000,
                                                   max=10000)

        self._text_query = widgets.Text(description="Query")

        self._button_download_eve = widgets.Button(description="Download EVE")
        self._button_download_eve.on_click(self._download_eve)

        self._box_search_area = widgets.VBox([self._interactive_time_pick,
                                              self._text_query,
                                              self._slider_page_size,
                                              self._button_download_eve])

    def _register_eve_explorer(self) -> None:
        self._slider_show_eve = widgets.IntSlider(min=10, max=1000)

        self._interactive_explore_eve = widgets.interactive(
            self._display_show_eve,
            limit=self._slider_show_eve
        )

        self._box_eve_explorer = widgets.VBox([self._slider_show_eve,
                                               self._output_data])

    def _register_tabs(self) -> None:
        boxes = [
            (widgets.HBox([self._box_search_area, self._output_debug]), "Query Events"),
            (self._box_eve_explorer, "Expore EVE"),
        ]

        self._tabs = widgets.Tab(children=[b[0] for b in boxes])

        for i, item in enumerate(boxes):
            self._tabs.set_title(i, item[1])

    def _display_aggregate_event_types(self):
        self._output_debug.clear_output()
        with self._output_debug:
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

    def _display_show_eve(self, limit: int):
        pd.set_option('display.max_rows', limit)
        pd.set_option('display.min_rows', limit)
        self._display_raw_data()

    def _display_raw_data(self) -> None:
        self._output_data.clear_output()
        with self._output_data:
            display(self.data)

# def handler_show_eve(limit: int, event_type: str, columns: list, sort: list, find_col: str, find_val: str):
#     pd.set_option('display.max_rows', limit)
#     pd.set_option('display.min_rows', limit)
#
#     df_filter = DF_EVENTS
#     df_filter = df_filter.dropna(how="all", axis=1)
#     if "event_type" in list(df_filter.columns.values):
#         df_filter = df_filter.loc[df_filter.event_type.str.contains(event_type)]
#
#     if columns is not None and len(columns) > 0:
#         columns = [c for c in columns if c in list(df_filter.columns.values)]
#         df_filter = df_filter[list(columns)]
#
#     if find_col != "" and find_val != "":
#         if find_col in list(df_filter.columns.values):
#             df_filter = df_filter.loc[pd.notna(df_filter[find_col])]
#             col = df_filter[find_col]
#             if col.dtype == "object":
#                 df_filter = df_filter.loc[col.str.contains(find_val, flags=re.IGNORECASE)]
#             elif col.dtype == "int64":
#                 df_filter = df_filter.loc[col == int(find_val)]
#             elif col.dtype == "float64":
#                 df_filter = df_filter.loc[col.astype(int) == int(find_val)]
#             else:
#                 OUTPUT_DEBUG.clear_output()
#                 with OUTPUT_DEBUG:
#                     print("col {} is {}, supported are string an int".format(find_col, col.dtype))
#         else:
#             OUTPUT_DEBUG.clear_output()
#             with OUTPUT_DEBUG:
#                 print("col {} not in dataframe".format(find_col))
#
#     SELECT_AGG_COLUMN.options = col_names_subset()
#     COLUMN_SELECTION.options = col_names_subset()
#
#     global DF_FILTER
#     DF_FILTER = df_filter
#     OUTPUT_EVE_DF.clear_output()
#     with OUTPUT_EVE_DF:
#         display(df_filter)
