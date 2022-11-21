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
        self._pickle_q_time = "./params.pkl"
        if os.path.exists(self._pickle_q_time):
            params = pickle.load(open(self._pickle_q_time, "rb"), encoding="bytes")
            self._connector.set_query_timeframe(params["time"][0], params["time"][1])

        self._register_search_area()
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

        self._search_area = [self._interactive_time_pick,
                             self._text_query,
                             self._slider_page_size,
                             self._button_download_eve]

    def _register_tabs(self) -> None:
        boxes = [
            (widgets.VBox(self._search_area), "Query Events"),
        ]

        self._tabs = widgets.Tab(children=[b[0] for b in boxes])

        for i, item in enumerate(boxes):
            self._tabs.set_title(i, item[1])
