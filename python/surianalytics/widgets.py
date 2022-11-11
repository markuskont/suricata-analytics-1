"""
Reusable widgets
"""

from ipywidgets.widgets.interaction import display
from .connectors import RESTSciriusConnector

import ipywidgets as widgets
import pandas as pd

class EveHunter(object):
    _connector: RESTSciriusConnector 
    _output_debug: widgets.Output
    _slider_page_size: widgets.IntSlider

    data: pd.DataFrame

    def __init__(self, c=RESTSciriusConnector()) -> None:
        # Data connector to backend
        self._connector = c

        # Outputs
        self._output_debug = widgets.Output()

        # Widgets
        # Data fetching
        self._slider_page_size = widgets.IntSlider(description="Document count",
                                                   min=1000,
                                                   max=10000)

        self._text_query = widgets.Text(description="Query")

        self._button_download_eve = widgets.Button(description="Download EVE")
        self._button_download_eve.on_click(self._download_eve)

        # Final app
        self._box_app = widgets.VBox([self._text_query,
                                      self._slider_page_size,
                                      self._button_download_eve,
                                      self._output_debug])


    def display(self) -> None:
        display(self._box_app)

    def _download_eve(self, args: None) -> None:
        self._output_debug.clear_output()
        with self._output_debug:
            try:
                self._connector.get_events_df(qfilter=self._text_query.value)
            except ConnectionError:
                print("unable to connect to %s" % self._connector.endpoint)
