# Copyright © 2023 Stamus Networks oss@stamus-networks.com

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Wrapper functions for data visualizations.
"""

import networkx as nx
import hvplot.networkx as hvnx

def draw_nx_graph(G: nx.Graph, width: int, height: int):
    # we expect graph object as returned by scirius API
    n_src = [i for i, (_, a) in enumerate(G.nodes(data=True)) if a["kind"] == "source"]
    n_dst = [i for i, (_, a) in enumerate(G.nodes(data=True)) if a["kind"] == "destination"]

    pos = nx.layout.spring_layout(G)
    nodes_src = hvnx.draw_networkx_nodes(G, pos, nodelist=n_src, node_color='#A0CBE2').opts(width=width, height=height)
    nodes_dst = hvnx.draw_networkx_nodes(G, pos, nodelist=n_dst, node_color="Orange", legend="right").opts(width=width, height=height)
    edge_params = {
        "edge_color": 'doc_count',
        "edge_cmap": 'viridis',
    }
    edges = (
        hvnx
        .draw_networkx_edges(G, pos, **edge_params)
        .opts(width=width, height=height)
    )
    return edges * nodes_src * nodes_dst
