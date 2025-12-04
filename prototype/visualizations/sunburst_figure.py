"""Functions for creating sunburst figures."""

from typing import List

import plotly.graph_objects as go
from utils.constants import (
    CENTER_FONT_SIZE,
    COMPONENT_FONT_SIZE,
    DATASET_FONT_SIZE,
    REQUIREMENT_FONT_SIZE,
    ROOT_FONT_SIZE,
    SUNBURST_HEIGHT,
    USE_CASE_FONT_SIZE,
)


def create_sunburst_figure(
    labels: List,
    parents: List,
    values: List,
    colors: List,
    ids: List,
    hover_text: List,
    title: str = "",
    hierarchy_levels: int = 2,
) -> go.Figure:
    """Create a single sunburst plotly figure."""
    font_sizes = []

    if hierarchy_levels == 3:
        # Three-level hierarchy: root → use cases → requirements → datasets
        for label, parent, node_id in zip(labels, parents, ids):
            if parent == " ":  # Root node
                font_sizes.append(ROOT_FONT_SIZE)
            elif parent.startswith("root"):  # Use cases (children of root)
                font_sizes.append(USE_CASE_FONT_SIZE)
            elif parent.startswith("uc_"):  # Requirements (children of use cases)
                font_sizes.append(REQUIREMENT_FONT_SIZE)
            else:  # Datasets (children of requirements)
                font_sizes.append(DATASET_FONT_SIZE)
    else:
        # Two-level hierarchy
        for label, parent in zip(labels, parents):
            if parent == " ":  # Root node
                font_sizes.append(ROOT_FONT_SIZE)
            elif parent.startswith("root"):
                font_sizes.append(COMPONENT_FONT_SIZE)
            else:  # Second level
                font_sizes.append(DATASET_FONT_SIZE)

    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(colors=colors, line=dict(color="white", width=3)),
            hovertext=hover_text,
            hoverinfo="text",
            textfont=dict(size=font_sizes, color="#3F3E3E", family="Arial"),
            insidetextorientation="radial",
            branchvalues="total",
        )
    )

    fig.update_layout(
        height=SUNBURST_HEIGHT,
        margin=dict(t=40, b=10, l=10, r=10),
        showlegend=False,
        title=dict(
            text=title, x=0.5, xanchor="center", font=dict(size=18, color="#333333")
        ),
    )

    return fig


def add_iqb_score_annotation(fig: go.Figure, iqb_score: float) -> None:
    """Add IQB score annotation to the center of the sunburst."""
    fig.add_annotation(
        text=f"<b>IQB Score</b><br>{iqb_score:.3f}",
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        font=dict(size=CENTER_FONT_SIZE, color="#333333", family="Arial"),
        showarrow=False,
        align="center",
    )
