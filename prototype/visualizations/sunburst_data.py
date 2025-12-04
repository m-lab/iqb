"""Data preparation functions for sunburst visualizations."""

from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Set, Tuple

from app_state import IQBAppState
from iqb import IQB_CONFIG
from utils.calculation_utils import (
    calculate_component_importance,
    calculate_dataset_importance_per_requirement,
)
from utils.constants import (
    COMPONENT_COLORS,
    DATASET_COLORS,
    USE_CASE_COLORS,
    ZERO_WEIGHT_DISPLAY_RATIO,
)
from utils.data_utils import get_available_datasets, get_requirement_display_name


@dataclass
class SunburstData:
    """Container for sunburst chart data."""

    root_id: str = "root"
    labels: List[str] = field(default_factory=list)
    parents: List[str] = field(default_factory=list)
    values: List[float] = field(default_factory=list)
    colors: List[str] = field(default_factory=list)
    ids: List[str] = field(default_factory=list)
    hover_text: List[str] = field(default_factory=list)
    zero_weight_nodes: Set[str] = field(default_factory=set)

    def __post_init__(self):
        # Initialize root node - empty parent "" is Plotly's convention for root
        self.labels.append(" ")
        self.parents.append("")
        self.values.append(1.0)
        self.colors.append("white")
        self.ids.append(self.root_id)
        self.hover_text.append("")

    def add_node(
        self,
        label: str,
        parent: str,
        value: float,
        color: str,
        node_id: str,
        hover: str = "",
    ):
        self.labels.append(label)
        self.parents.append(parent)
        self.values.append(value)
        self.colors.append(color)
        self.ids.append(node_id)
        self.hover_text.append(hover)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _normalize(value: float, total: float) -> float:
    return value / total if total > 0 else 0


def _format_dataset_hover(dataset: str, weight: Optional[float], exists: bool) -> str:
    name = f"<b>{dataset.upper()}</b>"
    if not exists:
        return f"{name}<br>No Data Available"
    return f"{name}<br>Weight: {weight:.2f}"


def _get_use_case_color_map() -> Dict[str, str]:
    color_list = list(USE_CASE_COLORS.values())
    return {
        uc: color_list[idx % len(color_list)]
        for idx, uc in enumerate(sorted(IQB_CONFIG["use cases"].keys()))
    }


# ---------------------------------------------------------------------------
# Shared iteration logic
# ---------------------------------------------------------------------------


@dataclass
class UseCaseNode:
    name: str
    display_name: str
    weight: float
    normalized_weight: float
    color: str
    config: dict


@dataclass
class RequirementNode:
    name: str
    display_name: str
    weight: float
    normalized_weight: float
    color: str
    config: dict
    use_case: UseCaseNode


def iter_use_cases(state: IQBAppState) -> Iterator[UseCaseNode]:
    """Iterate over use cases with computed weights."""
    use_cases = IQB_CONFIG["use cases"]
    total_weight = sum(state.use_case_weights.get(uc, 1.0) for uc in use_cases)
    colors = _get_use_case_color_map()

    for uc_name, uc_config in use_cases.items():
        weight = state.use_case_weights.get(uc_name, 1.0)
        yield UseCaseNode(
            name=uc_name,
            display_name=uc_name.replace("_", " ").title(),
            weight=weight,
            normalized_weight=_normalize(weight, total_weight),
            color=colors.get(uc_name, "#CCCCCC"),
            config=uc_config,
        )


def iter_requirements(state: IQBAppState, uc: UseCaseNode) -> Iterator[RequirementNode]:
    """Iterate over requirements within a use case with computed weights."""
    req_configs = uc.config["network requirements"]
    total_weight = sum(
        state.requirement_weights.get(uc.name, {}).get(req, rc.get("w", 1.0))
        for req, rc in req_configs.items()
    )

    for req_name, req_config in req_configs.items():
        display = get_requirement_display_name(req_name)
        weight = state.requirement_weights.get(uc.name, {}).get(
            req_name, req_config.get("w", 1.0)
        )
        yield RequirementNode(
            name=req_name,
            display_name=display,
            weight=weight,
            normalized_weight=_normalize(weight, total_weight) * uc.normalized_weight,
            color=COMPONENT_COLORS.get(display, "#CCCCCC"),
            config=req_config,
            use_case=uc,
        )


def get_dataset_info(
    state: IQBAppState, req: RequirementNode
) -> Tuple[Dict[str, float], Dict[str, bool]]:
    """Get dataset weights and existence flags for a requirement."""
    if req.name in state.dataset_weights:
        weights = state.dataset_weights[req.name]
    elif "datasets" in req.config:
        weights = {ds: dc.get("w", 1.0) for ds, dc in req.config["datasets"].items()}
    else:
        weights = {}

    exists = state.dataset_exists_in_config.get(req.name, {})
    if not exists and "datasets" in req.config:
        exists = {ds: True for ds in req.config["datasets"]}

    return weights, exists


# ---------------------------------------------------------------------------
# Sunburst preparation functions
# ---------------------------------------------------------------------------


def prepare_requirements_sunburst_data(state: IQBAppState) -> SunburstData:
    """Prepare data for requirements → datasets sunburst visualization."""
    data = SunburstData(root_id="root_req")

    component_importance = calculate_component_importance()
    dataset_importance, dataset_exists = calculate_dataset_importance_per_requirement(
        state
    )
    datasets = get_available_datasets()

    for req, importance in component_importance.items():
        display_name = get_requirement_display_name(req)
        data.add_node(
            label=display_name,
            parent=data.root_id,
            value=importance,
            color=COMPONENT_COLORS.get(display_name, "#CCCCCC"),
            node_id=display_name,
        )

        # First pass: collect raw values and track zero-weight nodes
        ds_values = {}
        for dataset in datasets:
            ds_imp = dataset_importance[req].get(dataset, 0.0)
            if ds_imp == 0.0:
                ds_values[dataset] = importance * ZERO_WEIGHT_DISPLAY_RATIO
                data.zero_weight_nodes.add(f"{dataset.upper()}-{display_name}")
            else:
                ds_values[dataset] = ds_imp

        # Normalize so children sum to parent's importance
        total_ds = sum(ds_values.values())
        if total_ds > 0:
            scale = importance / total_ds
            for dataset in ds_values:
                ds_values[dataset] *= scale

        # Second pass: add nodes with normalized values
        for dataset in datasets:
            unique_id = f"{dataset}-{display_name}"
            exists = dataset_exists.get(req, {}).get(dataset, False)
            weight = state.dataset_weights.get(req, {}).get(dataset, 0.0)

            data.add_node(
                label=dataset.upper(),
                parent=display_name,
                value=ds_values[dataset],
                color=DATASET_COLORS.get(dataset, "#CCCCCC"),
                node_id=unique_id,
                hover=_format_dataset_hover(dataset, weight, exists),
            )

    return data


def prepare_use_cases_sunburst_data(state: IQBAppState) -> SunburstData:
    """Prepare data for use cases → requirements sunburst visualization."""
    data = SunburstData(root_id="root_uc")

    for uc in iter_use_cases(state):
        uc_id = f"uc_{uc.name}"
        data.add_node(
            label=uc.display_name,
            parent=data.root_id,
            value=uc.normalized_weight,
            color=uc.color,
            node_id=uc_id,
            hover=f"<b>{uc.display_name}</b><br>Weight: {uc.weight:.2f}",
        )

        # First pass: collect requirement values
        req_values = {}
        req_data = {}
        for req in iter_requirements(state, uc):
            req_values[req.name] = req.normalized_weight
            req_data[req.name] = req

        # Normalize so children sum to parent's normalized_weight
        total_req = sum(req_values.values())
        if total_req > 0:
            scale = uc.normalized_weight / total_req
            for name in req_values:
                req_values[name] *= scale

        # Second pass: add nodes with normalized values
        for req_name, req in req_data.items():
            data.add_node(
                label=req.display_name,
                parent=uc_id,
                value=req_values[req_name],
                color=req.color,
                node_id=f"req_{uc.name}_{req.name}",
                hover=f"<b>{req.display_name}</b><br>Weight: {req.weight:.0f}",
            )

    return data


def prepare_complete_hierarchy_sunburst_data(state: IQBAppState) -> SunburstData:
    """Prepare data for use cases → requirements → datasets sunburst visualization."""
    data = SunburstData(root_id="root_complete")
    datasets = get_available_datasets()

    for uc in iter_use_cases(state):
        uc_id = f"uc_complete_{uc.name}"
        data.add_node(
            label=uc.display_name,
            parent=data.root_id,
            value=uc.normalized_weight,
            color=uc.color,
            node_id=uc_id,
            hover=f"<b>{uc.display_name}</b><br>Weight: {uc.weight:.2f}",
        )

        # First pass: collect requirement values
        req_values = {}
        req_data = {}
        for req in iter_requirements(state, uc):
            req_values[req.name] = req.normalized_weight
            req_data[req.name] = req

        # Normalize requirements to sum to use case's weight
        total_req = sum(req_values.values())
        if total_req > 0:
            scale = uc.normalized_weight / total_req
            for name in req_values:
                req_values[name] *= scale

        # Second pass: add requirement nodes and their datasets
        for req_name, req in req_data.items():
            req_id = f"req_complete_{uc.name}_{req.name}"
            req_value = req_values[req_name]

            data.add_node(
                label=req.display_name,
                parent=uc_id,
                value=req_value,
                color=req.color,
                node_id=req_id,
                hover=f"<b>{req.display_name}</b><br>Weight: {req.weight:.0f}",
            )

            # Collect dataset values
            ds_weights, ds_exists = get_dataset_info(state, req)
            ds_values = {}
            for dataset in datasets:
                ds_weight = ds_weights.get(dataset, 0.0)
                if ds_weight > 0:
                    ds_values[dataset] = ds_weight
                else:
                    ds_values[dataset] = ZERO_WEIGHT_DISPLAY_RATIO

            # Normalize datasets to sum to requirement's value
            total_ds = sum(ds_values.values())
            if total_ds > 0:
                scale = req_value / total_ds
                for dataset in ds_values:
                    ds_values[dataset] *= scale

            # Add dataset nodes
            for dataset in datasets:
                data.add_node(
                    label=dataset.upper(),
                    parent=req_id,
                    value=ds_values[dataset],
                    color=DATASET_COLORS.get(dataset, "#CCCCCC"),
                    node_id=f"ds_complete_{uc.name}_{req.name}_{dataset}",
                    hover=_format_dataset_hover(
                        dataset,
                        ds_weights.get(dataset, 0.0),
                        ds_exists.get(dataset, False),
                    ),
                )

    return data
