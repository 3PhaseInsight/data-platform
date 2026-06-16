"""
Helpers to pull the pieces we need out of the raw API payload returned by
get_latest_result(). The payload is a dict with a top-level "results" list,
where each item is one "label" produced by the DAG. Different label_types
carry different shapes in "details", so we pick out the ones the UI needs.
"""

from typing import Any, Optional


def _results(data: dict) -> list[dict]:
    return data.get("results", []) or []


def get_phase_recommendation(data: dict) -> Optional[dict]:
    """
    Returns the single result item where label_type == 'Phase Connector
    Recommendation'. This is the item that carries recommended_phase,
    appliance_type, scores per phase, and ranking.
    """
    for item in _results(data):
        if item.get("label_type") == "Phase Connector Recommendation":
            return item
    return None


def get_electric_heating_by_phase(data: dict) -> dict[str, dict]:
    """
    The API emits one 'Electric heating' result per phase (L1/L2/L3), each
    repeating the same MAE/MAEr/TDEL_info block. Returns a dict keyed by
    phase, e.g. {"L1": {...item...}, "L2": {...}, "L3": {...}}.
    """
    out = {}
    for item in _results(data):
        if item.get("label_type") == "Electric heating":
            phase = item.get("phase")
            if phase:
                out[phase] = item
    return out


def get_maer_by_phase(data: dict) -> dict[str, float]:
    """
    Parses the MAEr list (e.g. ["l1: 215.39%", "l2: 204.09%", "l3: 94.47%"])
    from any one of the Electric heating items into {"L1": 215.39, ...}.
    MAEr is a model-fit error metric, NOT an amperage reading. We use it
    here only as a stand-in bar value until/unless real per-phase current
    (Amps) is available from another source -- swap this out if you have
    that field elsewhere.
    """
    heating = get_electric_heating_by_phase(data)
    any_item = next(iter(heating.values()), None)
    if not any_item:
        return {}

    maer_list = (any_item.get("details") or {}).get("MAEr", []) or []
    out = {}
    for entry in maer_list:
        # entry looks like "l1: 215.3859%"
        try:
            label, value = entry.split(":")
            value = value.strip().rstrip("%")
            out[label.strip().upper()] = float(value)
        except (ValueError, AttributeError):
            continue
    return out


def get_scores(rec_item: dict) -> dict:
    """
    recommended_phase, scores, ranking, feeder_id, appliance_type all live
    inside details on the 'Phase Connector Recommendation' item.
    """
    if not rec_item:
        return {}
    return (rec_item.get("details") or {}).get("scores", {})


def get_imbalance_level(scores: dict) -> str:
    """
    Very rough heuristic to turn the recommendation's per-phase 'scores'
    totals into a Low/Moderate/High imbalance label for display. Adjust
    thresholds to match whatever your team considers meaningful.
    """
    totals = [v.get("total", 0) for v in (scores or {}).values()]
    if not totals:
        return "Unknown"

    spread = max(totals) - min(totals)
    if spread < 2:
        return "Low"
    elif spread < 6:
        return "Moderate"
    else:
        return "High"


def get_recommended_actions(rec_item: dict) -> list[str]:
    """
    Builds a short, human-readable action list from the recommendation
    item. Replace/extend this with real business logic as needed -- this
    is intentionally simple placeholder text generation.
    """
    if not rec_item:
        return []

    details = rec_item.get("details") or {}
    phase = details.get("recommended_phase", "the recommended phase")
    appliance = details.get("appliance_type", "the new appliance")
    feeder = details.get("feeder_id")

    actions = [
        f"Connect {appliance.upper()} to phase {phase}.",
        "Verify the recommendation on-site before final connection.",
    ]
    if feeder:
        actions.append(f"Reference feeder ID {feeder} when scheduling work.")
    return actions
