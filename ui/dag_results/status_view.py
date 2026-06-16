import streamlit as st

from ui.dag_results.parsing import (
    get_imbalance_level,
    get_maer_by_phase,
    get_phase_recommendation,
    get_scores,
)

IMBALANCE_COLORS = {
    "Low": "#2E7D32",
    "Moderate": "#E08A00",
    "High": "#C62828",
    "Unknown": "#555555",
}


def render_status_view(meter_id: str, data: dict):
    rec_item = get_phase_recommendation(data)
    scores = get_scores(rec_item)
    imbalance = get_imbalance_level(scores)
    imbalance_color = IMBALANCE_COLORS.get(imbalance, "#555555")

    bar_values = get_maer_by_phase(data)  # placeholder bar metric, see parsing.py
    max_val = max(bar_values.values()) if bar_values else 1

    st.markdown(
        f"""
        <style>
        .status-card {{
            background-color: #3a3a3a;
            border-radius: 18px;
            padding: 24px 22px;
            margin: 20px 0 28px 0;
        }}
        .status-row {{
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 18px;
        }}
        .status-row:last-child {{
            margin-bottom: 0;
        }}
        .status-dot {{
            width: 16px;
            height: 16px;
            border-radius: 50%;
            background-color: #2D4FE0;
            flex-shrink: 0;
        }}
        .status-label {{
            color: #f2f2f2;
            font-size: 20px;
            width: 36px;
            flex-shrink: 0;
        }}
        .status-bar-bg {{
            flex-grow: 1;
            height: 14px;
            background-color: #c7c7c7;
            border-radius: 7px;
            overflow: hidden;
        }}
        .status-bar-fill {{
            height: 100%;
            background-color: #2D4FE0;
            border-radius: 7px 0 0 7px;
        }}
        .status-value {{
            color: #f2f2f2;
            font-size: 18px;
            width: 64px;
            text-align: right;
            flex-shrink: 0;
        }}
        .meter-heading {{
            font-size: 32px;
            color: #f5f5f5;
            font-weight: 500;
            margin-bottom: 4px;
        }}
        .meter-sub {{
            color: #e6e6e6;
            font-size: 15px;
            margin: 2px 0;
        }}
        .imbalance-row {{
            display: flex;
            align-items: center;
            gap: 16px;
            margin: 28px 0;
        }}
        .imbalance-label {{
            font-size: 24px;
            color: #2a2a2a;
        }}
        .imbalance-pill {{
            background-color: #fafafa;
            color: {imbalance_color};
            font-weight: 600;
            font-size: 16px;
            padding: 8px 20px;
            border-radius: 10px;
        }}
        div[data-testid="stButton"] > button {{
            background-color: #2D4FE0;
            color: white;
            border: none;
            border-radius: 12px;
            height: 56px;
            font-size: 17px;
            width: 100%;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f'<div class="meter-heading">Smart meter: {meter_id}</div>', unsafe_allow_html=True)
    st.markdown('<div class="meter-sub">3-phase connection</div>', unsafe_allow_html=True)
    st.markdown('<div class="meter-sub">Address: If available*</div>', unsafe_allow_html=True)

    bars_html = '<div class="status-card">'
    for phase in ["L1", "L2", "L3"]:
        val = bar_values.get(phase, 0)
        pct = (val / max_val * 100) if max_val else 0
        row_html = f"""<div class="status-row">
<div class="status-dot"></div>
<div class="status-label">{phase}</div>
<div class="status-bar-bg">
<div class="status-bar-fill" style="width:{pct:.1f}%;"></div>
</div>
<div class="status-value">{val:.1f}</div>
</div>"""
        bars_html += row_html
    bars_html += "</div>"
    st.markdown(bars_html, unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="imbalance-row">
            <div class="imbalance-label">Imbalance:</div>
            <div class="imbalance-pill">{imbalance}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    view_capacity = st.button("View capacity details  ▶")
    go_to_assessment = st.button("Assessment  ▶")

    return view_capacity, go_to_assessment