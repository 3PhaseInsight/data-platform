import streamlit as st
import matplotlib.pyplot as plt

from ui.dag_results.parsing import (
    get_imbalance_level,
    get_phase_recommendation,
    get_scores,
)

IMBALANCE_COLORS = {
    "Low": "#2E7D32",
    "Moderate": "#E08A00",
    "High": "#C62828",
    "Unknown": "#555555",
}

def plot_pie(data: dict, title: str):
    filtered = {k: v for k, v in data.items() if v > 0}

    if not filtered:
        st.warning(f"No data for {title}")
        return

    fig, ax = plt.subplots(figsize=(4, 4))

    ax.pie(
        filtered.values(),
        labels=filtered.keys(),
        autopct="%1.1f%%",
        startangle=140,
    )

    ax.set_title(title, pad=40, fontweight="bold")
    ax.axis("equal")

    st.pyplot(fig, transparent=True)


def render_status_view(meter_id: str, data: dict):
    if not data:
        st.error(
            "No data loaded for this meter yet. Please go back and look it up again."
        )
        back = st.button("◀ Back", key="back_to_lookup_nodata")
        return None, back

    rec_item = get_phase_recommendation(data)
    scores = get_scores(rec_item)
    imbalance = get_imbalance_level(scores)
    imbalance_color = IMBALANCE_COLORS.get(imbalance, "#555555")

    rec_item = get_phase_recommendation(data)
    details = rec_item.get("details") if rec_item else {}

    feeder_data = details.get("phase_consumption", {})
    sm_data = details.get("sm_id_consumption", {})

    st.markdown(
        f"""
        <style>
        .meter-heading {{
            font-size: 32px;
            color: #2a2a2a;
            font-weight: 500;
        }}
        .meter-sub {{
            font-size: 16px;
            margin-bottom: 20px;
        }}
        .imbalance-pill {{
            background-color: #fff;
            color: {imbalance_color};
            font-weight: 600;
            padding: 6px 16px;
            border-radius: 10px;
            display: inline-block;
            margin-top: 10px;
        }}

        /* Assess buttons: full width on every screen size, including the
           narrow layout Streamlit switches to on phones. Targets the
           keyed container below so it doesn't affect other buttons. */
        .st-key-assess_buttons div[data-testid="stHorizontalBlock"] {{
            gap: 12px;
        }}
        .st-key-assess_buttons div[data-testid="column"] {{
            width: 100% !important;
            flex: 1 1 0 !important;
            min-width: 0 !important;
        }}
        .st-key-assess_buttons .stButton > button {{
            width: 100%;
        }}

        @media (max-width: 640px) {{
            .st-key-assess_buttons div[data-testid="stHorizontalBlock"] {{
                flex-direction: column;
            }}
            .st-key-assess_buttons div[data-testid="column"] {{
                width: 100% !important;
            }}
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([6, 1])
    with col1:
        st.markdown(
            f'<div class="meter-heading">Smart meter: {meter_id}</div>',
            unsafe_allow_html=True,
        )

    with col2:
        back = st.button("◀ Back", key="back_to_lookup")

    st.markdown(
        f'<div class="imbalance-pill">Imbalance: {imbalance}</div>',
        unsafe_allow_html=True,
    )

    st.markdown("###")

    col1, col2 = st.columns(2)

    with col1:
        plot_pie(feeder_data, "Feeder phase consumption")

    with col2:
        plot_pie(sm_data, f"Meter {meter_id} phase consumption")

    st.markdown("###")

    with st.container(key="assess_buttons"):
        col1, col2, col3 = st.columns(3)

        with col1:
            hp = st.button("Assess one-phase HP")

        with col2:
            ev = st.button("Assess one-phase EV")

        with col3:
            pv = st.button("Assess one-phase PV")

    if hp:
        selected = "HP"
    elif ev:
        selected = "EV"
    elif pv:
        selected = "PV"
    else:
        selected = None

    return selected, back