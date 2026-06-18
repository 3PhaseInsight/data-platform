import streamlit as st

from ui.dag_results.parsing import (
    get_phase_recommendation,
    get_recommended_actions,
)


def render_assessment_view(meter_id: str, data: dict):
    assessment_type = st.session_state.get("assessment_type")

    rec_item = get_phase_recommendation(data)

    if rec_item and rec_item.get("details"):
        details = rec_item["details"]
    else:
        details = {}

    feeder_data = details.get("phase_consumption", {})
    sm_data = details.get("sm_id_consumption", {})

    st.markdown(
        """
        <style>
        .result-card {
            background-color: #FBE8CC;
            border-radius: 18px;
            padding: 28px;
            margin: 24px 0;
        }
        .result-title {
            font-weight: 700;
            font-size: 20px;
            margin-bottom: 10px;
        }
        .result-body {
            font-size: 18px;
            font-weight: 600;
        }
        .actions-heading {
            font-weight: 700;
            font-size: 17px;
            margin-bottom: 4px;
        }
        .action-item {
            font-size: 15px;
            padding: 8px 0;
            border-bottom: 1px solid #ccc;
        }
        .warning-card {
            background-color: #ffe6e6;
            padding: 20px;
            border-radius: 12px;
            margin: 20px 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title(f"Assessment: {assessment_type}")

    # CASE 1: Heat Pump -> real data
    if assessment_type == "HP":
        best_phase = details.get("recommended_phase", "N/A")
        actions = get_recommended_actions(rec_item)

        st.markdown(
            f"""
            <div class="result-card">
                <div class="result-title">Result</div>
                <div class="result-body">
                    {best_phase} is the best suggestion
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            '<div class="actions-heading">Recommended actions</div>',
            unsafe_allow_html=True,
        )

        if actions:
            for action in actions:
                st.markdown(
                    f'<div class="action-item">{action}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.markdown(
                '<div class="action-item">No actions available</div>',
                unsafe_allow_html=True,
            )

    # CASE 2: EV / PV -> not ready
    else:
        st.markdown(
            """
            <div class="warning-card">
                Results are not ready for this appliance yet.
            </div>
            """,
            unsafe_allow_html=True,
        )

        request = st.button("Request assessment for phase connection?")

        if request:
            st.success("Request submitted (placeholder)")

    st.markdown("###")

    back = st.button("◀ Back to status")

    return back
