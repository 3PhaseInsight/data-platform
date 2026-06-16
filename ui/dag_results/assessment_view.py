import streamlit as st

from ui.dag_results.parsing import get_phase_recommendation, get_recommended_actions


def render_assessment_view(meter_id: str, data: dict):
    rec_item = get_phase_recommendation(data)
    best_phase = (rec_item.get("details") or {}).get("recommended_phase", "N/A") if rec_item else "N/A"
    actions = get_recommended_actions(rec_item)

    st.markdown(
        """
        <style>
        .meter-heading {
            font-size: 32px;
            color: #f5f5f5;
            font-weight: 500;
            margin-bottom: 4px;
        }
        .meter-sub {
            color: #e6e6e6;
            font-size: 15px;
            margin: 2px 0;
        }
        .result-card {
            background-color: #FBE8CC;
            border-radius: 18px;
            padding: 28px 26px;
            margin: 24px 0 30px 0;
        }
        .result-title {
            color: #B5651D;
            font-weight: 700;
            font-size: 20px;
            margin-bottom: 14px;
        }
        .result-body {
            color: #8c4a14;
            font-weight: 600;
            font-size: 18px;
            line-height: 1.4;
        }
        .actions-heading {
            color: #2a2a2a;
            font-weight: 700;
            font-size: 17px;
            margin-bottom: 4px;
        }
        .action-item {
            color: #2a2a2a;
            font-weight: 600;
            font-size: 15px;
            padding: 10px 0 8px 0;
            border-bottom: 1px solid #2a2a2a;
            margin-bottom: 6px;
        }
        .data-footer {
            color: #3a3a3a;
            font-size: 13px;
            margin-top: 40px;
        }
        div[data-testid="stButton"] > button {
            background-color: #2D4FE0;
            color: white;
            border: none;
            border-radius: 12px;
            height: 48px;
            font-size: 16px;
            width: 100%;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f'<div class="meter-heading">Smart meter: {meter_id}</div>', unsafe_allow_html=True)
    st.markdown('<div class="meter-sub">3-phase connection</div>', unsafe_allow_html=True)
    st.markdown('<div class="meter-sub">Address: If available*</div>', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="result-card">
            <div class="result-title">Result:</div>
            <div class="result-body">{best_phase} is the best suggestion</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="actions-heading">Recommended actions</div>', unsafe_allow_html=True)

    if actions:
        for action in actions:
            st.markdown(f'<div class="action-item">{action}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="action-item">No actions available</div>', unsafe_allow_html=True)

    generated_at = data.get("generated_at", "")
    st.markdown(
        f'<div class="data-footer">Data generated at {generated_at}</div>',
        unsafe_allow_html=True,
    )

    back = st.button("◀ Back to status")
    return back
