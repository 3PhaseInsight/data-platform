import streamlit as st
from ui.shared.api import get_latest_result
from ui.dag_results.status_view import render_status_view
from ui.dag_results.assessment_view import render_assessment_view


def render_dag_results():
    if "page" not in st.session_state:
        st.session_state.page = "lookup"
    if "data" not in st.session_state:
        st.session_state.data = None
    if "meter_id" not in st.session_state:
        st.session_state.meter_id = None

    # --- Global styling ---
    st.markdown(
        """
        <style>
        .stApp {
            background-color: #FFC800;
        }

        .title {
            font-size: 48px;
            font-weight: 600;
            color: #754C24;
            margin-bottom: 5px;
        }

        .subtitle {
            font-size: 20px;
            margin-bottom: 30px;
        }

        .st-key-input_card {
            background-color: #A7A7A7;
            padding: 25px;
            border-radius: 20px;
            max-width: 500px;
            margin: 0 auto;
        }

        .stButton > button {
            width: 100%;
            height: 38px;
            border-radius: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.session_state.page == "lookup":
        _render_lookup()
    elif st.session_state.page == "status":
        view_capacity, go_to_assessment = render_status_view(
            st.session_state.meter_id, st.session_state.data
        )
        if go_to_assessment:
            st.session_state.page = "assessment"
            st.rerun()
        if view_capacity:
            st.info("Capacity details view not implemented yet.")
    elif st.session_state.page == "assessment":
        back = render_assessment_view(st.session_state.meter_id, st.session_state.data)
        if back:
            st.session_state.page = "status"
            st.rerun()


def _render_lookup():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.image("ui/3pha_Std-naked_4K.png", width=900)

    st.markdown('<div class="title">Smart meter lookup</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="subtitle">Input smart meter number</div>',
        unsafe_allow_html=True,
    )

    with st.container(key="input_card"):
        col_input, col_button = st.columns([3, 1])
        with col_input:
            meter_id = st.text_input("", placeholder="Enter meter ID")
        with col_button:
            submit = st.button("Lookup")

    if submit and meter_id:
        with st.spinner("Fetching data..."):
            try:
                data = get_latest_result("default_dag", meter_id)
                st.session_state.data = data
                st.session_state.meter_id = meter_id
                st.session_state.page = "status"
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
    elif submit:
        st.warning("Enter a meter ID")
