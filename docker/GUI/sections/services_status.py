import streamlit as st

from utils.data_builders import build_services_df, color_state


def render_services_status():
    # --- Services Status placeholder ---
    st.subheader("Services Status")

    runtime = st.session_state.get("selected_runtime", "LOCAL")
    st.caption(f"Runtime services view: {runtime}")

    services_df = build_services_df()

    st.dataframe(
        services_df.style.map(color_state, subset=["State"]),
        width="stretch",
        hide_index=True,
    )

    st.divider()
