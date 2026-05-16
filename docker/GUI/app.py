import streamlit as st

from state import init_session_state
from sections.environment import render_environment
from runtimes.controller import refresh_runtime_status
from sections.pipeline_flow import render_pipeline_flow
from sections.services_status import render_services_status
from sections.tabs import render_tabs
from sections.pipeline_status import render_pipeline_status
from sections.controls import render_pipeline_controls
from dotenv import load_dotenv
from pathlib import Path


def _load_env() -> None:
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent / "docker" / ".env",  # standard layout: gui/../docker/.env
        here.parent / ".env",  # flat layout: project root .env
        here / ".env",  # same directory as app.py
    ]
    for path in candidates:
        if path.exists():
            load_dotenv(path, override=False)  # don't overwrite vars already in shell
            break


_load_env()


st.set_page_config(page_title="Control Plane", layout="wide")


def main():
    init_session_state()

    st.title("HDMAS GUI")

    render_environment()  # process the runtime dropdown

    refresh_runtime_status()  # refresh status for whichever runtime is selected
    render_pipeline_controls()

    render_pipeline_flow()
    render_pipeline_status()
    render_services_status()
    render_tabs()


if __name__ == "__main__":
    main()
