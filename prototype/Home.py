"""IQB Streamlit Prototype - Main Entry Point"""

import streamlit as st
from iqb import IQB, IQB_CONFIG

st.set_page_config(page_title="IQB Prototype", page_icon="📊", layout="wide")

st.title("Internet Quality Barometer (IQB)")
st.write("Phase 1 Prototype - Streamlit Dashboard")

st.markdown("""
### Welcome to the IQB Prototype

This dashboard implements the Internet Quality Barometer framework, which assesses
Internet quality beyond simple "speed" measurements by considering multiple use cases
and their specific network requirements.

**Current status**: Under active development
""")

# Smoke test: calculate IQB score
try:
    iqb = IQB()
    score = iqb.calculate_iqb_score()

    st.success("✓ IQB Library loaded successfully")
    st.metric("Sample IQB Score", f"{score:.3f}")

    with st.expander("View IQB Configuration"):
        st.json(IQB_CONFIG)

except Exception as e:
    st.error(f"Error loading IQB library: {e}")

st.info("Dashboard functionality coming soon...")
