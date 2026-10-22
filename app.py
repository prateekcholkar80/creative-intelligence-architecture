"""Run with: .venv/bin/python -m streamlit run app.py"""
from src.application.campaign_serving_startup import startup
from src.ui.campaign_chat import main

startup()
main()
