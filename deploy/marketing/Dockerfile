# Reuse the existing immutable, verified dependency image; no credential files.
FROM asia-south1-docker.pkg.dev/project-661d501a-e709-4276-a26/ci-operator-images/prompt-registration@sha256:6c13d1d0fee5bc806315bb3acfbb6fb31b2a33644d10ac9f0066fc831084c504
WORKDIR /app
ENV CI_DURABLE_WORKFLOW_UI_ENABLED=true
RUN python -m pip install --no-cache-dir pypdf==6.21.1
COPY src/ src/
COPY config/ config/
COPY sql/ sql/
COPY deploy/marketing/app.py marketing_app.py
RUN python -c "from src.ui.marketing_pilot import main"
CMD ["python", "-m", "streamlit", "run", "marketing_app.py", "--server.address=0.0.0.0", "--server.port=8080", "--server.headless=true", "--server.enableXsrfProtection=true", "--server.enableCORS=true"]
