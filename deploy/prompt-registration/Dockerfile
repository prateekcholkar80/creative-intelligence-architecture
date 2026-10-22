# Reuse the existing verified immutable dependency image and non-root user.
FROM asia-south1-docker.pkg.dev/project-661d501a-e709-4276-a26/ci-operator-images/prompt-registration@sha256:12f81c8df8bfa204cd2b0412df06f2f2ba6680186e8ee5b7dceca4069da8a5f6
WORKDIR /app
COPY src/ src/
COPY config/ config/
COPY sql/ sql/
COPY deploy/prompt-registration/app.py app_prompt_registration.py
COPY docs/CAMPAIGN_ACCEPTANCE_POLICY.md docs/CAMPAIGN_ACCEPTANCE_POLICY.md
COPY docs/CAMPAIGN_ACCEPTANCE_POLICY_V02.md docs/CAMPAIGN_ACCEPTANCE_POLICY_V02.md
COPY data/generated/campaign_e1_diagnostics/catalog_import_verification_20261015.json data/generated/campaign_e1_diagnostics/catalog_import_verification_20261015.json
RUN python -c "import json; from pathlib import Path; from dataclasses import replace; from src.validation.campaign_semantic_benchmark import load_cases; from src.application.campaign_full_quality_checks import FullQualityEvaluator; from src.application.campaign_interpretation_evaluation import InterpretationEvaluationSettings; catalog,cases=load_cases(); assert len(cases)==159; bundle=json.loads(Path('config/evaluation/interpretation_pilot_deployment.json').read_text()); settings=[InterpretationEvaluationSettings(**v) for v in bundle.values()]; assert len(settings)==3; assert sum(not s.recovery_only for s in settings)==1; assert all(FullQualityEvaluator(replace(catalog,contract_version=s.judge_contract_version),replace(catalog,contract_version=s.judge_contract_version).sha256,lambda *a:None).evaluator_sha256==s.evaluator_sha256 for s in settings)"
RUN python -c "from src.application.campaign_interpretation_evaluation import ProtectedInterpretationEvaluation"
# Entrypoint, non-root user and security settings are inherited unchanged.
