---
name: token-efficient-development
description: Apply targeted repository inspection, risk-based reasoning effort and bounded test output when implementing or debugging Python changes in Creative Intelligence or tuning its development token usage.
---

# Token-efficient development

Start with repository `docs/PROJECT_MAP.md` to locate the affected module, then read applicable instructions and relevant contracts. Verify stale paths. Avoid loading the historical checkpoint archive or all design documents unless needed to establish current constraints.

Read relevant sections of `docs/TOKEN_EFFICIENCY.md` for effort selection and command examples. Low suits isolated functions and straightforward bugs; medium suits endpoints, business logic and schema changes; high suits architecture, concurrency and threat modeling. Escalate for actual ambiguity or risk, including identity, approvals, transaction recovery and uncertain model dispatch. Recommendations cannot change the active model's effort; use supported client controls.

Search known paths with specific symbols and bounded matches. Read the smallest useful region plus callers/tests when contracts cross boundaries. Reuse verified findings and implementations. Preserve architecture; avoid unrelated refactoring or dependencies. Follow the existing 2–3-file batch convention where practical.

Give a brief plan for complex work and implement the smallest complete change. Run affected offline tests first. Preserve exit status and complete logs for noisy commands; retrieve relevant failures from logs instead of repeating tests. Expand verification for shared contracts, regression risk, failures or required gates. Never truncate evidence needed to assess success.

Finish concisely with files changed, key behavior, verification and remaining issues; omit empty boilerplate. Never claim measured savings without usage evidence. Efficiency does not relax source authority, privacy, security, evaluation, approvals or release gates, and does not authorize cloud/model calls.
