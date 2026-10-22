# Project navigation map

Verified 2026-10-19. Maintained routing map; generated files and individual modules are intentionally omitted. Update affected entries when moving modules and verify stale paths with a scoped listing.

```text
creative-intelligence/
├── AGENTS.md                  Rules and checkpoints
├── .agents/skills/             Repository-local skills
├── .codex/config.toml          Coding-assistant defaults
├── app.py                     Streamlit entry point
├── src/
│   ├── application/           Protected services, access and workflows
│   ├── common/                Shared utilities/configuration
│   ├── ingestion/             Source ingestion
│   ├── retrieval/             Product/image/evidence/analytics retrieval
│   ├── generators/            Campaign/banner generation
│   ├── ui/                    Streamlit interactions/runtime connection
│   └── validation/            Explicit diagnostic/integration runners
├── tests/                     Mainly unittest modules
├── config/
│   ├── clarification/
│   └── evaluation/
├── sql/                       BigQuery SQL
├── deploy/
│   ├── marketing/
│   └── prompt-registration/
├── docs/                      Designs, plans and evidence limits
├── assets/                    Visual assets
├── data/                      Source/export/generated evidence
└── requirements.txt           Dependencies
```

Start with [AI_SOLUTION_ARCHITECT_ROLE.md](AI_SOLUTION_ARCHITECT_ROLE.md) and [TOKEN_EFFICIENCY.md](TOKEN_EFFICIENCY.md). Consult only the relevant part of [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for milestone scope.

| Work | Starting point |
| --- | --- |
| Chat | `src/ui/campaign_chat.py` |
| Questions | `src/ui/question_chat.py` |
| Runtime | `src/ui/workflow_runtime.py`, matching application services |
| Products | `src/retrieval/product_knowledge.py` |
| Access | `src/application/access_policy.py`, `tests/test_access_policy.py` |

Find filenames first: `rg --files src/application tests -g '*interpreter*'`. Search deeper within that component. Avoid recursive trees of data, caches, virtual environments or the whole repository.
