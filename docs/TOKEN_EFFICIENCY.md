# Token-efficient development plan

Effective 2026-10-19. This development workflow supports the fixed MVP plan; it does not change application prompts, approved profiles or release gates.

## Reasoning effort defaults

| Task | Effort |
| --- | --- |
| Adding an isolated Python function | Low |
| Fixing straightforward bugs | Low |
| Creating REST API endpoints | Medium |
| Implementing business logic | Medium |
| Database schema changes | Medium |
| Designing RAG architecture | High |
| Complex concurrency problems | High |
| Security threat modeling | High |
| Enterprise architecture decisions | High |

Use these as starting points. Increase effort for ambiguity, authorization, destructive migrations, fencing, recovery or uncertain provider dispatch. Even a small function can require high effort. Choose the lowest effort that reliably completes the task; rework can erase savings.

## Workflow

1. Locate the module using [PROJECT_MAP.md](PROJECT_MAP.md), then read applicable instructions and relevant contracts.
2. Search specific filenames/symbols and inspect bounded regions plus necessary callers. Reuse findings instead of rereading unchanged files.
3. Give a brief plan for complex work. Preserve architecture and conventions; avoid unrelated edits, abstractions and unnecessary dependencies.
4. Make the smallest complete change, following the existing 2–3-file batch convention where practical.
5. Run affected offline tests first. Broaden tests for shared interfaces, failures, regression risk or required gates. Do not repeat passing suites without a reason.
6. Report files changed, key changes, tests/results and remaining issues concisely. Explain material risks; omit repeated requirements and obvious code narration.

Efficiency never weakens source authority, security, privacy, approvals or evaluation. Inspect enough code to understand behavior and finish the authorized task.

## Targeted commands

```sh
rg --files src/application tests -g '*interpreter*'
rg -n -m 15 'class AccessPolicyTests|def authorize' src/application/access_policy.py tests/test_access_policy.py
sed -n '25,95p' src/application/access_policy.py
.venv/bin/python -m unittest discover -s tests -p 'test_access_policy.py' -q
```

For noisy tests, keep a complete log and preserve the actual test exit status. Run this as one shell command:

```sh
test_log=$(mktemp /private/tmp/ci-tests.XXXXXX)
.venv/bin/python -m unittest discover -s tests -p 'test_access_policy.py' -q >"$test_log" 2>&1
test_status=$?
tail -n 60 "$test_log"
printf '\nFull log: %s\nExit status: %s\n' "$test_log" "$test_status"
exit "$test_status"
```

Read more of the retained log if needed; do not rerun merely to retrieve output. Avoid unrestricted discovery, full-file dumps and repository-wide searches when a component is known. Do not pipe tests into `head` or `tail` and mistake the pipe's status for success. `src/validation/` runners may use cloud resources; check their modes and authorization first.

## Configuration and skill

[../.codex/config.toml](../.codex/config.toml) sets medium reasoning and a proposed 3,000-token history-storage budget per tool result. This is a starting value, not a measured optimum, process-output limit, total-token cap or billing limit. Bound command output separately. Verbosity remains a commented option because model/client support varies.

Select task effort in supported client controls, or start a CLI session with one of:

```sh
codex -c model_reasoning_effort='"low"'
codex -c model_reasoning_effort='"medium"'
codex -c model_reasoning_effort='"high"'
```

Markdown cannot automatically switch effort. Project configuration depends on client support and trust; explicit session settings and managed policy can take precedence. Files do not retroactively change this conversation's effort. No invented hard-budget/task-router keys, global configuration, model selection or permission changes are included.

The repository-local skill is [token-efficient-development](../.agents/skills/token-efficient-development/SKILL.md). Invoke `$token-efficient-development` or use normal discovery; restart the client if it does not appear.

Verified official references, 2026-10-19: [configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference), [skill discovery](https://learn.chatgpt.com/docs/build-skills).

## Adoption and measurement

Apply this workflow to the next already-planned MVP tasks. Where the client exposes usage, compare similar tasks by total input/output/reasoning tokens, latency, retries and correctness/test outcomes. Keep unavailable metrics unknown. Review several representative tasks before adjusting defaults. No percentage savings or application-level optimization is claimed.
