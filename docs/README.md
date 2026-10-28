# Docs

The same three records, in two versions:

| | [`forengineer/`](forengineer/) | [`forai/`](forai/) |
| --- | --- | --- |
| For | people reading the design | the working sessions (Claude and the user) |
| Shows | the current state, explained | everything as it was written, with dated review notes |
| Written | rewritten after the work, section by section | appended during the work by `/decide`, `/slice` and `/break` |

- **Decisions** (D1–D6): [readable](forengineer/decisions.md) · [working log](forai/decisions.md)
- **Concerns** (K1–K62): [readable](forengineer/concerns.md) · [working log](forai/concerns.md)
- **Chaos experiments** (C1–C4): [readable](forengineer/chaos-log.md) · [working log](forai/chaos-log.md)

Start with `forengineer/`. Go to `forai/` for the history: what a decision first said, what
changed it, and the evidence at each step (run ids, timestamps, log lines). IDs are the
same in both. If the two disagree, `forai/` is the source and `forengineer/` is out of date.

[`html/`](html/) has the explainers drawn for D1 and D4 while they were being decided.
