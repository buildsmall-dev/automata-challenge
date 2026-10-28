---
name: decide
description: Work through one design fork in the lab automation challenge with the user (alternatives, assumptions, evidence, recommendation) and record the user's choice in docs/forai/decisions.md. Use when the user says /decide, or when a slice depends on an open design question.
---

# /decide <question>

The user makes the decision. Claude's job is to make the choice clear and honest.

## Steps

1. **State the question** in one sentence, and why it matters for the record being true.

2. **Two or three real alternatives.** Not straw men. For each one:
   - how it works here, concretely (subjects, streams, tables, who does what);
   - what it makes easy, and what it makes hard or impossible;
   - the **assumption** it rests on. Attack that assumption: is it true of this
     system? Cite `instrument.go`, the README, or a measurement.

3. **Settle facts by observation, not opinion.** If the answer depends on behaviour
   you could check (what NATS does on disconnect, what the instrument does on a ctx
   cancel), propose a small throwaway experiment, under 10 minutes, and run it if
   the user agrees. Label each claim: measured, inferred, or guess.

4. **Recommend one** and say what would change your mind. Then stop and ask the
   user to choose. Agreeing with the user is not the default.

5. **Record it** in `docs/forai/decisions.md` using the template there, including the
   alternatives rejected and the evidence. Keep it short: it becomes NOTES.md §2.
