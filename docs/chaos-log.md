# Chaos log

One entry per experiment, newest last. Feeds NOTES.md §4–5.

## Template

```
## C<n>. <fault> during <moment>

- **Setup:** commit, env vars (CHAOS_SEED, *_FAIL_PCT, *_HANG_PCT), workflow
- **Command:** exactly what was run, so it can be repeated
- **Predicted:** what we expected the record and the ledgers to say
- **Observed:** what they said (run id, key output, trimmed)
- **Record true?** yes / no / stuck, and which step disagreed
- **Understanding:** what the gap between prediction and result taught us
- **Follow-up:** slice / PR that fixed it, or "accepted" with a reason
```

---
