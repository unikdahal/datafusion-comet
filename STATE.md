# Runtime pruning — engineering checkpoint

Notes branch: `adaptive-notes/runtime-pruning` on unikdahal/datafusion-comet (matches no workflow trigger).
Machine-readable twin: `state.json`. Update both after every milestone.

## Verified state (2026-10-08 ~09:45 UTC)
- iceberg-rust `adaptive-ci/runtime-pruning-rewrite-20261006` = 949cc88 (matches handoff).
- comet `adaptive-ci/runtime-pruning-rewrite-20261006` = 92bba61b6 (matches handoff); native/Cargo.toml pins iceberg 949cc88.
- Apache comet main at checkpoint: f80042f78 (already merged into candidate via 793506a54).

## CI
- Comet validation run 37755479696 @92bba61b6: in progress (native+rust passed; spark_sql + integration 3.5/4.1 running).
- iceberg-rust: no CI on branch (upstream ci.yml only main/PR). Fork workflow being added (task rp-iceberg-ci-wf-r1).

## Benchmarks
- Bench branch `adaptive-bench/rewrite-20261007-main-integrated` (comet fork). Push cancels in-progress run.
- Run 37703972798 (pins ce049/... older): all 280 validation failures were BASELINE-only (Apache main):
  string-join mismatches (fuzz *_fz_str, strjoin broadcast_distinct) + OversizedAllocationException. 0 candidate failures.
  Jobs fail (exit 1) when baseline fails — harness behavior, not candidate defect. Open question: why baseline main wrong on string joins.
- Run 37746953413 @15ec197 (pins comet 4226d7e3 / iceberg 288a0ee): cancelled by superseding push.
- Run 37758359536 @163b5c235 (pins comet 92bba61b6 / iceberg 949cc88): STARTED. First campaign including fixes 3+4. ~3.5h.

## Missing evidence
- Previous report `/workspace/reports/runtime-pruning-close-inspection-20261008.md` + JSON: Codex cloud only, NOT accessible. Reconstruct from bench artifacts.

## Open reviews
- rp-iceberg-review-r1b (gemini-3.8-flash-high; r1 on gemini-pro-agent cancelled): bad79ba, 288a0ee, 949cc88 adversarial.
- rp-comet-review-r1 (Codex): 9cccd0676, 92bba61b6, 4226d7e3c, 058991ede.

## Next
1. Collect reviews; triage findings; regression tests via GHA.
2. Analyze run 37758359536 artifacts (per-query RCA on regressions).
3. Investigate baseline string-join mismatch (does candidate contain a fix not in main? or harness?).

## 2026-10-08 10:30 — iceberg review r1b (gemini-3.8-flash) triaged by lead
- F1 VERIFIED latent defect: strict evaluator `not_eq`/`not_in` NaN literal → MUST_MATCH; reader fast path skips row filter → NaN rows leak. Library-only (Comet unreachable). Fix task rp-iceberg-nan-literal-fix-r1 (codex 6.1 sol), worktree rp-work/iceberg-nanfix.
- F2 rejected (file bounds cover splits). F3 only consequence of F1.
- Judged SAFE by reviewer (not yet independently re-verified): empty projection w/ deletes, range reuse, truncated bounds, 3VL.
- After fix lands + Comet review returns: bump Comet iceberg pin in one commit.

## 2026-10-08 10:50 — Comet review r1 (codex 6.1 sol)
- Fork CI workflow landed on iceberg: 5faa5bdba.
- F1 widened: float semantics mismatch strict-evaluator vs Arrow (NaN literal, NaN-only column w/ equality delete of NaN, signed zero). Fix = reader guard excluding float binary/set predicates from all-match shortcut (task rp-iceberg-float-guard-r2). Evaluator untouched (upstream code).
- F4 (P2): detached delete-loading after cancel → leaked I/O + unreported metrics. Next after F1.
- F5: shuffle-probe filter removal = cost policy, unproven; need selective shuffle probe bench.
- F6 (P3): skipped-filter counter misses shuffle rejection; bundle into Comet pin bump.

## 2026-10-08 11:00 — CI
- iceberg fork CI run 37758600458 @5faa5bdba GREEN (verified via gh): fmt, clippy -D warnings, 2,360 tests, 95 doctests.
- Comet validation 37755479696 @92bba61b6: integration (3.5) + (4.1) FAILED. Last green 37703958482 @058991ede. Diagnosis task rp-comet-integ-fail-r1. Top priority alongside F1.

## 2026-10-08 ~10:05 UTC
- F1 fix pushed iceberg 5f326d103 (reader guard: float/double Binary+Set preds keep row filter; unit + e2e NaN equality-delete tests). CI 37760274980: fmt FAILED (line width) → local fix b3b63ba76 in rp-work/iceberg-nanfix, PUSH AFTER clippy/tests of 37760274980 finish.
- Codex hit usage limit (resets 12:04 UTC). Reassigned: integration diagnosis → gemini (rp-comet-integ-fail-r1g); float-guard upstream review → gemini (rp-float-guard-review-r1).
- Float guard review (gemini, rp-float-guard-review-r1): correctness APPROVED (only skip site; unary NaN preds agree; nested ok). Applied: is_floating_type simplification → local 0703d53f5 (amended fmt commit). Declined: test import change (inline path is file convention). Deferred F7: evaluator NaN-literal arm.

## 2026-10-08 10:30 UTC
- Comet integration failure root cause (gemini, verified by lead): 9cccd0676 disables upstream-tested shuffle-probe dynamic filter → CometJoinSuite SHUFFLE_HASH/AQE assertions fail. DECISION: revert (local 72981e4c3); do not weaken test. Re-propose only as benchmarked cost policy. Bench run 37758359536 includes 9cccd0676 → its join numbers = "no shuffle-probe filter" variant; useful A/B evidence.
- iceberg float guard: 5f326d103 failed fmt + clippy (f32 literal fallback). Amended → 1e3f2a868 pushed; CI running (watcher).
- NEXT: on iceberg green, bump Comet pin to 1e3f2a868 + push with revert; then bench pins update.

## 2026-10-08 10:45 UTC
- iceberg 1e3f2a868 GREEN (run 37761104305).
- Comet pushed 72981e4c3 (revert 9cccd0676) + 434d18333 (pin iceberg 1e3f2a868). CI 37761771346 running.
- Bench 37758359536 left running (pins 92bba61b6/949cc88); do NOT push bench until it finishes (~13:15 UTC).
- F4 design delegated (gemini, rp-f4-design-r1).
- F4 CONFIRMED (gemini design, lead verified spawn is upstream-original b9b6c7e01). DECISION: minimal abort-on-drop handle paired with receiver; reject waiter refcounting. Impl by codex after 12:04 UTC.

## 2026-10-08 11:50 UTC heartbeat
- Comet 434d18333 GREEN (run 37761771346, all jobs). Both impl branches now remotely validated.
- F4 impl dispatched (codex, rp-f4-impl-r1).
- Bench 37758359536 still running; when done: analyze + pin bench to comet 434d18333 / iceberg 1e3f2a868 (or later head if F4 lands green first).
- 12:07 UTC: F4 re-dispatched as rp-f4-impl-r2 (r1 hit codex limit).

## 2026-10-08 ~12:50 UTC
- F4 implemented iceberg 8294719e9 (DeleteLoad future, abort on drop; gated tests pos+eq). CI 37775247993 GREEN. Lead checked: only error paths drop DeleteLoad early.
- Comet 8d59a5855 pins iceberg 8294719e9; CI running.
- F4 upstream-style review (gemini rp-f4-review-r1) running — key Q: abort mid-load leaving half-populated delete state visible as complete?
- Bench target after Comet green: comet 8d59a5855 / iceberg 8294719e9.
- 13:00 UTC: user asked faster benchmarks w/o accuracy loss. Decision: query sharding (each shard = full 8-round balanced protocol, paired on same runner), build cache per SHA, data-gen cache, runner CPU metadata, targeted mode. Codex task rp-bench-shard-r1 (waits for bench 37758359536 before push; also repins candidate to newest green Comet).
