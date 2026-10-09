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
- 13:10 UTC: F4 review (gemini) REQUEST-CHANGES accepted: cancel marks shared entries Failed permanently (reader Clone shares cache) = regression for library users. Follow-up commit (no force push — 8294719e9 pinned by Comet 8d59a5855): cancellation releases claim, waiters reload; DeleteLoad -> Result. Task rp-f4-impl-r4. Comet re-pin after it lands green.

## Standing instruction (user, 2026-10-08)
Genuine bug on upstream main + not tracked upstream (read-only dedupe search) + 200% certain (GHA repro on unmodified main + root cause) → open issue on matching unikdahal fork. Never upstream. Unsure → skip.
Candidate: Apache Comet main string-join wrong results seen in bench baseline (fuzz *_fz_str, strjoin broadcast_distinct) — needs confirmation.
- ~13:05 UTC: F4 follow-up landed iceberg 157cf6475 (CI 37778485388 green); lead reviewed waiter loop. Comet 8cd4d9a98 pins it; CI running. Bench shard task should pin newest GREEN comet (may become 8cd4d9a98).

## 2026-10-08 13:55 UTC heartbeat
- Bench 37758359536 DONE: candidate 0 validation failures; baseline (main f80042f78) 160 string-join mismatches again → main has wrong-results bug that fork avoids. RCA task rp-strjoin-rca-r1 (gemini) — fork-issue candidate if 200% proven + untracked upstream.
- Perf analysis task rp-bench-analysis-r1 (gemini). Note candidate 92bba61b6 still had 9cccd0676.
- Comet CI 37780938158 @8cd4d9a98 running. Bench shard task (codex) can now push (bench done).
- 14:20 UTC: M1 (main bug) RCA: broadcast coalescing of sliced string vectors (offset[0]>0) in Utils.scala; fork fix 422cb76f1. Untracked upstream/fork. GHA proof task rp-strjoin-repro-r1 (test-only on main must fail; with fix pass). File fork issue only if confirmed.
- 14:40 UTC: bench analysis r1 PRELIMINARY — headline geomeans mix unequal coverage (main Spark fallbacks); fallback reasons appeared guessed; shuffle-probe claim suspicious. Round 2 (rp-bench-analysis-r2) for coverage-equal numbers, on/off isolation, real fallback reasons, regression list. Do not cite r1 numbers as results.
- M1 repro runs: A 37792317030 (main+tests) / B 37792322262 (+fix). Diff A vs B = Utils.scala only (lead verified).

## 2026-10-08 15:20 UTC — bench 37758359536 analysis r2 (accepted with caveats)
- Equal-coverage cand_on/base_on: join 3.49x, topk 3.87x, layouts 2.59x, tpch 0.98x, strjoin 0.98x. off/off: 1.36/1.66/1.00/0.99/1.03.
- 25 unequal-coverage queries from main's metadata-table suffix bug (M2) — tracked upstream already → no fork issue.
- Discounted: report's "JIT profiling" claims (no profiling exists).
- Perf follow-ups: dynamic-filter overhead on non-selective joins (strjoin/tpch on/off 0.97-0.99); count_all__f_evolved 0.88x; join_inner__f_unsorted__dim_empty 0.88x.

## 2026-10-08 15:35 UTC — per-query RCA program (user directive)
- Every query: ledger sorted slowest→fastest, RCA line + classification, deep dives on CI<1, pruning-costs-time, under-delivers, >10x suspicious.
- Tasks: join+topk (codex), layouts+tpch (gemini), strjoin+fuzz (gemini). Outputs rp-work/reports/rca/.
- Then: fixes (codex) → targeted bench → re-verify on next full campaign. TODO: emit ledger automatically in bench report job (after shard task lands).
- 14:55 UTC heartbeat: Comet 8cd4d9a98 GREEN. First sharded campaign 37788815352 running (pins 434d18333/1e3f2a868). Repro runs + 3 RCA tasks + shard task in flight.

## 2026-10-08 15:20 UTC
- M1 CONFIRMED via GHA A/B (main+tests: 6 fail incl E2E SQL; +fix: 18 pass). Filed fork issue unikdahal/datafusion-comet#42 (per standing instruction; untracked upstream).
- RCA layouts/tpch + strjoin/fuzz done (reports/rca/). fuzz: candidate 4000/4000 exact.
- P1: adaptive bypass + zero-copy for join dynamic filter (codex rp-adaptive-bypass-r1).
- Deferred: tpch clustered under-delivery (layout-inherent), Range native, cross-shuffle pruning. count_all__f_evolved recheck in sharded campaign 37788815352.
- 15:45 UTC: join/topk RCA done (144 exact; 5 small join regressions all "pruning-on costs time w/o I/O savings"). Backlog P1,B1..B5 in state.json. B1 design (gemini) started; B2/B3/B4 wait for P1 (same files).
- B1 design accepted; decimal phase queued after P1.
- Bench sharding landed (51478edbc); first sharded campaign 37788815352 shards ~45 min each. Watcher running.
- 16:00 UTC: sharded campaign 37788815352 done in 1h46m. Candidate 0 failures; baseline string-join failures again. Delta-ledger task rp-delta-37788815352-r1 (gemini).
- 16:20 UTC: delta analysis 37788815352 vs 37758359536: shuffle-probe revert confirmed beneficial; most small "regressions" were noise. POLICY: regression requires CI<1 in 2 campaigns. B6 added (safe float all-match).
- 16:45 UTC: P1 adaptive bypass landed c739c322e (CI green). Targeted bench 21f15844d pushed. B1 decimal impl started (codex).
- 16:40 UTC: B1 decimal pushed 901348bea/1bfd6239b; uncommitted Spark lineage-guard WIP in comet-decimal; codex limited until 17:07 UTC -> resume.
- 16:55 UTC: targeted bench 37807233835: P1 accepted (eval cost 4-6x lower, selective pruning intact). dim_10pct regression confirmed -> B4 design (gemini). Decimal continuation waits for codex reset 17:07.
- 17:10 UTC: Comet branch head 1bfd6239b RED (decimal). rp-b1-decimal-r2 fixing. Last green Comet: c739c322e.
- B4 design accepted (gate advisory row_filter on actual RG/page pruning). Queued after decimal.

## 2026-10-09 — parallel operating model (user directive: never idle)
- Workstreams W1..W7 on separate adaptive-ci/ws-* branches (see state.json "workstreams"); lead reviews + merges into integration branch; full validation only on integration branch once fast lane (W3) lands.
- Providers: codex, gemini-3.8-flash-high ×4, glm-5.3-free ×2 (grok disabled).
- Next after merges: combined targeted bench (decimal, B4, B3, B2, B6 queries) → full sharded campaign.
- W5 re-dispatched to opencode/big-pickle (glm-5.3-free channel unavailable).
- W4 re-dispatched to gemini (glm-5.3-free unavailable; avoid tokenrouter free channel).
- W1 decimal DONE (bef150221 green). Targeted decimal bench c43593010. W1b string started (codex). Gemini quota hit -> sonnet subagents fixing ws-b4 CI and fast-ci lane.
- B6 merged into iceberg integration (3e0b1ca54). Sonnet shepherds on ws-b4, ws-fast-ci, ws-b3, ws-b2.
- Upstream audit done; refactor track R2/R3 (iceberg) + R5 (Comet) started on ws branches; R1/R4/R6 queued.
- B4 merged into iceberg integration (f3a157f81). Await CI → Comet pin bump + B4 targeted bench.
- Comet 6e7d582b5 pins iceberg f3a157f81 (B4+B6). B4 bench queued after decimal bench.
- R2/R3 iceberg hygiene merged into iceberg integration (rebased).
- Fast CI lane merged into Comet integration (681bdcbff). ws-* branches now ~30 min CI.
- Decimal bench: f_dec queries 0.96x -> 2.06-5.19x (I/O 588->12 MiB). B4/B6 targeted bench 7c2dd47e8 started.
- 19:58 UTC heartbeat: iceberg 0c3d9eede green; batching Comet pin bump with ws-b3/ws-b2 merges. Cancelled stale ws-b3 full run.
- R4: gemini claimed CI green but clippy failed (ColumnStatsSelection dead after making include_column_stats cfg(test)). Lead decision: stats retention is intended public API → restored pub (c4e8a66e9); split kept; independent pure-extraction review running. LESSON: always verify agent CI claims with gh.
- R4 merged into iceberg integration (c4e8a66e9). Started fuzz-coverage extension + dim_empty RCA (gemini).
- dim_empty RCA: second driver manifest scan per runtime-filtered scan (+4.9ms on 16 files; scales). Design task started.
- Fuzz extension ready (c8437490b) for next full campaign.

## 2026-10-09 ~23:20 UTC
- B4 targeted bench: B4 REGRESSED selective unsorted queries (unsorted dim_128 2.41x→1.01x; topk10 unsorted 3.91x→3.00x) → REVERT (gemini). Also broke Comet test (reader row-filter expectation) → Comet integration 681bdcbff red.
- Manifest rescan design done; impl queued (skip guards + snapshot cache).
- W1b string → gemini (codex at capacity). ws-b3/b2 rust fixes → big-pickle. R5 green, merge with pin bump after revert.
- B4 reverted (iceberg 1f154f5f3); Comet c408a06bd = R5 + pin. Manifest stats cache impl started (codex).
- String join keys merged (iceberg 17d0a1232, Comet 007080ae0). R1 allowlist unification unblocked. Next: targeted string bench.
- Targeted string bench cbb461b77 started; R1 allowlist unification started (gemini).
- Manifest stats cache ready (0635e00d8, reviewed). Batch merge with R1 pending.
- String bench: f_str joins 2.6-4.8x, unsorted dim_128 2.50x restored; topk10 unsorted ratio diff is runner noise (work identical). B7 string TopK queued.
- Codex weekly limit until 2026-10-14 12:15 IST. B7 string TopK -> gemini.
- 02:20 UTC: manifest cache merged (fcd020283). ACTIONS ANOMALY: runs queued/not created on comet fork. B7 pushed 5608eab25 awaiting CI.
- R1 rejected (would disable decimal/string pruning); fix dispatched. ACTIONS BLOCKED on comet fork — escalated to user.
- R1 v2 rejected (Scala widenings). Finding: decimal/string get NO driver file-level stats today (row-group/page pruning only) -> new B8.
- R1 v3 pushed 1ca825a4d (awaiting CI). B8 design started.
- B8 design accepted; impl started stacked on R1 (CI blocked).
- 03:05 UTC: Actions still blocked. B8 pushed. Unvalidated queue recorded. B7+B8 review dispatched (big-pickle). /tmp cleaned.
- 04:10 UTC: Actions disabled at ACCOUNT level (dispatch 422). ws-b3/ws-b2 fixed (pin restore). Worktrees audited clean. B7/B8 review on gemini.
- B7 + B8 reviews: APPROVE (nits only). All work blocked on Actions re-enable.
- 09:20 UTC: Actions RECOVERED. Dispatched validation for integration + r1/b7/b8/b3/b2.
- Integration fcd020283 GREEN. b3+b2 green+reviewed -> staging ws-merge-b3b2. R1/B8 fmt fixes + B7 compile fix dispatched (gemini).
- B7 green 4e4ace6be; queued for merge after b3b2 staging.
- Integration ff to 93d2ddc0b (b3+b2). B7 staged on top: ws-merge-b7 0c32f9d36.
- R1 3029829dc + B8 e7fa1781e green; reconciling onto B7 staging (semantic: TopK/MinMax string widening).
- B7 staging green; integration will ff to ws-merge-r1b8 once green.
- Office PDF summary produced (~/Downloads, notes reports/). r1b8 staging green 2aec2d370; delta review pending before integration ff.
