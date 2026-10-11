# Galaxy Football network protocol 2

Implemented locally for Android 20.6 (versionCode 26), 2026-10-11. This document does not record a production deployment.

## Problem and model

The old client predicted balls and fields separately from the server, sent paddle coordinates, and displayed remote paddles on a different timeline. A moving field could therefore hit a ball at a position different from its displayed owner.

Protocol `gf-net2-1` uses the exact same physics source in Node, the web client and the packaged Android assets. The server owns the result; the client predicts input for responsiveness and reconciles the complete world. It never applies an authoritative ball bounce to an unrelated paddle timeline.

| Component | Responsibility |
| --- | --- |
| `galaxy-physics.js` | Pure 60 Hz simulation, seeded randomness, all four balls/paddles/fields/magnets, complete checkpoint |
| `galaxy-netcode.js` | Server command history and bounded rollback; client prediction, replay, clock estimate and rendering sample |
| `galaxy-wire.js` | Compact full-state and input serialization, independent of previous packets |
| `netcode-worker.js` | Physics scheduling and rollback outside the main I/O thread |
| `netcode-server.js` | Worker pool, authenticated socket-to-slot binding, rate limits, state delivery and committed events |
| `server.js` | Existing matchmaking, verified player stats, authentication, rewards, legacy compatibility |
| `../app/www/index.html` | Existing game UI and renderer connected to the shared simulation |
| `../app/sync-netcode.mjs` | Syntax-check and copy the three canonical modules into web/Android assets |

## Time, input and collisions

- Simulation runs at 60 fixed ticks/second. Input contains tick, sequence, direction, monotonic boost edge and magnet state; no client-provided paddle coordinates are accepted by protocol 2.
- The client predicts its controls immediately, with a two-tick lead at ordinary RTT and a bounded additional lead at higher RTT. This is a compromise between responsiveness, upstream delay and the amount of unknown opponent input to predict.
- The authority accepts delayed input only after the last committed tick, up to eight ticks (about 133 ms). First accepted input for a slot/tick is immutable. Input more than fourteen ticks ahead is rejected.
- A late accepted command rewinds and replays **the entire world**, including all balls, moving paddles, held balls, energy, respawns, field flags, bot state and RNG. A 64-tick history bounds memory. Replay is batched once per simulation pump/snapshot.
- Paddle collisions use relative swept motion and the paddle's position at contact. Moving fields use relative circle sweeps. Rendering uses one coherent sample for paddles, held balls and fields.
- Magnet release starts at the held position; the previous 85-pixel displacement is removed. A checkpointed capture cooldown prevents immediate re-capture.
- Field visuals show their actual collision radius on activation; the old expanding visual radius did not match the already active collision boundary.
- Small central corrections with an unchanged collision can fade over 80 ms. New impacts, held balls and boundary corrections are not smoothed through geometry. Successive snapshots preserve any unfinished visual correction.

## Results and fairness

Goals, eliminations and final results become external events only after the eight-tick commit window. Replaying provisional physics cannot repeat a committed goal or award a second result. The UI displays committed lives/scores. Existing reward handling is called once for a completed authoritative match.

This is not a durable transaction log for server crashes: a whole-server restart still loses an in-memory match. Exactly-once commit within one running match does not promise cross-process exactly-once financial/reward delivery. Purchases remain on the existing independent billing path.

No algorithm can know a remote player's unreceived button press. A late field may still cause a visible trajectory correction. Commands older than the fairness window are discarded instead of rewriting an already confirmed goal. Increasing this window indiscriminately would make other players' outcomes less stable.

## Transport and failures

- Socket.IO/WebSocket remains the transport. This does **not** eliminate TCP head-of-line blocking or add UDP/WebRTC.
- State is sent at approximately 30 Hz using volatile delivery. Input repeats up to sixteen recent commands at approximately 30 Hz. Full snapshots avoid a missing-delta dependency.
- Two workers isolate physics from the main authentication/database/dashboard thread. Each worker allows only one unacknowledged state message per room; the adapter coalesces delivery and discards stale states rather than draining an old backlog.
- Catch-up is bounded. Worker metrics include discarded wall-clock debt and worst step/pump time; they are emitted as `[NET2]` records without user tokens.
- Commands expire after eight missing ticks, stopping held movement and magnet. Prediction is bounded to twenty ticks and pauses after 400 ms without a fresh snapshot. The UI then shows a connection-recovery notice and requests authoritative state again.
- Background/resume and reconnect use a fresh connection epoch so buffered input from the old connection cannot steer the resumed slot. Resume retries are bounded. Rejoin verifies the existing UID and checks slot ownership again after async account reads.
- Existing 30-second disconnect grace remains. Expiry eliminates the disconnected slot. Worker failure aborts affected matches without inventing a winner/rating reward.
- A worker pool improves isolation, not infinite capacity. Multi-room production load, mobile thermal throttling and real cellular outages still need field measurements.

## Compatibility and rollout

1. Keep the three shared modules byte-identical using `node sync-netcode.mjs` from the app directory.
2. Deploy the compatible server before the new Android client. Old clients remain on protocol 1; new and old protocol rooms are deliberately separated.
3. Verify a four-player match on real Android devices, including moving field impacts, simultaneous magnets, lock/unlock and Wi-Fi/mobile handoff, before promoting 20.6 widely.
4. Observe `[NET2]` worker metrics and `window.netcodeDiagnostics` on the client. Interpret `tooLate` as command copies, not unique lost user actions.
5. `NETCODE_V2=0` makes new matchmaking fall back to protocol 1. The new client retains its legacy path. Restarting/redeploying the server to change environment settings still interrupts in-memory matches; this is not a live drain mechanism.

On an unupdated production server, the local 20.6 APK uses the legacy path and therefore does not demonstrate the new protocol. A release upload must be signed with the approved production upload key; the local debug APK is not a Play release.

## Verification and reproducibility

Run from the server directory:

```text
node --test *.test.js
node netcode-chaos.js <output-report.json>
```

Run from the app directory:

```text
node sync-netcode.mjs
node --test netcode.test.mjs netcode-integration.test.mjs play-billing-client.test.mjs
```

Tests cover deterministic replay in separate Node/browser-style VM contexts, late field movement affecting four balls, reversal of provisional goals, one-time terminal commit, malformed/duplicate/old input, moving collision sweeps, four magnet-held balls, input expiry, lost state, coherent render samples, compact serialization, correction decay, a blocked main thread, real Socket.IO four-client sessions, protocol separation, background/resume and competing rejoin attempts. Browser-style VM equivalence does not by itself prove equivalence on every Android WebView version.

The chaos runner uses eight conditions and three seeds: 70–250 ms RTT, jitter, 0–5% application packet omission, ordered 500 ms delivery stalls, unordered delivery, asymmetric delay and 30/60 FPS. Each run simulates one minute with four independently predicted clients. It asserts finite bounded state, bounded history, no duplicate commits and complete recovery convergence. Omission of application snapshots is not a packet-level model of all TCP retransmission behavior.

Detailed numerical results and local build provenance are in `outputs/galaxy-football/netcode-v2` in the parent workspace. The in-app browser could not access the local QA server; interactive visual gameplay and touch controls have **not** been signed off. No claim of production-proven AAA stability follows from automated tests alone.
