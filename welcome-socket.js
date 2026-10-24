'use strict';

const ALLOWED_ERRORS = new Set(['auth_required','account_blocked','profile_missing','tutorial_incomplete','welcome_not_delivered','welcome_unavailable','sender_unavailable','welcome_state_invalid','conversation_conflict','invalid_wallet','server_unavailable','rate_limit','busy']);
const fail = code => Object.assign(new Error(code), {code});

// Shared across sockets: repeated logins cannot multiply welcome transactions.
// Only delivery knowledge is cached; wallet values always come from the service.
function createWelcomeGateway({getService, now = Date.now}) {
  const players = new Map();
  let lastSweep = -Infinity;
  function player(uid) {
    const at = now();
    if (at - lastSweep >= 60000) {
      lastSweep = at;
      for (const [key, state] of players) if (!state.ensurePending && !state.claimPending && at - state.touched > 15 * 60000) players.delete(key);
    }
    let state = players.get(uid);
    if (!state) {
      if (players.size >= 5000) {
        const evict = [...players].find(([, item]) => !item.ensurePending && !item.claimPending);
        if (!evict) throw fail('busy');
        players.delete(evict[0]);
      }
      state = {ensurePending: null, claimPending: null, delivered: false, lastStatus: -Infinity, lastClaim: -Infinity, lastBackground: -Infinity, lastTutorial: -Infinity, completionPending: false};
      players.set(uid, state);
    }
    state.touched = at;
    return state;
  }
  function service() {const value = getService(); if (!value) throw fail('server_unavailable'); return value;}
  function ensure(uid, state) {
    if (state.ensurePending) return state.ensurePending;
    state.ensurePending = Promise.resolve().then(() => service().ensure(uid)).then(result => {
      if (result.delivered) state.delivered = true;
      return result;
    }).finally(() => {state.ensurePending = null;});
    return state.ensurePending;
  }
  function background(uid, completed) {
    let state;
    try {state = player(uid);} catch {return;}
    if (state.delivered) return;
    const at = now();
    if (completed) {
      if (at - state.lastTutorial < 5000) return;
      state.lastTutorial = at;
    } else {
      if (at - state.lastBackground < 60000) return;
      state.lastBackground = at;
    }
    // Completion may race the initial login read while it still reports an
    // unfinished lesson. Recheck once after that read, never create duplicate jobs.
    if (completed && state.ensurePending) {
      if (state.completionPending) return;
      state.completionPending = true;
      state.ensurePending.catch(() => null).then(result => {
        if (!result?.delivered) return ensure(uid, state);
      }).catch(() => {}).finally(() => {state.completionPending = false;});
      return;
    }
    ensure(uid, state).catch(() => {});
  }
  function attach(socket, {getSession}) {
    function current() {
      const session = getSession();
      if (!socket.connected || !session || session.verified !== true || session.anonymous || typeof session.uid !== 'string' || !session.uid ||
        !Number.isFinite(session.expiresAt) || session.expiresAt <= now() || (socket.uid && socket.uid !== session.uid)) return null;
      return session;
    }
    function same(expected) {const live = current(); return live && live.uid === expected.uid && live.generation === expected.generation;}
    function register(event, claim) {
      socket.on(event, async (payload, ack) => {
        if (typeof payload === 'function') ack = payload;
        if (typeof ack !== 'function') return;
        const session = current();
        if (!session) {ack({ok: false, error: 'auth_required'}); return;}
        const respond = result => {
          if (!socket.connected) return;
          ack(same(session) ? result : {ok: false, error: 'auth_changed'});
        };
        try {
          const state = player(session.uid), at = now();
          let result;
          if (claim) {
            if (state.claimPending) throw fail('busy');
            if (at - state.lastClaim < 2000) throw fail('rate_limit');
            state.lastClaim = at;
            // uid, sender and reward amount in payload are intentionally unused.
            state.claimPending = Promise.resolve().then(() => service().claim(session.uid));
            try {result = await state.claimPending;} finally {state.claimPending = null;}
          } else {
            if (at - state.lastStatus < 5000) throw fail('rate_limit');
            state.lastStatus = at;
            result = await ensure(session.uid, state);
          }
          respond({ok: true, ...result});
        } catch (error) {
          const code = error?.code || error?.message;
          respond({ok: false, error: ALLOWED_ERRORS.has(code) ? code : 'server_unavailable'});
        }
      });
    }
    register('welcomeStatus', false);
    register('claimWelcomeReward', true);
    return {
      authenticated() {const session = current(); if (session) background(session.uid, false);},
      afterTutorial(uid) {const session = current(); if (session?.uid === uid) background(uid, true);},
    };
  }
  return {attach};
}
module.exports = {createWelcomeGateway};
