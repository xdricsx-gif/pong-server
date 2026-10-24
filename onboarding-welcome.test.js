'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createWelcome, WELCOME_TEXT, MESSAGE_ID, GOLD_REWARD} = require('./onboarding-welcome');
const NOW = Date.parse('2026-10-24T18:00:00Z');
const SENDER = 'verified-drics', PLAYER = 'player-a';
const CHAT = [PLAYER, SENDER].sort().join('_');
const MESSAGE = `chats/${CHAT}/messages/${MESSAGE_ID}`;
const clone = value => structuredClone(value);

// Optimistic transaction fixture: reads register document versions, commits
// conflict/retry atomically, and reading after the first write is rejected.
function database(initial = {}) {
  const data = new Map(Object.entries(initial).map(([key, value]) => [key, clone(value)]));
  const versions = new Map();
  const stats = {attempts: 0, commits: 0, writes: 0};
  let failCommit = false, beforeCommit = null;
  function ref(key) {return {key, collection(name) {return {doc(id) {return ref(`${key}/${name}/${id}`);}};}};}
  function external(key, value) {data.set(key, clone(value)); versions.set(key, (versions.get(key) || 0) + 1);}
  const db = {
    collection(name) {return {doc(id) {return ref(`${name}/${id}`);}};},
    async runTransaction(fn) {
      for (let attempt = 0; attempt < 64; attempt++) {
        stats.attempts++; const reads = new Map(), changes = []; let writing = false;
        const tx = {
          async get(r) {assert.equal(writing, false, 'transaction reads must precede writes'); reads.set(r.key, versions.get(r.key) || 0); const value = clone(data.get(r.key)); return {exists: data.has(r.key), data: () => value};},
          set(r, value, options) {writing = true; changes.push({key: r.key, value: clone(value), merge: options?.merge});},
          create(r, value) {writing = true; changes.push({key: r.key, value: clone(value), create: true});},
          update(r, value) {writing = true; changes.push({key: r.key, value: clone(value), merge: true, update: true});},
        };
        const result = await fn(tx);
        if (beforeCommit) {const hook = beforeCommit; beforeCommit = null; await hook();}
        if ([...reads].some(([key, version]) => (versions.get(key) || 0) !== version)) continue;
        if (failCommit) throw new Error('commit_failed');
        for (const change of changes) {
          if (change.create && data.has(change.key)) throw new Error('already_exists');
          if (change.update && !data.has(change.key)) throw new Error('missing_document');
        }
        for (const change of changes) external(change.key, change.merge ? {...data.get(change.key), ...change.value} : change.value);
        stats.commits++; stats.writes += changes.length;
        return result;
      }
      throw new Error('too_many_retries');
    },
  };
  return {db, data, stats, external, fail(value) {failCommit = value;}, before(fn) {beforeCommit = fn;}};
}
function initial(extra = {}) {
  return {
    [`users_private/${PLAYER}`]: {gold: 100, silver: 300, flightSchool: {version: 2, step: 6, completed: true}},
    [`users_public/${PLAYER}`]: {nickname: 'Пілот', avatarId: 2},
    [`users_public/${SENDER}`]: {nickname: 'DRICS', avatarId: 7},
    [`flight_school_rewards/${PLAYER}`]: {version: 2, silver: 300, claimedAt: NOW - 1000},
    ...extra,
  };
}
function fixture(extra = {}, options = {}) {
  const store = database(initial(extra));
  const service = createWelcome({db: store.db, senderUid: SENDER, now: () => NOW, ...options});
  return {...store, service};
}
const receipt = uid => `onboarding_welcome_rewards/${uid}`;
const delivery = uid => `onboarding_welcome/${uid}`;


test('welcome text is exact and signs as Ruslan while sender remains DRICS', async () => {
  assert.match(WELCOME_TEXT, /^Дякую, що ти тут! 💙💛\n\nПривіт!/);
  assert.ok(WELCOME_TEXT.endsWith('Приємної гри!\nЗ повагою, Руслан'));
  assert.ok(WELCOME_TEXT.includes('Тримай 100 золота 🪙'));
  const f = fixture(); const result = await f.service.ensure(PLAYER), message = f.data.get(MESSAGE);
  assert.equal(message.text, WELCOME_TEXT); assert.equal(message.fromUid, SENDER); assert.equal(message.fromNick, 'DRICS');
  assert.equal(result.created, true); assert.equal(result.delivered, true); assert.equal(result.claimable, true);
  assert.equal(result.messageId, MESSAGE_ID); assert.equal(result.gold, 100);
  assert.deepEqual(message.reward, {id: MESSAGE_ID, currency: 'gold', amount: 100, claimed: false, claimedAt: null});
  assert.ok(message.date instanceof Date);
});

test('delivery creates a replyable normal chat and updates unread counters only once', async () => {
  const mine = `users_private/${PLAYER}/msgThreads/${SENDER}`, theirs = `users_private/${SENDER}/msgThreads/${PLAYER}`;
  const f = fixture({[mine]: {unread: 2, custom: 'keep'}, [theirs]: {unread: 4}});
  await f.service.ensure(PLAYER); const again = await f.service.ensure(PLAYER);
  assert.equal(again.created, false); assert.equal(again.delivered, true);
  assert.deepEqual(f.data.get(`chats/${CHAT}`).participants, [PLAYER, SENDER].sort());
  assert.equal(f.data.get(mine).unread, 3); assert.equal(f.data.get(mine).custom, 'keep');
  assert.equal(f.data.get(mine).withUid, SENDER); assert.equal(f.data.get(mine).avatarId, 7);
  assert.equal(f.data.get(theirs).unread, 4); assert.equal(f.data.get(theirs).withUid, PLAYER); assert.equal(f.data.get(theirs).withNick, 'Пілот');
  assert.equal([...f.data.keys()].filter(k => k.endsWith('/messages/welcome_v1')).length, 1);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100);
  assert.equal(f.data.get(`users_private/${PLAYER}`).silver, 300);
});

test('concurrent reconnects create exactly one message and one unread increment', async () => {
  const f = fixture(); const results = await Promise.all(Array.from({length: 20}, () => f.service.ensure(PLAYER)));
  assert.equal(results.filter(r => r.created).length, 1);
  assert.equal(f.data.get(`users_private/${PLAYER}/msgThreads/${SENDER}`).unread, 1);
  assert.equal([...f.data.keys()].filter(k => k.startsWith('onboarding_welcome/')).length, 1);
});

test('completed field alone cannot deliver or authorize a claim without the server tutorial receipt', async () => {
  const f = fixture(); f.data.delete(`flight_school_rewards/${PLAYER}`);
  const result = await f.service.ensure(PLAYER);
  assert.equal(result.eligible, false); assert.equal(result.delivered, false); assert.equal(f.data.has(MESSAGE), false);
  await assert.rejects(f.service.claim(PLAYER), /tutorial_incomplete/);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100);
});

test('receipt alone cannot authorize an incomplete lesson or a false final step', async () => {
  for (const flightSchool of [{version: 2, step: 6, completed: false}, {version: 2, step: 5, completed: true}, {version: 2, step: 7, completed: true}]) {
    const f = fixture({[`users_private/${PLAYER}`]: {gold: 100, flightSchool}});
    assert.equal((await f.service.ensure(PLAYER)).eligible, false);
    await assert.rejects(f.service.claim(PLAYER), /tutorial_incomplete/);
  }
});

test('legacy completion receipt remains eligible without paying the old silver reward twice', async () => {
  const f = fixture({[`users_private/${PLAYER}`]: {gold: 100, silver: 300, flightSchool: {version: 1, step: 11, completed: true}}, [`flight_school_rewards/${PLAYER}`]: {version: 1}});
  assert.equal((await f.service.ensure(PLAYER)).claimable, true); await f.service.claim(PLAYER);
  assert.equal(f.data.get(`users_private/${PLAYER}`).silver, 300);
  assert.deepEqual(f.data.get(`flight_school_rewards/${PLAYER}`), {version: 1});
});

test('claim adds exactly 100 gold and marks the message and private receipt atomically', async () => {
  const f = fixture(); await f.service.ensure(PLAYER);
  const result = await f.service.claim(PLAYER, {amount: 999999, uid: 'victim'});
  assert.equal(result.gold, 200); assert.equal(result.claimed, true); assert.equal(result.claimable, false); assert.equal(result.grantedGold, GOLD_REWARD);
  assert.equal(f.data.get(receipt(PLAYER)).gold, 100); assert.equal(f.data.get(MESSAGE).reward.claimed, true);
  assert.equal(f.data.get(MESSAGE).reward.claimedAt, NOW); assert.equal(f.data.has(receipt('victim')), false);
});

test('double taps and concurrent claims grant once, including a lost acknowledgment retry', async () => {
  const f = fixture(); await f.service.ensure(PLAYER);
  const results = await Promise.all(Array.from({length: 20}, () => f.service.claim(PLAYER)));
  assert.equal(results.filter(r => !r.alreadyClaimed).length, 1);
  assert.equal(results.reduce((sum, r) => sum + r.grantedGold, 0), 100);
  const restarted = createWelcome({db: f.db, senderUid: SENDER, now: () => NOW + 1000});
  const retry = await restarted.claim(PLAYER);
  assert.equal(retry.alreadyClaimed, true); assert.equal(retry.gold, 200); assert.equal(retry.grantedGold, 0);
});

test('purchase concurrent with gift retries from the current balance instead of losing either update', async () => {
  const f = fixture(); await f.service.ensure(PLAYER);
  f.before(() => {
    const key = `users_private/${PLAYER}`, wallet = f.data.get(key);
    f.external(key, {...wallet, gold: wallet.gold - 30});
  });
  const result = await f.service.claim(PLAYER);
  assert.equal(result.gold, 170); assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 170);
  assert.equal(f.data.get(receipt(PLAYER)).gold, 100);
});

test('transaction failure cannot leave a message without a delivery receipt or grant without a claim receipt', async () => {
  const f = fixture(); f.fail(true);
  await assert.rejects(f.service.ensure(PLAYER), /commit_failed/);
  assert.equal(f.data.has(MESSAGE), false); assert.equal(f.data.has(delivery(PLAYER)), false);
  f.fail(false); await f.service.ensure(PLAYER); f.fail(true);
  await assert.rejects(f.service.claim(PLAYER), /commit_failed/);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100); assert.equal(f.data.has(receipt(PLAYER)), false); assert.equal(f.data.get(MESSAGE).reward.claimed, false);
  f.fail(false); assert.equal((await f.service.claim(PLAYER)).gold, 200);
});

test('ban check applies to delivery and claim, including after the message was delivered', async () => {
  const f = fixture({[`dashboard_admin_players/${PLAYER}`]: {banned: true}});
  await assert.rejects(f.service.ensure(PLAYER), /account_blocked/); await assert.rejects(f.service.claim(PLAYER), /account_blocked/);
  f.data.delete(`dashboard_admin_players/${PLAYER}`); await f.service.ensure(PLAYER);
  f.external(`dashboard_admin_players/${PLAYER}`, {banned: true});
  await assert.rejects(f.service.claim(PLAYER), /account_blocked/); assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100);
});

test('fake client message or supplied reward fields cannot establish delivery eligibility', async () => {
  const f = fixture({[MESSAGE]: {fromUid: PLAYER, reward: {amount: 99999, claimed: false}}});
  await assert.rejects(f.service.claim(PLAYER), /welcome_not_delivered/);
  await f.service.ensure(PLAYER);
  assert.equal(f.data.get(MESSAGE).fromUid, SENDER); assert.equal(f.data.get(MESSAGE).reward.amount, 100);
  f.external(MESSAGE, {...f.data.get(MESSAGE), reward: {amount: 99999, currency: 'silver', claimed: false}});
  assert.equal((await f.service.claim(PLAYER)).grantedGold, 100);
});

test('sender configuration must reference an existing trusted account, never a nickname lookup', async () => {
  const f = fixture({}, {senderUid: undefined}); await assert.rejects(f.service.ensure(PLAYER), /welcome_unavailable/);
  const g = fixture(); g.data.delete(`users_public/${SENDER}`); await assert.rejects(g.service.ensure(PLAYER), /sender_unavailable/);
  const h = fixture({[`dashboard_admin_players/${SENDER}`]: {banned: true}}); await assert.rejects(h.service.ensure(PLAYER), /sender_unavailable/);
  assert.equal(g.data.has(MESSAGE), false);
});

test('previously delivered messages remain bound to their original sender when configuration changes', async () => {
  const f = fixture(); await f.service.ensure(PLAYER);
  const changed = createWelcome({db: f.db, senderUid: 'different-sender', now: () => NOW});
  assert.equal((await changed.ensure(PLAYER)).senderUid, SENDER);
  assert.equal((await changed.claim(PLAYER)).senderUid, SENDER);
  assert.equal(f.data.get(MESSAGE).fromUid, SENDER);
});

test('missing or malformed recipients cannot access reward transactions', async () => {
  const f = fixture();
  for (const uid of ['', null, undefined, 'someone/path', 'x'.repeat(129)]) {
    await assert.rejects(f.service.ensure(uid), /auth_required/); await assert.rejects(f.service.claim(uid), /auth_required/);
  }
  await assert.rejects(f.service.ensure('missing-user'), /profile_missing/);
  assert.equal(f.stats.writes, 0);
});

test('sender account never receives a self-message or a self-reward', async () => {
  const f = fixture({[`users_private/${SENDER}`]: {gold: 100, flightSchool: {version: 2, step: 6, completed: true}}, [`flight_school_rewards/${SENDER}`]: {version: 2}});
  const result = await f.service.ensure(SENDER);
  assert.equal(result.delivered, false); assert.equal(result.reason, 'sender_account');
  await assert.rejects(f.service.claim(SENDER), /welcome_not_delivered/);
});

test('corrupted wallet or mismatched server delivery receipt fails closed', async () => {
  for (const gold of [-1, '100', NaN]) {
    const f = fixture({[`users_private/${PLAYER}`]: {gold, flightSchool: {version: 2, step: 6, completed: true}}});
    await assert.rejects(f.service.ensure(PLAYER), /invalid_wallet/); assert.equal(f.data.has(MESSAGE), false);
  }
  const f = fixture(); await f.service.ensure(PLAYER);
  f.external(delivery(PLAYER), {...f.data.get(delivery(PLAYER)), uid: 'victim'});
  await assert.rejects(f.service.claim(PLAYER), /welcome_state_invalid/);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100);
});

test('deleting a display message cannot erase the one-time claim receipt', async () => {
  const f = fixture(); await f.service.ensure(PLAYER); f.data.delete(MESSAGE);
  await f.service.claim(PLAYER); const again = await f.service.claim(PLAYER);
  assert.equal(again.alreadyClaimed, true); assert.equal(again.gold, 200); assert.equal(f.data.has(MESSAGE), false);
});

test('existing parent chat participants cannot be replaced with a different pair', async () => {
  const f = fixture({[`chats/${CHAT}`]: {participants: ['other-user', SENDER]}});
  await assert.rejects(f.service.ensure(PLAYER), /conversation_conflict/);
  assert.deepEqual(f.data.get(`chats/${CHAT}`).participants, ['other-user', SENDER]);
  assert.equal(f.data.has(delivery(PLAYER)), false); assert.equal(f.data.has(MESSAGE), false);
});

test('a ban applied during a gift transaction is observed on retry before any grant', async () => {
  const f = fixture(); await f.service.ensure(PLAYER);
  f.before(() => f.external(`dashboard_admin_players/${PLAYER}`, {banned: true}));
  await assert.rejects(f.service.claim(PLAYER), /account_blocked/);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, 100); assert.equal(f.data.has(receipt(PLAYER)), false);
});

test('gift cannot overflow the integer wallet balance', async () => {
  const f = fixture({[`users_private/${PLAYER}`]: {gold: Number.MAX_SAFE_INTEGER, flightSchool: {version: 2, step: 6, completed: true}}});
  await f.service.ensure(PLAYER);
  await assert.rejects(f.service.claim(PLAYER), /invalid_wallet/);
  assert.equal(f.data.get(`users_private/${PLAYER}`).gold, Number.MAX_SAFE_INTEGER);
});
