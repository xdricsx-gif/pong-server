const test = require('node:test');
const assert = require('node:assert/strict');
const { accountId, PRODUCTS, validatePurchase, createBillingService } = require('./play-billing');

function fixture(overrides = {}) {
  return { purchaseStateContext: { purchaseState: 'PURCHASED' }, obfuscatedExternalAccountId: accountId('alice'),
    productLineItem: [{ productId: 'gold_100', productOfferDetails: { quantity: 1, refundableQuantity: 1,
      consumptionState: 'CONSUMPTION_STATE_YET_TO_BE_CONSUMED' } }], ...overrides };
}
function setup({ purchase = fixture(), consumeFails = false, enabled = true } = {}) {
  const data = new Map([['users_private/alice', { gold: 100 }], ['users_private/bob', { gold: 50 }]]);
  let queue = Promise.resolve();
  const snapshot = ref => ({ exists: data.has(ref.path), data: () => structuredClone(data.get(ref.path)) });
  const db = {
    collection: name => ({ doc: id => ({ path: name + '/' + id, get: async function () { return snapshot(this); },
      update: async function (value) { data.set(this.path, { ...data.get(this.path), ...value }); } }) }),
    runTransaction: callback => {
      const result = queue.then(async () => {
        const writes = [];
        const result = await callback({ get: async ref => snapshot(ref),
          update: (ref, fields) => writes.push(() => data.set(ref.path, { ...data.get(ref.path), ...fields })),
          create: (ref, value) => writes.push(() => { assert.equal(data.has(ref.path), false); data.set(ref.path, value); }) });
        writes.forEach(write => write()); return result;
      });
      queue = result.catch(() => {}); return result;
    },
  };
  let consumeCount = 0;
  const publisher = {
    checkAccess: async () => {}, getPurchase: async () => structuredClone(purchase),
    consume: async () => { consumeCount++; if (consumeFails) throw new Error('network'); },
  };
  const service = createBillingService({ db, publisher, enabled, verifyIdToken: async token => {
    if (!['alice','bob','guest'].includes(token)) throw new Error('invalid auth');
    return { uid: token, firebase: { sign_in_provider: token === 'guest' ? 'anonymous' : 'google.com' } };
  } });
  const buy = fields => service.verify({ idToken: 'alice', productId: 'gold_100', purchaseToken: 'token-1', ...fields });
  return { data, service, buy, consumeCount: () => consumeCount };
}
test('catalog uses the five existing package quantities', () => assert.deepEqual(Object.values(PRODUCTS), [100,250,600,1400,3500]));
test('account identity is stable and hashed', () => { assert.match(accountId('alice'), /^[a-f0-9]{64}$/); assert.notEqual(accountId('alice'),accountId('bob')); });
for (const [name, mutation, code] of [
  ['pending', p => p.purchaseStateContext.purchaseState = 'PENDING', 'pending'],
  ['cancelled', p => p.purchaseStateContext.purchaseState = 'CANCELLED', 'purchase_not_completed'],
  ['wrong account', p => p.obfuscatedExternalAccountId = accountId('bob'), 'wrong_account'],
  ['missing account binding', p => delete p.obfuscatedExternalAccountId, 'wrong_account'],
  ['wrong product', p => p.productLineItem[0].productId = 'gold_3500', 'invalid_product'],
  ['zero quantity', p => p.productLineItem[0].productOfferDetails.quantity = 0, 'invalid_quantity'],
  ['fractional quantity', p => p.productLineItem[0].productOfferDetails.quantity = 1.5, 'invalid_quantity'],
  ['refunded', p => p.productLineItem[0].productOfferDetails.refundableQuantity = 0, 'purchase_refunded'],
]) test('rejects ' + name + ' without awarding currency', async () => {
  const purchase = fixture(); mutation(purchase); const s = setup({ purchase });
  await assert.rejects(s.buy(), { code }); assert.equal(s.data.get('users_private/alice').gold,100);
  assert.equal(s.consumeCount(),0);
});
test('credits, consumes, and safely replays the same purchase', async () => {
  const s = setup(); assert.equal((await s.buy()).creditedGold,100);
  assert.equal((await s.buy()).creditedGold,0);
  assert.equal(s.data.get('users_private/alice').gold,200);
  assert.equal(s.data.size,3);
});
test('concurrent duplicate notifications only credit once', async () => {
  const s = setup(); const results = await Promise.all([s.buy(),s.buy(),s.buy()]);
  assert.equal(results.reduce((sum,r)=>sum+r.creditedGold,0),100);
  assert.equal(s.data.get('users_private/alice').gold,200);
});
test('failed consume does not lose purchase or award twice', async () => {
  const s = setup({ consumeFails: true });
  assert.equal((await s.buy()).consumptionPending,true);
  assert.equal((await s.buy()).creditedGold,0); assert.equal(s.consumeCount(),2);
  assert.equal(s.data.get('users_private/alice').gold,200);
});
test('consumed token with no local ledger cannot be claimed', async () => {
  const purchase=fixture();purchase.productLineItem[0].productOfferDetails.consumptionState='CONSUMPTION_STATE_CONSUMED';
  const s=setup({purchase});await assert.rejects(s.buy(),{code:'already_consumed'});
});
test('multi-quantity purchase uses verified Google quantity', () => {
  const p=fixture();p.productLineItem[0].productOfferDetails.quantity=3;p.productLineItem[0].productOfferDetails.refundableQuantity=3;
  assert.equal(validatePurchase(p,'alice','gold_100').gold,300);
});
test('fake client amount cannot change grant', async () => {
  const s=setup();await s.buy({gold:10000000,quantity:999});assert.equal(s.data.get('users_private/alice').gold,200);
});
test('different account cannot claim another account purchase', async () => {
  const s=setup();await s.buy();await assert.rejects(s.buy({idToken:'bob'}),{code:'wrong_account'});
  assert.equal(s.data.get('users_private/bob').gold,50);
});
test('invalid authentication fails before grant', async () => {const s=setup();await assert.rejects(s.buy({idToken:'forged'}),{code:'auth_required'});});
test('anonymous account cannot pay', async () => {const s=setup();await assert.rejects(s.service.prepare({idToken:'guest'}),{code:'registered_account_required'});});
test('disabled configuration fails closed', async () => {const s=setup({enabled:false});await assert.rejects(s.buy(),{code:'billing_unavailable'});});
test('prepare returns matching stable account and catalog',async()=>{const s=setup();const p=await s.service.prepare({idToken:'alice'});assert.equal(p.accountId,accountId('alice'));assert.deepEqual(p.productIds,Object.keys(PRODUCTS));});
