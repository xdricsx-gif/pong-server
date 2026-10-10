'use strict';

const { createHash } = require('node:crypto');
const PACKAGE_NAME = 'com.galaxyfootball.app';
const PRODUCTS = Object.freeze({
  gold_100: 100, gold_250: 250, gold_600: 600, gold_1400: 1400, gold_3500: 3500,
});
const accountId = uid => createHash('sha256').update('galaxy-football:' + uid).digest('hex');
const tokenId = token => createHash('sha256').update(token).digest('hex');
function billingError(code) { return Object.assign(new Error(code), { code }); }

function validatePurchase(purchase, uid, productId) {
  if (!Object.hasOwn(PRODUCTS, productId)) throw billingError('invalid_product');
  const state = purchase.purchaseStateContext?.purchaseState;
  if (state === 'PENDING') throw billingError('pending');
  if (state !== 'PURCHASED') throw billingError('purchase_not_completed');
  if (purchase.obfuscatedExternalAccountId !== accountId(uid)) throw billingError('wrong_account');
  const items = purchase.productLineItem;
  if (!Array.isArray(items) || items.length !== 1 || items[0].productId !== productId) {
    throw billingError('invalid_product');
  }
  const details = items[0].productOfferDetails || {};
  const quantity = details.quantity;
  if (!Number.isSafeInteger(quantity) || quantity < 1 || quantity > 100) throw billingError('invalid_quantity');
  if (details.refundableQuantity !== undefined && details.refundableQuantity !== quantity) {
    throw billingError('purchase_refunded');
  }
  return { quantity, gold: PRODUCTS[productId] * quantity,
    consumed: details.consumptionState === 'CONSUMPTION_STATE_CONSUMED' };
}

function createBillingService({ db, verifyIdToken, publisher, enabled }) {
  const inFlight = new Map();
  async function authenticate(idToken) {
    if (!enabled || !db || !publisher) throw billingError('billing_unavailable');
    if (typeof idToken !== 'string' || idToken.length > 10000) throw billingError('auth_required');
    let decoded;
    try { decoded = await verifyIdToken(idToken); } catch { throw billingError('auth_required'); }
    if (!decoded?.uid) throw billingError('auth_required');
    // Paid currency must belong to an account the player can recover.
    if (decoded.firebase?.sign_in_provider === 'anonymous') throw billingError('registered_account_required');
    return decoded.uid;
  }
  async function prepare({ idToken } = {}) {
    const uid = await authenticate(idToken);
    const profile = await db.collection('users_private').doc(uid).get();
    if (!profile.exists) throw billingError('profile_missing');
    await publisher.checkAccess();
    return { accountId: accountId(uid), productIds: Object.keys(PRODUCTS) };
  }
  async function verify({ idToken, productId, purchaseToken } = {}) {
    const uid = await authenticate(idToken);
    if (!Object.hasOwn(PRODUCTS, productId)) throw billingError('invalid_product');
    if (typeof purchaseToken !== 'string' || !purchaseToken || purchaseToken.length > 4096) {
      throw billingError('invalid_token');
    }
    const key = tokenId(purchaseToken);
    // Do not share another account's promise or purchase result.
    if (inFlight.has(key)) {
      await inFlight.get(key).catch(() => {});
      return verify({ idToken, productId, purchaseToken });
    }
    const work = (async () => {
      const purchase = await publisher.getPurchase(purchaseToken);
      const validated = validatePurchase(purchase, uid, productId);
      const ledgerRef = db.collection('play_purchase_ledger').doc(key);
      const userRef = db.collection('users_private').doc(uid);
      const result = await db.runTransaction(async tx => {
        const [ledger, user] = await Promise.all([tx.get(ledgerRef), tx.get(userRef)]);
        if (!user.exists) throw billingError('profile_missing');
        const balance = user.data().gold;
        if (!Number.isSafeInteger(balance) || balance < 0) throw billingError('invalid_balance');
        if (ledger.exists) {
          const previous = ledger.data();
          if (previous.uid !== uid || previous.productId !== productId) throw billingError('wrong_account');
          return { gold: balance, creditedGold: 0, alreadyGranted: true };
        }
        if (validated.consumed) throw billingError('already_consumed');
        const gold = balance + validated.gold;
        if (!Number.isSafeInteger(gold)) throw billingError('invalid_balance');
        tx.update(userRef, { gold });
        tx.create(ledgerRef, { uid, productId, quantity: validated.quantity, gold: validated.gold,
          orderId: purchase.orderId || null, testPurchase: !!purchase.testPurchaseContext,
          grantedAt: new Date().toISOString(), consumed: false });
        return { gold, creditedGold: validated.gold, alreadyGranted: false };
      });
      // Grant durably before consuming. An interrupted consume can safely retry;
      // the Firestore transaction prevents a second credit across server replicas.
      let consumptionPending = false;
      try {
        if (!validated.consumed) await publisher.consume(productId, purchaseToken);
        await ledgerRef.update({ consumed: true });
      } catch { consumptionPending = true; }
      return { ...result, consumptionPending, productId };
    })();
    inFlight.set(key, work);
    try { return await work; } finally { inFlight.delete(key); }
  }
  return { prepare, verify };
}

function createPublisher(credentials) {
  const { GoogleAuth } = require('google-auth-library');
  const auth = new GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/androidpublisher'] });
  const base = `https://androidpublisher.googleapis.com/androidpublisher/v3/applications/${PACKAGE_NAME}`;
  let checkedAt = 0;
  async function request(url, method = 'GET') {
    try {
      const client = await auth.getClient();
      const response = await client.request({ url, method, timeout: 15000 });
      return response.data;
    } catch (error) {
      const status = error.response?.status;
      if (status === 401 || status === 403) throw billingError('billing_unavailable');
      if (status === 400 || status === 404) throw billingError('invalid_purchase');
      // Never log Google errors: they can contain a purchase token in the URL.
      throw billingError('verification_unavailable');
    }
  }
  return {
    async checkAccess() {
      if (Date.now() - checkedAt < 60000) return;
      await request(`${base}/oneTimeProducts?pageSize=1`);
      checkedAt = Date.now();
    },
    getPurchase: token => request(`${base}/purchases/productsv2/tokens/${encodeURIComponent(token)}`),
    consume: (product, token) => request(`${base}/purchases/products/${encodeURIComponent(product)}/tokens/${encodeURIComponent(token)}:consume`, 'POST'),
  };
}

function registerBillingHandlers(socket, getService) {
  let busy = false;
  let windowStart = Date.now();
  let count = 0;
  for (const [event, method] of [['billing:prepare', 'prepare'], ['billing:verify', 'verify']]) {
    socket.on(event, async (payload, reply) => {
      if (typeof reply !== 'function') return;
      if (Date.now() - windowStart > 60000) { windowStart = Date.now(); count = 0; }
      if (busy || ++count > 20) return reply({ ok: false, code: 'try_again' });
      busy = true;
      try {
        const service = getService();
        if (!service) throw billingError('billing_unavailable');
        const result = await service[method](payload);
        reply({ ok: true, ...result });
      } catch (error) {
        const safeCodes = new Set(['billing_unavailable','auth_required','registered_account_required',
          'profile_missing','invalid_product','invalid_token','pending','purchase_not_completed',
          'wrong_account','invalid_quantity','purchase_refunded','invalid_balance','already_consumed',
          'invalid_purchase','verification_unavailable']);
        reply({ ok: false, code: safeCodes.has(error.code) ? error.code : 'verification_unavailable' });
      } finally { busy = false; }
    });
  }
}
module.exports = { PRODUCTS, PACKAGE_NAME, accountId, validatePurchase, createBillingService, createPublisher, registerBillingHandlers };
