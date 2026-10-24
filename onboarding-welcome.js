'use strict';

const {normalize} = require('./flight-school');
const VERSION = 1;
const MESSAGE_ID = 'welcome_v1';
const GOLD_REWARD = 100;
const DELIVERY_COLLECTION = 'onboarding_welcome';
const CLAIM_COLLECTION = 'onboarding_welcome_rewards';
const WELCOME_TEXT = `Дякую, що ти тут! 💙💛

Привіт! Ти щойно пройшов навчання, і я хочу сказати тобі велике спасибі за те, що серед тисяч ігор ти обрав саме мою.

Трохи про мене: я український розробник, і цю гру роблю сам, від першого рядка коду до кожної кнопки, яку ти бачиш на екрані. Без великої студії та команди, лише я, ідея і багато безсонних ночей. Тому кожен гравець для мене справді важливий.

Якщо щось не працює, є ідея чи просто хочеш поділитися враженнями, це все можна написати прямо сюди у чат зі мною. Я читаю все і саме завдяки гравцям гра стає кращою.

І наостанок: дякую, що дочитав до кінця! Мало хто це робить, тому ти заслуговуєш на маленький подарунок. Тримай 100 золота 🪙, нехай воно допоможе тобі на старті.

Приємної гри!
З повагою, Руслан`;
const PREVIEW = WELCOME_TEXT.split('\n')[0];
const fail = code => Object.assign(new Error(code), {code});
const validUid = uid => typeof uid === 'string' && uid.length > 0 && uid.length <= 128 && !/[\x00-\x1f/]/.test(uid);
const conversationId = (a, b) => [a, b].sort().join('_');
const unread = value => Number.isSafeInteger(value) && value >= 0 ? Math.min(value, 9999) : 0;
const avatar = value => Number.isSafeInteger(value) && value >= 0 ? value : 0;

/**
 * Callers must pass only a verified session UID, never a body-supplied recipient.
 * Delivery and reward receipts are private, server-only collections. A client
 * message, nickname or claimed amount never establishes reward eligibility.
 */
function createWelcome({db, senderUid, senderNick = 'DRICS', now = Date.now}) {
  const privateRef = uid => db.collection('users_private').doc(uid);
  const publicRef = uid => db.collection('users_public').doc(uid);
  const deliveryRef = uid => db.collection(DELIVERY_COLLECTION).doc(uid);
  const claimRef = uid => db.collection(CLAIM_COLLECTION).doc(uid);
  const threadRef = (uid, other) => privateRef(uid).collection('msgThreads').doc(other);
  const chatRef = id => db.collection('chats').doc(id);
  const messageRef = id => chatRef(id).collection('messages').doc(MESSAGE_ID);
  const name = typeof senderNick === 'string' && senderNick.trim() ? senderNick.trim().slice(0, 64) : 'DRICS';

  function available(uid) {
    if (!db) throw fail('server_unavailable');
    if (!validUid(uid)) throw fail('auth_required');
  }
  function walletGold(profile) {
    if (!Number.isSafeInteger(profile.gold) || profile.gold < 0) throw fail('invalid_wallet');
    return profile.gold;
  }
  function completion(profile, receipt) {
    try {
      return normalize(profile.flightSchool).completed && receipt.exists && [1, 2].includes(receipt.data()?.version);
    } catch { return false; }
  }
  function identity(uid, record) {
    if (!record || record.version !== VERSION || record.uid !== uid || !validUid(record.senderUid) ||
      record.senderUid === uid || record.messageId !== MESSAGE_ID || record.chatId !== conversationId(uid, record.senderUid)) {
      throw fail('welcome_state_invalid');
    }
    return {senderUid: record.senderUid, messageId: MESSAGE_ID, chatId: record.chatId};
  }
  async function readBase(tx, uid) {
    const [profile, tutorial, control, delivery, reward] = await Promise.all([
      tx.get(privateRef(uid)), tx.get(db.collection('flight_school_rewards').doc(uid)),
      tx.get(db.collection('dashboard_admin_players').doc(uid)), tx.get(deliveryRef(uid)), tx.get(claimRef(uid)),
    ]);
    if (control.data()?.banned) throw fail('account_blocked');
    if (!profile.exists) throw fail('profile_missing');
    return {profile: profile.data(), tutorial, delivery, reward};
  }
  function response(uid, state, record, created = false) {
    const eligible = completion(state.profile, state.tutorial);
    return {
      eligible, delivered: Boolean(record), created,
      claimable: Boolean(eligible && record && !state.reward.exists), claimed: state.reward.exists,
      ...(record ? identity(uid, record) : {senderUid: validUid(senderUid) ? senderUid : null, messageId: MESSAGE_ID, chatId: validUid(senderUid) ? conversationId(uid, senderUid) : null}),
      gold: walletGold(state.profile),
    };
  }

  async function ensure(uid) {
    available(uid);
    if (!validUid(senderUid)) throw fail('welcome_unavailable');
    return db.runTransaction(async tx => {
      const state = await readBase(tx, uid);
      if (state.delivery.exists) return response(uid, state, state.delivery.data());
      if (!completion(state.profile, state.tutorial) || uid === senderUid) {
        return {...response(uid, state, null), eligible: false, reason: uid === senderUid ? 'sender_account' : 'tutorial_incomplete'};
      }
      const id = conversationId(uid, senderUid);
      // Every read precedes every write, including the two existing unread counts.
      const [sender, recipient, senderControl, recipientThread, senderThread, existingChat] = await Promise.all([
        tx.get(publicRef(senderUid)), tx.get(publicRef(uid)),
        tx.get(db.collection('dashboard_admin_players').doc(senderUid)),
        tx.get(threadRef(uid, senderUid)), tx.get(threadRef(senderUid, uid)), tx.get(chatRef(id)),
      ]);
      if (!sender.exists || senderControl.data()?.banned) throw fail('sender_unavailable');
      const participants = existingChat.data()?.participants;
      if (participants !== undefined && (!Array.isArray(participants) || participants.length !== 2 ||
        [...participants].sort().join('\n') !== [uid, senderUid].sort().join('\n'))) throw fail('conversation_conflict');
      const at = +now();
      if (!Number.isFinite(at)) throw fail('server_unavailable');
      const date = new Date(at), alreadyClaimed = state.reward.exists;
      const record = {version: VERSION, uid, senderUid, senderNick: name, chatId: id, messageId: MESSAGE_ID, deliveredAt: at};
      tx.set(chatRef(id), {participants: [uid, senderUid].sort(), updatedAt: date}, {merge: true});
      tx.set(messageRef(id), {
        fromUid: senderUid, fromNick: name, text: WELCOME_TEXT, date,
        kind: 'developer_welcome', welcomeVersion: VERSION, recipientUid: uid,
        reward: {id: MESSAGE_ID, currency: 'gold', amount: GOLD_REWARD, claimed: alreadyClaimed, claimedAt: alreadyClaimed ? state.reward.data()?.claimedAt ?? null : null},
      });
      tx.set(threadRef(uid, senderUid), {
        withUid: senderUid, withNick: name, avatarId: avatar(sender.data()?.avatarId),
        lastMsg: PREVIEW, lastDate: date, unread: Math.min(9999, unread(recipientThread.data()?.unread) + 1),
      }, {merge: true});
      tx.set(threadRef(senderUid, uid), {
        withUid: uid, withNick: String(recipient.data()?.nickname || 'Гравець').slice(0, 64), avatarId: avatar(recipient.data()?.avatarId),
        lastMsg: PREVIEW, lastDate: date, unread: unread(senderThread.data()?.unread),
      }, {merge: true});
      tx.create(deliveryRef(uid), record);
      return response(uid, state, record, true);
    });
  }

  async function claim(uid) {
    available(uid);
    return db.runTransaction(async tx => {
      const state = await readBase(tx, uid);
      if (!completion(state.profile, state.tutorial)) throw fail('tutorial_incomplete');
      if (!state.delivery.exists) throw fail('welcome_not_delivered');
      const record = state.delivery.data(), details = identity(uid, record);
      const gold = walletGold(state.profile);
      if (state.reward.exists) return {...response(uid, state, record), alreadyClaimed: true, grantedGold: 0};
      if (!Number.isSafeInteger(gold + GOLD_REWARD)) throw fail('invalid_wallet');
      const message = await tx.get(messageRef(details.chatId));
      const at = +now();
      if (!Number.isFinite(at)) throw fail('server_unavailable');
      tx.update(privateRef(uid), {gold: gold + GOLD_REWARD});
      tx.create(claimRef(uid), {version: VERSION, uid, ...details, gold: GOLD_REWARD, claimedAt: at});
      // The private receipt is authoritative even if an old message was removed.
      // Never recreate a partial chat message merely to update its button.
      if (message.exists) tx.update(messageRef(details.chatId), {
        reward: {id: MESSAGE_ID, currency: 'gold', amount: GOLD_REWARD, claimed: true, claimedAt: at},
      });
      return {...response(uid, state, record), claimable: false, claimed: true, alreadyClaimed: false, grantedGold: GOLD_REWARD, gold: gold + GOLD_REWARD};
    });
  }

  return {ensure, claim};
}

module.exports = {createWelcome, WELCOME_TEXT, MESSAGE_ID, GOLD_REWARD};
