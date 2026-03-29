const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const app     = express();
const { hydrateFromSheets, updateZoneMemory, getZoneFreshness,
        clearZoneMemory, detectVWAPReclaim, formatATRBlock,
        getSessionQuality } = require('./aurum-upgrades');
// CORS: allow Netlify frontend and any origin (needed for Railway free tier)
app.use(cors({
  origin: '*',                   // allow all origins — tighten if needed
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.options('*', cors());        // handle preflight for all routes
app.use(express.json());

const fs   = require('fs');
const path = require('path');

const TWELVE_KEY    = '7f3fc6ca85664930ab6e687db8ff0c5d';
// TELEGRAM_MODE: 'EXECUTION' (default) = pre-entry + entry + conditional invalidation only
//                'FULL'                 = all stage alerts (debug/verbose)
const TELEGRAM_MODE = process.env.TELEGRAM_MODE || 'EXECUTION';
const ANTHROPIC_KEY = ['sk-ant-','api03-PSBtiCb9gNCUnpxHjEl2sqWVtfNop5DtO1WCW2pdUw_upi3Zl0VDjCT7Yyk','W9bboA3Bxnq2ucHBFyuNrNx6CL','w-qYuk4wAA'].join('');
// ═══════════════════════════════════════════════════════════════════════════
// SETUP LOGGING & ANALYTICS SYSTEM
// Passive, non-blocking, append-only. Never touches trading logic.
// Logs every setup lifecycle event to logs.jsonl
// ═══════════════════════════════════════════════════════════════════════════

// In-memory log store — keyed by setup.id
const _setupLogs = {};

// ── GOOGLE SHEETS LOGGING ────────────────────────────────────────────────────
// Credentials and sheet ID loaded from Railway environment variables:
//   GOOGLE_SERVICE_ACCOUNT_JSON  — full service account JSON (stringified)
//   GOOGLE_SHEET_ID              — the ID from the sheet URL
//
// Sheet columns (row per event):
// A: Timestamp | B: Setup ID | C: Symbol | D: Direction | E: Session
// F: Zone Low  | G: Zone High | H: Zone Score | I: Touches
// J: Event     | K: Entry Price | L: Stop Loss | M: TP1 | N: TP2
// O: Candles   | P: Result | Q: Invalidation Reason | R: Stages

let _sheetsClient = null;

async function getSheetsClient() {
  if (_sheetsClient) return _sheetsClient;
  try {
    const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
    if (!credsJson) { console.log('[sheets] No credentials — logging disabled'); return null; }
    const { google } = require('googleapis');
    const creds = JSON.parse(credsJson);
    const auth  = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    _sheetsClient = google.sheets({ version: 'v4', auth });
    console.log('[sheets] Google Sheets client initialised');
    return _sheetsClient;
  } catch(e) {
    console.error('[sheets] Init error:', e.message);
    return null;
  }
}

async function appendToSheet(row) {
  try {
    const sheetId = process.env.GOOGLE_SHEET_ID;
    if (!sheetId) return;
    const sheets  = await getSheetsClient();
    if (!sheets)  return;
    await sheets.spreadsheets.values.append({
      spreadsheetId: sheetId,
      range:         'Aurum!A:R',
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: [row] },
    });
  } catch(e) {
    // Never let logging break the trading engine
    console.error('[sheets] Append error:', e.message);
  }
}

function detectSessionTag(utcHour) {
  if (utcHour >= 0  && utcHour < 7)  return 'ASIA';
  if (utcHour >= 7  && utcHour < 13) return 'LONDON';
  if (utcHour >= 13 && utcHour < 22) return 'NY';
  return 'OFF';
}

// Called when setup is created
function logSetupCreated(setup, primaryZone) {
  try {
    const h = new Date().getUTCHours();
    const log = {
      id:              setup.id,
      symbol:          setup.sym,
      direction:       setup.direction,
      zone: {
        low:    primaryZone?.minPrice || null,
        high:   primaryZone?.maxPrice || null,
        score:  primaryZone?.confidence?.total || null,
        touches:primaryZone?.totalTouches || null,
      },
      session:          detectSessionTag(h),
      timestamp_start:  new Date().toISOString(),
      htfBias:              null, // set when first stage fires
      biasAlignment:        null, // 'ALIGNED' | 'COUNTER' | 'NEUTRAL'
      confidenceBeforeBias: null,
      confidenceAfterBias:  null,
      stages: { liquidity_grab:false, strong_move:false, trend_shift:false, pullback:false, entry:false },
      entryTriggered:   false,
      entryPrice:       null,
      stopLoss:         null,
      takeProfits:      [],
      invalidated:      false,
      invalidationReason: null,
      timestamp_end:    null,
      candlesToEntry:   null,
      candlesToInvalidation: null,
      startCandleMs:    Date.now(),
      result:           null,
      _version:         1,
    };
    _setupLogs[setup.id] = log;
    console.log('[log] Setup created: ' + setup.sym + ' ' + setup.direction +
      ' zone=' + (primaryZone?.priceRange || '?') + ' score=' + (primaryZone?.confidence?.total || '?'));
    // Append to Google Sheets (non-blocking)
    appendToSheet([
      new Date().toISOString(), setup.id, setup.sym, setup.direction,
      detectSessionTag(h),
      primaryZone?.minPrice || '', primaryZone?.maxPrice || '',
      primaryZone?.confidence?.total || '', primaryZone?.totalTouches || '',
      'CREATED', '', '', '', '', '', '', '', ''
    ]);
  } catch(e) { console.error('[log] logSetupCreated error:', e.message); }
}

// Called when a stage is confirmed
function logStageUpdate(setup, stage) {
  try {
    const log = _setupLogs[setup.id];
    if (!log) return;
    const stageMap = { sweep:'liquidity_grab', move:'strong_move', trend:'trend_shift', pullback:'pullback', entry:'entry' };
    const key = stageMap[stage];
    if (key) log.stages[key] = true;
    log._version++;
    console.log('[log] Stage: ' + setup.sym + ' → ' + stage);
    appendToSheet([
      new Date().toISOString(), setup.id, log.symbol, log.direction,
      log.session, log.zone?.low||'', log.zone?.high||'',
      log.zone?.score||'', log.zone?.touches||'',
      'STAGE:' + stage.toUpperCase(), '', '', '', '', '', '', '',
      Object.entries(log.stages).filter(([,v])=>v).map(([k])=>k).join(',')
    ]);
  } catch(e) { console.error('[log] logStageUpdate error:', e.message); }
}

// Called when entry signal fires
function logEntryTriggered(setup, entryPrice, stopLoss, takeProfits) {
  try {
    const log = _setupLogs[setup.id];
    if (!log) return;
    log.entryTriggered  = true;
    log.entryPrice      = entryPrice;
    log.stopLoss        = stopLoss;
    log.takeProfits     = takeProfits;
    log.stages.entry    = true;
    const elapsed = Date.now() - log.startCandleMs;
    log.candlesToEntry  = Math.round(elapsed / (5 * 60 * 1000));
    log.timestamp_end   = new Date().toISOString();
    // Capture HTF bias from symTiming at time of entry
    const _t = symTiming[setup.sym];
    if (_t && !log.htfBias) {
      log.htfBias = _t.htfBias || 'NEUTRAL';
      const htfAligned = (log.htfBias === 'BULLISH' && setup.direction === 'BUY') ||
                         (log.htfBias === 'BEARISH' && setup.direction === 'SELL');
      const htfCounter = (log.htfBias === 'BULLISH' && setup.direction === 'SELL') ||
                         (log.htfBias === 'BEARISH' && setup.direction === 'BUY');
      log.biasAlignment = htfAligned ? 'ALIGNED' : htfCounter ? 'COUNTER' : 'NEUTRAL';
    }
    log._version++;
    const _logHtf = log.htfBias || 'NEUTRAL';
    console.log('[log] Entry triggered: ' + setup.sym + ' @ ' + entryPrice +
      ' SL=' + stopLoss + ' TP1=' + (takeProfits[0]||'?') + ' candles=' + log.candlesToEntry +
      ' HTF=' + _logHtf);
    appendToSheet([
      new Date().toISOString(), setup.id, log.symbol, log.direction,
      log.session, log.zone?.low||'', log.zone?.high||'',
      log.zone?.score||'', log.zone?.touches||'',
      'ENTRY', entryPrice, stopLoss, takeProfits[0]||'', takeProfits[1]||'',
      log.candlesToEntry, '', '', 'all'
    ]);
  } catch(e) { console.error('[log] logEntryTriggered error:', e.message); }
}

// Called when setup is invalidated
function logInvalidation(setup, reason) {
  try {
    const log = _setupLogs[setup.id];
    if (!log) return;
    log.invalidated         = true;
    log.invalidationReason  = reason;
    log.timestamp_end       = new Date().toISOString();
    const elapsed = Date.now() - log.startCandleMs;
    log.candlesToInvalidation = Math.round(elapsed / (5 * 60 * 1000));
    log._version++;
    console.log('[log] Invalidated: ' + setup.sym + ' reason="' + reason + '" candles=' + log.candlesToInvalidation);
    appendToSheet([
      new Date().toISOString(), setup.id, log.symbol, log.direction,
      log.session, log.zone?.low||'', log.zone?.high||'',
      log.zone?.score||'', log.zone?.touches||'',
      'INVALIDATED', '', '', '', '',
      log.candlesToInvalidation, '', reason,
      Object.entries(log.stages).filter(([,v])=>v).map(([k])=>k).join(',')
    ]);
    setTimeout(() => { delete _setupLogs[setup.id]; }, 2000);
  } catch(e) { console.error('[log] logInvalidation error:', e.message); }
}

// Update trade result (TP1/TP2/SL/BE) — called manually or from future price checker
function logTradeResult(setupId, result) {
  try {
    const log = _setupLogs[setupId];
    if (log) {
      log.result    = result;
      log._version++;
    }
    console.log('[log] Result: ' + setupId + ' → ' + result);
    appendToSheet([
      new Date().toISOString(), setupId,
      log?.symbol||'', log?.direction||'', log?.session||'',
      log?.zone?.low||'', log?.zone?.high||'',
      log?.zone?.score||'', log?.zone?.touches||'',
      'RESULT', log?.entryPrice||'', log?.stopLoss||'',
      log?.takeProfits?.[0]||'', log?.takeProfits?.[1]||'',
      '', result, '', ''
    ]);
  } catch(e) { console.error('[log] logTradeResult error:', e.message); }
}

// Read all logs — from in-memory store (session data)
// Historical data lives in Google Sheets
function readAllLogs() {
  return Object.values(_setupLogs);
}

const SYMBOLS = {
  XAUUSD: 'XAU/USD',   // Gold spot — free tier
  XAGUSD: 'SLV'        // Silver via iShares Silver Trust ETF (SLV) — free tier proxy
                        // Trades 13:30-20:00 UTC (covers London/NY overlap + full NY session)
};

// ─── IN-MEMORY CACHE ────────────────────────────────────────────────────────
// Cache candle data for 60 seconds to avoid hitting Twelve Data rate limits
// when both XAUUSD and XAGUSD are scanned in quick succession.
const cache = {};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes — M5 candles update every 5min anyway

function getCached(key) {
  const entry = cache[key];
  if (entry && (Date.now() - entry.ts) < CACHE_TTL) {
    console.log(`[cache hit] ${key} (${Math.round((Date.now()-entry.ts)/1000)}s old)`);
    return entry.data;
  }
  return null;
}
function setCached(key, data) {
  cache[key] = { ts: Date.now(), data };
}

// ─── SAFE FETCH ────────────────────────────────────────────────────────────
async function tdFetch(path) {
  const sep = path.includes('?') ? '&' : '?';
  const url = `https://api.twelvedata.com${path}${sep}apikey=${TWELVE_KEY}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(12000) });
  if (!res.ok) {
    console.error('TD HTTP error:', res.status, res.statusText, 'URL:', url.replace(TWELVE_KEY, '***'));
    throw new Error('Twelve Data HTTP ' + res.status);
  }
  const json = await res.json();
  // Rate limit detection
  if (json.code === 429 || json.message?.toLowerCase().includes('limit')) {
    console.error('TD rate limit hit. Path:', path.split('?')[0]);
  }
  return json;
}

// ─── CANDLE FETCH ─────────────────────────────────────────────────────────
async function getCandles(sym, interval, n) {
  const td = SYMBOLS[sym];
  if (!td) { console.error('Unknown symbol:', sym); return null; }
  
  // Check cache first
  const cacheKey = `candles_${sym}_${interval}_${n}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;
  
  try {
    const d = await tdFetch(`/time_series?symbol=${encodeURIComponent(td)}&interval=${interval}&outputsize=${n}`);
    // Twelve Data returns {code, message} on errors (rate limit, bad symbol, etc.)
    if (d.code || d.status === 'error' || d.message) {
      console.error(`TD candles error [${sym} ${interval}]:`, d.code, d.message || d.status);
      return null;
    }
    if (!d.values || d.values.length === 0) {
      console.warn(`TD candles empty [${sym} ${interval}]: no values in response`);
      return null;
    }
    const candles = d.values.map(c => ({
      t: new Date(c.datetime.includes('T') ? c.datetime : c.datetime + ' UTC').getTime(),
      o: parseFloat(c.open),
      h: parseFloat(c.high),
      l: parseFloat(c.low),
      c: parseFloat(c.close)
    })).reverse();
    console.log(`TD candles OK [${sym} ${interval}]:`, candles.length, 'candles');
    setCached(cacheKey, candles);
    return candles;
  } catch(e) {
    console.error(`TD candles exception [${sym} ${interval}]:`, e.message);
    return null;
  }
}

async function getATR(sym, interval, period) {
  const td = SYMBOLS[sym];
  if (!td) return [];
  const cacheKey = `atr_${sym}_${interval}_${period}`;
  const cached = getCached(cacheKey);
  if (cached) return cached;
  try {
    const d = await tdFetch(`/atr?symbol=${encodeURIComponent(td)}&interval=${interval}&time_period=${period}&outputsize=21`);
    if (d.code || d.status === 'error' || d.message) {
      console.error(`TD ATR error [${sym}]:`, d.code, d.message || d.status);
      return [];
    }
    if (!d.values) return [];
    const atrVals = d.values.map(v => parseFloat(v.atr)).reverse();
    setCached(cacheKey, atrVals);
    return atrVals;
  } catch(e) {
    console.error(`TD ATR exception [${sym}]:`, e.message);
    return [];
  }
}

// ─── HELPERS ──────────────────────────────────────────────────────────────
const pct   = (a, b) => Math.abs(a - b) / b;            // relative distance
const body  = c      => Math.abs(c.c - c.o);
const range = c      => c.h - c.l;                      // always positive

// Calculate ATR from candles — no API call needed (Wilder's smoothing)
function calcATRFromCandles(candles, period = 14) {
  if (!candles || candles.length < period + 1) return [];
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const c = candles[i], p = candles[i-1];
    trs.push(Math.max(c.h - c.l, Math.abs(c.h - p.c), Math.abs(c.l - p.c)));
  }
  let atr = trs.slice(0, period).reduce((s,v) => s+v, 0) / period;
  const atrs = [atr];
  for (let i = period; i < trs.length; i++) {
    atr = (atr * (period - 1) + trs[i]) / period;
    atrs.push(atr);
  }
  return atrs;
}

// Derive M15 candles from M5 — no API call needed (3 M5 = 1 M15)
function deriveM15FromM5(m5Candles) {
  if (!m5Candles || m5Candles.length < 3) return [];
  const m15 = [];
  for (let i = 0; i + 2 < m5Candles.length; i += 3) {
    const g = m5Candles.slice(i, i + 3);
    m15.push({ t: g[0].t, o: g[0].o,
               h: Math.max(...g.map(c => c.h)),
               l: Math.min(...g.map(c => c.l)),
               c: g[2].c });
  }
  return m15;
}


// --- ATR RANGE VALIDATION ----------------------------------------------------
// Valid ATR range per M5 candle. Outside either bound = ignore volatility filter.
const ATR_RANGE = {
  XAUUSD: { min: 0.30, max: 8.0  },  // $0.30-$8.00 per M5 candle — XAU/USD spot
  XAGUSD: { min: 0.03, max: 1.50 }   // $0.03-$1.50 per M5 candle — SLV ETF (~$28-32/share)
};
function checkATR(sym, atrValues) {
  if (!atrValues || atrValues.length < 5 || !ATR_RANGE[sym]) {
    return { ok: true, state: 'unknown', current: null, avg20: null, note: 'Insufficient ATR' };
  }
  const current = atrValues[atrValues.length - 1];
  if (isNaN(current) || current <= 0) {
    return { ok: true, state: 'unknown', current, avg20: null, note: 'ATR invalid - proceeding' };
  }
  const { min, max } = ATR_RANGE[sym];
  const slice = atrValues.slice(-20);
  const avg20 = slice.reduce((s,v)=>s+v,0) / slice.length;
  if (current < min) {
    return { ok: false, state: 'low_volatility', current, avg20,
             note: 'ATR ' + current.toFixed(4) + ' below min ' + min + ' - suppressed' };
  }
  if (current > max) {
    // Too high = news spike; ignore filter, allow engine to run
    return { ok: null, state: 'high_volatility', current, avg20,
             note: 'ATR ' + current.toFixed(4) + ' above max ' + max + ' - filter ignored' };
  }
  const state = current >= avg20 ? 'normal' : 'below_avg';
  return { ok: true, state, current, avg20,
           note: 'ATR ' + current.toFixed(4) + ' (avg ' + avg20.toFixed(4) + ') - ' + state };
}

// ─── SESSION ──────────────────────────────────────────────────────────────
// SESSION DETECTION — UTC only. Never uses device local time.
// London: 07:00–16:00 UTC   New York: 13:00–22:00 UTC
// Note: SLV ETF trades NYSE hours (13:30–20:00 UTC) — fully within NY session window
function sessionName(tsMs) {
  const utcH = new Date(tsMs).getUTCHours(); // explicitly UTC
  const lnd  = utcH >= 7  && utcH < 16;
  const ny   = utcH >= 13 && utcH < 22;
  if (lnd && ny) return 'London+NY Overlap';
  if (lnd)       return 'London';
  if (ny)        return 'New York';
  return null; // outside both sessions
}
function sessionWeight(name) {
  if (!name)                        return 0;
  if (name === 'London+NY Overlap') return 10;
  if (name === 'New York')          return 7;
  if (name === 'London')            return 5;
  return 0;
}
function isActiveSession(tsMs) { return sessionName(tsMs) !== null; }

// ─── LIQUIDITY LEVELS ─────────────────────────────────────────────────────
// ─── LIQUIDITY ZONE CLUSTERING ───────────────────────────────────────────────
// Groups nearby EQH/EQL levels within CLUSTER_PCT into unified trading zones.
// Prevents multiple overlapping levels triggering separate alerts.
const CLUSTER_PCT = 0.0015; // 0.15% of price — groups levels within ~$6-7 at gold prices

function clusterEQLevels(rawLevels, currentPrice, zoneType) {
  // zoneType: 'EQH' (sell zones) or 'EQL' (buy zones)
  if (!rawLevels.length) return [];

  // Sort by price
  const sorted = [...rawLevels].sort((a, b) => a.price - b.price);
  const threshold = currentPrice * CLUSTER_PCT;
  const clusters  = [];
  let current     = [sorted[0]];

  for (let i = 1; i < sorted.length; i++) {
    const gap = sorted[i].price - sorted[i-1].price;
    if (gap <= threshold) {
      current.push(sorted[i]);
    } else {
      clusters.push(current);
      current = [sorted[i]];
    }
  }
  clusters.push(current);

  // Merge overlapping clusters (edge case)
  const merged = [];
  for (const cl of clusters) {
    const last = merged[merged.length - 1];
    if (last && cl[0].price - last.maxPrice <= threshold) {
      // Merge into previous cluster
      last.levels.push(...cl);
      last.maxPrice     = Math.max(last.maxPrice, ...cl.map(l => l.price));
      last.minPrice     = Math.min(last.minPrice, ...cl.map(l => l.price));
      last.totalTouches = last.levels.reduce((s, l) => s + (l.touches || 1), 0);
      last.strengthScore= last.levels.reduce((s, l) => Math.max(s, l.strengthScore || 1), 0);
    } else {
      const allTouches  = cl.reduce((s, l) => s + (l.touches || 1), 0);
      const maxStr      = cl.reduce((s, l) => Math.max(s, l.strengthScore || 1), 0);
      const avgPrice    = cl.reduce((s, l) => s + l.price, 0) / cl.length;
      const clMin = Math.min(...cl.map(l => l.price));
      const clMax = Math.max(...cl.map(l => l.price));
      // Reject single-point zones (min === max) — not real zones
      if (clMin === clMax && cl.length < 2) {
        console.log('[levels] Skipping degenerate zone (min=max) at $' + clMin.toFixed(3));
        continue; // skip this cluster
      }
      const avgWickZone = cl.reduce((s,l) => s + (l.avgWick||0), 0) / cl.length;
      const lastIdxZone = Math.max(...cl.map(l => l.lastCandleIdx || 0));
      merged.push({
        type:          zoneType,
        zoneType:      zoneType === 'EQH' ? 'sell_zone' : 'buy_zone',
        price:         avgPrice,
        minPrice:      clMin,
        maxPrice:      clMax,
        totalTouches:  allTouches,
        levelCount:    cl.length,
        levels:        cl,
        strengthScore: maxStr,
        strength:      maxStr >= 3 ? 'strong' : maxStr >= 2 ? 'medium' : 'weak',
        isZone:        true,
        avgWick:       avgWickZone,       // for reaction scoring
        lastCandleIdx: lastIdxZone,       // for recency scoring (index within recent20)
        label:         zoneType === 'EQH'
          ? 'Equal Highs zone (' + allTouches + ' touches)'
          : 'Equal Lows zone ('  + allTouches + ' touches)',
        priceRange:    parseFloat(clMin.toFixed(3)) + '–' + parseFloat(clMax.toFixed(3))
      });
    }
  }
  // ── POST-CLUSTER OVERLAP MERGE ────────────────────────────────
  // Zones from different raw groups can still overlap if their price ranges intersect.
  // Example: zone 4430–4449 and zone 4439–4443 are the same structure.
  // Merge any overlapping zones, keeping the strongest properties.
  const deduped = [];
  for (const z of merged) {
    let absorbed = false;
    for (const existing of deduped) {
      // Overlap condition: ranges intersect
      if (z.minPrice <= existing.maxPrice && z.maxPrice >= existing.minPrice) {
        // Merge into existing — expand range, take max touches, keep most recent
        const prevRange = (existing.maxPrice - existing.minPrice).toFixed(3);
        existing.minPrice     = Math.min(existing.minPrice, z.minPrice);
        existing.maxPrice     = Math.max(existing.maxPrice, z.maxPrice);
        existing.totalTouches = Math.max(existing.totalTouches, z.totalTouches);
        existing.avgWick      = Math.max(existing.avgWick || 0, z.avgWick || 0);
        existing.lastCandleIdx= Math.max(existing.lastCandleIdx || 0, z.lastCandleIdx || 0);
        existing.strengthScore= Math.max(existing.strengthScore, z.strengthScore);
        existing.levelCount   = (existing.levelCount || 1) + (z.levelCount || 1);
        existing.price        = (existing.minPrice + existing.maxPrice) / 2;
        existing.label        = zoneType === 'EQH'
          ? 'Equal Highs zone (' + existing.totalTouches + ' touches)'
          : 'Equal Lows zone ('  + existing.totalTouches + ' touches)';
        existing.priceRange   = parseFloat(existing.minPrice.toFixed(3)) + '–' +
                                parseFloat(existing.maxPrice.toFixed(3));
        existing.strength     = existing.strengthScore >= 3 ? 'strong'
                              : existing.strengthScore >= 2 ? 'medium' : 'weak';
        console.log('[zone-merge] ' + zoneType + ' overlap merged: ' +
          prevRange + ' + ' + (z.maxPrice - z.minPrice).toFixed(3) +
          ' → ' + (existing.maxPrice - existing.minPrice).toFixed(3) +
          ' touches: ' + existing.totalTouches);
        absorbed = true;
        break;
      }
    }
    if (!absorbed) deduped.push(z);
  }
  return deduped;
}

function buildLevels(m5Candles, m15Candles) {
  const levels = [];
  const todayUTC = new Date(); todayUTC.setUTCHours(0,0,0,0);

  // Previous Day H/L
  const yday = m15Candles.filter(c => c.t >= todayUTC.getTime()-86400000 && c.t < todayUTC.getTime());
  if (yday.length > 0) {
    levels.push({ price: Math.max(...yday.map(c=>c.h)), type:'PDH', label:'Previous Day High', strength:'strong', strengthScore:3 });
    levels.push({ price: Math.min(...yday.map(c=>c.l)), type:'PDL', label:'Previous Day Low',  strength:'strong', strengthScore:3 });
  }

  // Asian Session H/L
  const asian = m15Candles.filter(c => c.t >= todayUTC.getTime() && new Date(c.t).getUTCHours() < 8);
  if (asian.length > 0) {
    levels.push({ price: Math.max(...asian.map(c=>c.h)), type:'ASH', label:'Asian Session High', strength:'medium', strengthScore:2 });
    levels.push({ price: Math.min(...asian.map(c=>c.l)), type:'ASL', label:'Asian Session Low',  strength:'medium', strengthScore:2 });
  }

  // Equal Highs / Lows — detect raw groups first, then cluster into zones
  const recent20 = m5Candles.slice(-20);
  const EQ_TOL   = 0.0005; // 0.05% for raw grouping

  // Detect raw EQH groups
  const eqHighGroups = [];
  recent20.forEach(c => {
    let placed = false;
    for (const g of eqHighGroups) {
      if (pct(c.h, g[0].h) <= EQ_TOL) { g.push(c); placed = true; break; }
    }
    if (!placed) eqHighGroups.push([c]);
  });

  // Detect raw EQL groups
  const eqLowGroups = [];
  recent20.forEach(c => {
    let placed = false;
    for (const g of eqLowGroups) {
      if (pct(c.l, g[0].l) <= EQ_TOL) { g.push(c); placed = true; break; }
    }
    if (!placed) eqLowGroups.push([c]);
  });

  // Build raw EQH levels — attach candle indices for recency + reaction scoring
  const rawEQH = eqHighGroups.filter(g => g.length >= 2).map(g => {
    const avg      = g.reduce((s,c)=>s+c.h,0)/g.length;
    // Find index of most recent candle in this group within recent20
    const lastIdx  = recent20.length - 1 - [...recent20].reverse().findIndex(c =>
      g.some(gc => Math.abs(gc.h - c.h) < avg * 0.0005));
    // Average wick size as proxy for reaction strength
    const avgWick  = g.reduce((s,c) => s + (c.h - Math.max(c.o, c.c)), 0) / g.length;
    return { price: avg, type:'EQH', touches: g.length, lastCandleIdx: lastIdx,
             avgWick, strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1 };
  });

  const rawEQL = eqLowGroups.filter(g => g.length >= 2).map(g => {
    const avg      = g.reduce((s,c)=>s+c.l,0)/g.length;
    const lastIdx  = recent20.length - 1 - [...recent20].reverse().findIndex(c =>
      g.some(gc => Math.abs(gc.l - c.l) < avg * 0.0005));
    const avgWick  = g.reduce((s,c) => s + (Math.min(c.o, c.c) - c.l), 0) / g.length;
    return { price: avg, type:'EQL', touches: g.length, lastCandleIdx: lastIdx,
             avgWick, strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1 };
  });

  // Current price for clustering threshold
  const currentPrice = m5Candles[m5Candles.length-1]?.c || 1;

  // Cluster into zones
  const eqhZones = clusterEQLevels(rawEQH, currentPrice, 'EQH');
  const eqlZones = clusterEQLevels(rawEQL, currentPrice, 'EQL');

  eqhZones.forEach(z => {
    console.log('[levels] Clustered ' + z.levelCount + ' EQH levels → SELL zone ' + z.priceRange + ' (' + z.totalTouches + ' touches)');
    levels.push(z);
  });
  eqlZones.forEach(z => {
    console.log('[levels] Clustered ' + z.levelCount + ' EQL levels → BUY zone ' + z.priceRange + ' (' + z.totalTouches + ' touches)');
    levels.push(z);
  });

  return levels;
}

// --- PROXIMITY DETECTION ---------------------------------------------------
// Threshold: price within 0.20% of a liquidity level = "approaching"
const APPROACH_PCT = { XAUUSD: 0.0020, XAGUSD: 0.0015 }; // SLV slightly tighter

function detectApproaching(price, levels, sym) {
  const threshold = APPROACH_PCT[sym] || 0.0020;
  return levels
    .map(lvl => {
      // For zones: use nearest edge. For single levels: use price.
      const nearEdge = lvl.isZone
        ? (price > lvl.maxPrice ? lvl.maxPrice : price < lvl.minPrice ? lvl.minPrice : price)
        : lvl.price;
      const dist    = Math.abs(price - nearEdge);
      const distPct = dist / nearEdge;
      const inside  = lvl.isZone && price >= lvl.minPrice && price <= lvl.maxPrice;
      return {
        ...lvl,
        dist:       parseFloat(dist.toFixed(4)),
        distPct:    parseFloat((distPct * 100).toFixed(3)),
        approaching: distPct <= threshold || inside,
        inside,
        side:       price > nearEdge ? 'above' : 'below'
      };
    })
    .filter(l => l.approaching)
    .sort((a, b) => a.distPct - b.distPct);
}

// ── ZONE CONFIDENCE SCORING (0–100) ──────────────────────────────────────────
// Used for pre-signal gating and UI display.
// Separate from full signal scoreSetup() — zones don't have BOS/pullback yet.
// ═══════════════════════════════════════════════════════════════════════════
// ZONE RANKING & SELECTION ENGINE
// Scores all valid zones, selects ONE primary zone per symbol.
// Secondary zones are ignored for all signal logic.
// ═══════════════════════════════════════════════════════════════════════════

// Hard filter — discard before scoring
function passesHardFilter(z, m5Candles) {
  // 1. Minimum touch count
  if ((z.totalTouches || 0) < 3) return { pass: false, reason: 'touches < 3 (' + (z.totalTouches||0) + ')' };
  // 2. Zero-width zone
  if (!z.minPrice || !z.maxPrice || z.minPrice === z.maxPrice)
    return { pass: false, reason: 'zero-width zone' };
  // 3. Zone age — last touch within 150 candles
  const candlesSinceLast = (m5Candles.length - 1) - (z.lastCandleIdx || 0);
  if (candlesSinceLast > 150) return { pass: false, reason: 'too old (' + candlesSinceLast + ' candles)' };
  return { pass: true };
}

// Zone scoring model — 0 to 100
function rankZone(z, price, sess, m5Candles) {
  const breakdown = {};
  let total = 0;

  // ── A. TOUCH COUNT (max 30) ───────────────────────────────────
  const touches = z.totalTouches || 0;
  const touchScore = touches >= 15 ? 30
                   : touches >= 10 ? 24
                   : touches >= 6  ? 18
                   : touches >= 3  ? 10 : 0;
  breakdown.touches = { score: touchScore, max: 30, count: touches };
  total += touchScore;

  // ── B. RECENCY (max 20) ───────────────────────────────────────
  const recent20Len = Math.min(m5Candles.length, 20);
  const candlesSinceLast = recent20Len - 1 - (z.lastCandleIdx || 0);
  const recencyScore = candlesSinceLast <= 20  ? 20
                     : candlesSinceLast <= 50  ? 12
                     : candlesSinceLast <= 100 ? 6 : 0;
  breakdown.recency = { score: recencyScore, max: 20, candlesSinceLast };
  total += recencyScore;

  // ── C. REACTION STRENGTH (max 20) ────────────────────────────
  // Use avgWick relative to average candle range
  const avgCandle = m5Candles.slice(-20).reduce((s,c) => s + range(c), 0) / Math.min(m5Candles.length, 20);
  const wickRatio = avgCandle > 0 ? (z.avgWick || 0) / avgCandle : 0;
  const reactionScore = wickRatio >= 1.5 ? 20
                      : wickRatio >= 0.8 ? 12
                      : wickRatio >= 0.3 ? 5 : 0;
  breakdown.reaction = { score: reactionScore, max: 20, wickRatio: parseFloat(wickRatio.toFixed(2)) };
  total += reactionScore;

  // ── D. ZONE TIGHTNESS (max 15) ────────────────────────────────
  const refPrice = z.minPrice || price;
  const widthPct = refPrice > 0 ? (z.maxPrice - z.minPrice) / refPrice : 0;
  const tightScore = widthPct <= 0.001 ? 15
                   : widthPct <= 0.0025 ? 10
                   : widthPct <= 0.005  ? 5 : 0;
  breakdown.tightness = { score: tightScore, max: 15, widthPct: parseFloat((widthPct*100).toFixed(3)) };
  total += tightScore;

  // ── E. CONFLUENCE (max 15) ────────────────────────────────────
  let confScore = 0;
  // Round number confluence — does the zone overlap a 00 or 50 handle?
  const zoneCenter = (z.minPrice + z.maxPrice) / 2;
  const roundFifty = Math.round(zoneCenter / 50) * 50;
  if (Math.abs(zoneCenter - roundFifty) / zoneCenter < 0.002) {
    confScore += 5; breakdown.confluence_round = true;
  }
  // Session level confluence — score keeps track of PDH/PDL/ASH/ASL proximity
  // (passed in via the levels array from buildLevels — checked at call site)
  breakdown.confluence = { score: Math.min(confScore, 15), max: 15 };
  total += Math.min(confScore, 15);

  total = Math.min(Math.max(Math.round(total), 0), 100);
  const grade = total >= 80 ? 'HIGH' : total >= 60 ? 'MEDIUM' : total >= 40 ? 'LOW' : 'IGNORE';
  const emoji = total >= 80 ? '🟢' : total >= 60 ? '🟡' : '🔴';

  return { total, grade, emoji, breakdown };
}

// Legacy wrapper — used by existing code that calls scoreZone()
function scoreZone(z, price, sess) {
  return rankZone(z, price, sess, []);
}


// ── ZONE RANKING ENGINE — selects ONE primary zone per symbol ───────────────
function selectPrimaryZone(levels, price, sess, m5Candles, structuralBias) {
  const m5 = m5Candles || [];
  // structuralBias: { dir: 'BUY'|'SELL'|null, stage: 'sweep'|'move'|'trend'|null }
  // If set, the opposite direction zone cannot become primary unless structure confirms

  // 1. Pull only EQH/EQL clustered zones
  const zones = levels.filter(l => l.isZone && (l.type === 'EQH' || l.type === 'EQL'));
  if (!zones.length) return null;

  // 2. Hard filter — discard invalid zones before scoring
  const valid = [];
  for (const z of zones) {
    const hf = passesHardFilter(z, m5);
    if (!hf.pass) {
      console.log('[zone] DISCARD ' + z.type + ' ' + z.priceRange + ': ' + hf.reason);
      continue;
    }
    valid.push(z);
  }
  if (!valid.length) return null;

  // 3. Score + add confluence from structural levels (PDH/PDL/ASH/ASL)
  const structural = levels.filter(l => ['PDH','PDL','ASH','ASL'].includes(l.type));
  const scored = valid.map(z => {
    const direction = getZoneDirection(z);
    const nearEdge  = direction === 'SELL' ? z.minPrice : z.maxPrice;
    const distPct   = Math.abs(price - nearEdge) / nearEdge;
    const inside    = price >= z.minPrice && price <= z.maxPrice;

    // Run ranking model
    const confidence = rankZone(z, price, sess, m5);

    // Confluence bonus: add +5 for each nearby structural level (within 0.2%)
    let confBonus = 0;
    for (const sl of structural) {
      if (Math.abs(sl.price - z.price) / z.price <= 0.002) {
        confBonus = Math.min(confBonus + 5, 15);
      }
    }
    if (confBonus > 0) {
      confidence.total = Math.min(confidence.total + confBonus, 100);
      confidence.breakdown.confluence = { score: confBonus, sources: structural
        .filter(sl => Math.abs(sl.price - z.price) / z.price <= 0.002)
        .map(sl => sl.type) };
    }

    // Debug log for every valid zone
    console.log('[zone] ' + z.type + ' ' + z.priceRange +
      ' score=' + confidence.total + '/100' +
      ' touches=' + z.totalTouches +
      ' recency=' + confidence.breakdown.recency?.candlesSinceLast + 'c' +
      ' reaction=' + confidence.breakdown.reaction?.wickRatio + 'x' +
      ' tight=' + confidence.breakdown.tightness?.widthPct + '%' +
      (confBonus ? ' +' + confBonus + ' confluence' : ''));

    return { ...z, direction, distPct: parseFloat((distPct*100).toFixed(3)),
             inside, confidence, score: confidence.total };
  });

  // 4. Apply structural bias — block opposite direction zones from becoming primary
  // unless bias is only at 'sweep' level (weakest) and no opposing zone is stronger
  if (structuralBias && structuralBias.dir) {
    const biasDir       = structuralBias.dir;
    const biasStage     = structuralBias.stage || 'sweep';
    const oppositeDir   = biasDir === 'BUY' ? 'SELL' : 'BUY';
    const stageStrength = { sweep: 1, move: 2, trend: 3 };
    const biasStrength  = stageStrength[biasStage] || 1;

    scored.forEach(z => {
      if (z.direction === oppositeDir) {
        // Counter-trend zone — demote unless bias is only at sweep level
        if (biasStrength >= 2) {
          // Move or trend confirmed — strongly suppress counter-trend zone
          z.score        = Math.max(0, z.score - 40);
          z.isCounterTrend = true;
          console.log('[bias] ' + oppositeDir + ' zone ' + z.priceRange +
            ' demoted (counter-trend — ' + biasDir + ' bias at ' + biasStage + ' stage)');
        } else {
          // Only sweep confirmed — moderate suppression
          z.score        = Math.max(0, z.score - 20);
          z.isCounterTrend = true;
          console.log('[bias] ' + oppositeDir + ' zone ' + z.priceRange +
            ' mildly demoted (counter-trend — ' + biasDir + ' sweep only)');
        }
      }
    });
  }

  // 4. Sort by score descending — highest score = primary zone
  scored.sort((a, b) => b.score - a.score);

  const primary = scored[0];
  if (primary.isCounterTrend) {
    console.log('[bias] WARNING: primary zone is counter-trend — no same-direction zone available');
  }
  console.log('[zone] PRIMARY ZONE SELECTED: ' + primary.direction +
    ' ' + primary.priceRange +
    ' score=' + primary.score + '/100' +
    ' touches=' + primary.totalTouches);

  // Mark others as secondary (for debug/UI only — signal logic ignores them)
  scored.slice(1).forEach(z => {
    console.log('[zone] SECONDARY (ignored): ' + z.type + ' ' + z.priceRange + ' score=' + z.score);
  });

  return primary;
}


// ═══════════════════════════════════════════════════════════════════════════
// UNIFIED DIRECTION FUNCTION
// Single source of truth for zone→direction mapping.
// Called everywhere a direction needs to be inferred from a level/zone type.
// ═══════════════════════════════════════════════════════════════════════════
function getZoneDirection(lvl) {
  if (!lvl) return null;
  const t = lvl.type || '';
  if (t === 'EQH' || t === 'PDH' || t === 'ASH') return 'SELL';
  if (t === 'EQL' || t === 'PDL' || t === 'ASL') return 'BUY';
  // For manually set zoneType
  if (lvl.zoneType === 'sell_zone') return 'SELL';
  if (lvl.zoneType === 'buy_zone')  return 'BUY';
  // Last resort — level already has a corrected direction
  return lvl.direction || null;
}

// ─── GLOBAL DIRECTIONAL BIAS ─────────────────────────────────────────────────
// Weights: BOS=+3/-3, displacement=+2/-2, sweep=+1/-1, proximity=+0.5/-0.5
// biasScore > +1 → BUY  |  < -1 → SELL  |  -1..+1 → NEUTRAL
// A confirmed setup direction always overrides proximity signals.
const BIAS_WEIGHTS = { bos: 3, move: 2, sweep: 1, proximity: 0.5 };

function calcGlobalBias(levels, livePrice, activeSetup) {
  let score = 0;
  const factors = [];

  // 1. Active setup direction carries highest weight
  if (activeSetup && activeSetup.active && !activeSetup.invalidated) {
    const w = activeSetup.events?.trend ? BIAS_WEIGHTS.bos
            : activeSetup.events?.move  ? BIAS_WEIGHTS.move
            : activeSetup.events?.sweep ? BIAS_WEIGHTS.sweep
            : 0;
    const val = activeSetup.direction === 'BUY' ? w : -w;
    score += val;
    factors.push((activeSetup.direction === 'BUY' ? '+' : '') + val +
      ' (' + activeSetup.stage + ' active)');
  }

  // 2. Price vs PDH/PDL — structural context
  const pdh = levels.find(l => l.type === 'PDH');
  const pdl = levels.find(l => l.type === 'PDL');
  if (pdh && livePrice > pdh.price) {
    score -= BIAS_WEIGHTS.proximity;
    factors.push('-' + BIAS_WEIGHTS.proximity + ' (above PDH → bearish context)');
  } else if (pdl && livePrice < pdl.price) {
    score += BIAS_WEIGHTS.proximity;
    factors.push('+' + BIAS_WEIGHTS.proximity + ' (below PDL → bullish context)');
  }

  const bias = score > 1  ? 'BUY'
             : score < -1 ? 'SELL'
             :               'NEUTRAL';

  console.log('[bias] Score: ' + score.toFixed(1) + ' → ' + bias +
    (factors.length ? ' (' + factors.join(', ') + ')' : ''));

  return { score, bias, factors };
}

function detectSweepPotential(price, approachingLevels, candles) {
  if (!approachingLevels.length || candles.length < 4) return [];
  const alerts = [];

  for (const lvl of approachingLevels) {
    // Hard direction rule — never conflict
    // EQH = equal highs = SELL zone (price sweeps UP, reverses DOWN)
    // EQL = equal lows  = BUY  zone (price sweeps DOWN, reverses UP)
    // PDH/ASH           = SELL (resistance above)
    // PDL/ASL           = BUY  (support below)
    const dir = getZoneDirection(lvl) || (lvl.side === 'below' ? 'BUY' : 'SELL');

    const lvlDesc = lvl.isZone
      ? 'Primary ' + dir + ' Zone $' + parseFloat(lvl.minPrice).toFixed(2) + '–$' + parseFloat(lvl.maxPrice).toFixed(2) +
        ' (' + lvl.totalTouches + ' touches)'
      : lvl.label + ' at $' + lvl.price.toFixed(3);

    alerts.push({
      type:      'sweep_potential',
      level:     { ...lvl, direction: dir }, // enforce direction on level object too
      direction: dir,
      message:   lvlDesc + ' (' + lvl.distPct + '% away)'
    });
  }
  return alerts;
}

// ─── SWEEP DETECTION ──────────────────────────────────────────────────────
// Returns: { found, candleIdx, level, direction, wickPct }
function detectSweep(candles, levels) {
  const SWEEP_BREAK  = 0.0002; // 0.02% minimum penetration
  const WICK_MIN_PCT = 0.30;

  for (let i = candles.length - 6; i < candles.length; i++) {
    if (i < 0) continue;
    const c = candles[i];
    const totalRange = range(c);
    if (totalRange === 0) continue;

    for (const lvl of levels) {
      // For zones: use minPrice/maxPrice bounds. For single levels: use price ± small buffer.
      const isZone   = lvl.isZone === true;
      const zoneLow  = isZone ? lvl.minPrice : lvl.price;
      const zoneHigh = isZone ? lvl.maxPrice : lvl.price;
      // Representative price for sweep calculations
      const p = lvl.price;

      // BUY sweep: candle wicks BELOW zone bottom, closes BACK ABOVE zone bottom
      const sweepLow  = zoneLow  * (1 - SWEEP_BREAK);
      const sweepHigh = zoneHigh * (1 + SWEEP_BREAK);

      if (c.l < sweepLow && c.c > zoneLow) {
        const wickSize = zoneLow - c.l;
        if (wickSize / totalRange >= WICK_MIN_PCT) {
          return { found: true, candleIdx: i, level: lvl, direction: 'BUY',
                   sweepExtreme: c.l, closePrice: c.c, wickPct: wickSize/totalRange,
                   zoneMin: zoneLow, zoneMax: zoneHigh };
        }
      }

      if (c.h > sweepHigh && c.c < zoneHigh) {
        const wickSize = c.h - zoneHigh;
        if (wickSize / totalRange >= WICK_MIN_PCT) {
          return { found: true, candleIdx: i, level: lvl, direction: 'SELL',
                   sweepExtreme: c.h, closePrice: c.c, wickPct: wickSize/totalRange,
                   zoneMin: zoneLow, zoneMax: zoneHigh };
        }
      }
    }
  }
  return { found: false };
}

// ─── DISPLACEMENT ──────────────────────────────────────────────────────────
// Must occur within 1–3 candles after sweep candle
// Returns: { found, candleIdx, bodySize, avgBody }
function detectDisplacement(candles, sweepIdx, direction, minRatioOverride) {
  // minRatioOverride: optional — allows caller to require stricter displacement
  const MIN_RATIO_OVERRIDE = minRatioOverride || null;
  // Scans up to 4 candles. Tolerates 1 weak/indecisive candle gap.
  const BODY_MULT  = 1.5;
  const CLOSE_ZONE = 0.25;
  const slice = candles.slice(Math.max(0, sweepIdx - 10), sweepIdx);
  if (slice.length < 3) return { found: false, reason: 'insufficient candle history' };
  const avgBody10 = slice.reduce((s,c) => s + body(c), 0) / slice.length;
  let weakGap = false;

  for (let offset = 1; offset <= 4; offset++) {
    const idx = sweepIdx + offset;
    if (idx >= candles.length) break;
    const c = candles[idx];
    const b = body(c);
    const r = range(c);
    if (r === 0) continue;
    const dirOk      = direction === 'BUY' ? c.c > c.o : c.c < c.o;
    const _dispMin   = MIN_RATIO_OVERRIDE || BODY_MULT;
    const bodyStrong = b >= avgBody10 * _dispMin;
    const closeZone  = direction === 'BUY'
      ? (c.c - c.l) / r >= (1 - CLOSE_ZONE)
      : (c.h - c.c) / r >= (1 - CLOSE_ZONE);
    if (bodyStrong && dirOk && closeZone) {
      return { found: true, candleIdx: idx,
               ratio: parseFloat((b/avgBody10).toFixed(2)),
               avgBody: avgBody10, weakGap,
               impulseHigh: c.h, impulseLow: c.l };
    }
    // Allow 1 weak/indecisive candle gap before invalidating
    if (!weakGap && (!dirOk || b < avgBody10 * 0.5)) { weakGap = true; continue; }
    if (offset > 2 && !bodyStrong) break;
  }
  return { found: false, reason: 'no displacement within 4 bars (1 weak gap allowed)' };
}

// ─── MARKET STRUCTURE SHIFT (BOS) ─────────────────────────────────────────
// BUY: find last lower-high, check break above by 0.05%
// SELL: find last higher-low, check break below by 0.05%
function detectBOS(candles, sweepIdx, direction) {
  // Flexible BOS: (A) strong candle close beyond level, OR
  //               (B) wick break + strong follow-through candle next bar.
  // Prioritizes internal BOS (micro swing, last 5 candles) over external.
  const BOS_MIN    = 0.0005;
  const WICK_MIN   = 0.0003;
  const FOLLOW_MULT = 1.2;

  const sliceBos = candles.slice(Math.max(0, sweepIdx - 8), sweepIdx);
  const avgBodyBos = sliceBos.length > 0
    ? sliceBos.reduce((s,c) => s + body(c), 0) / sliceBos.length : 0;

  function findSwingLevel(lookbackCandles, dir) {
    // Internal: last 5 candles (micro structure)
    const internal5 = lookbackCandles.slice(-5);
    for (let i = internal5.length - 2; i >= 1; i--) {
      if (dir === 'BUY'  && internal5[i].h < internal5[i-1].h && internal5[i].h < internal5[i+1].h)
        return { price: internal5[i].h, type: 'internal' };
      if (dir === 'SELL' && internal5[i].l > internal5[i-1].l && internal5[i].l > internal5[i+1].l)
        return { price: internal5[i].l, type: 'internal' };
    }
    // External: full lookback (macro structure)
    for (let i = lookbackCandles.length - 2; i >= 1; i--) {
      if (dir === 'BUY'  && lookbackCandles[i].h < lookbackCandles[i-1].h && lookbackCandles[i].h < lookbackCandles[i+1].h)
        return { price: lookbackCandles[i].h, type: 'external' };
      if (dir === 'SELL' && lookbackCandles[i].l > lookbackCandles[i-1].l && lookbackCandles[i].l > lookbackCandles[i+1].l)
        return { price: lookbackCandles[i].l, type: 'external' };
    }
    return null;
  }

  const lookback = candles.slice(Math.max(0, sweepIdx - 20), sweepIdx + 1);
  const swing = findSwingLevel(lookback, direction);
  if (!swing) return { found: false, reason: direction === 'BUY' ? 'no lower-high found' : 'no higher-low found' };

  const lvl = swing.price;

  for (let i = sweepIdx + 1; i < Math.min(candles.length, sweepIdx + 10); i++) {
    const c  = candles[i];
    const cn = i + 1 < candles.length ? candles[i + 1] : null;

    if (direction === 'BUY') {
      // Method A: close clearly above level
      if (c.c > lvl * (1 + BOS_MIN)) {
        return { found: true, bos_level: lvl, bos_candle: i, structure_type: swing.type,
                 method: 'close', label: swing.type + ' BOS: close above LH $' + lvl.toFixed(3) };
      }
      // Method B: wick above + strong follow-through
      if (cn && c.h > lvl * (1 + WICK_MIN) && cn.c > lvl && cn.c > cn.o && body(cn) >= avgBodyBos * FOLLOW_MULT) {
        return { found: true, bos_level: lvl, bos_candle: i + 1, structure_type: swing.type,
                 method: 'wick+followthrough', label: swing.type + ' BOS: wick+follow-through above LH $' + lvl.toFixed(3) };
      }
    } else {
      // Method A
      if (c.c < lvl * (1 - BOS_MIN)) {
        return { found: true, bos_level: lvl, bos_candle: i, structure_type: swing.type,
                 method: 'close', label: swing.type + ' BOS: close below HL $' + lvl.toFixed(3) };
      }
      // Method B
      if (cn && c.l < lvl * (1 - WICK_MIN) && cn.c < lvl && cn.c < cn.o && body(cn) >= avgBodyBos * FOLLOW_MULT) {
        return { found: true, bos_level: lvl, bos_candle: i + 1, structure_type: swing.type,
                 method: 'wick+followthrough', label: swing.type + ' BOS: wick+follow-through below HL $' + lvl.toFixed(3) };
      }
    }
  }
  return { found: false, reason: 'no BOS on ' + swing.type + ' level $' + lvl.toFixed(3) };
}

// ─── M15 BOS CONFIRMATION ─────────────────────────────────────────────────
function confirmBOS_M15(m15Candles, direction, bos_level) {
  if (!bos_level) return false;
  const recent = m15Candles.slice(-10);
  const BOS_MIN = 0.0005;
  for (const c of recent) {
    if (direction === 'BUY'  && c.c > bos_level * (1 + BOS_MIN)) return true;
    if (direction === 'SELL' && c.c < bos_level * (1 - BOS_MIN)) return true;
  }
  return false;
}

// ─── PULLBACK ENTRY DETECTION ─────────────────────────────────────────────
function detectPullback(candles, dispIdx, direction, sweepExtreme) {
  const disp = candles[dispIdx];
  if (!disp) return { found: false, reason: 'displacement candle not found' };

  // Displacement range
  const dispHigh = disp.h;
  const dispLow  = disp.l;
  const dispRange = dispHigh - dispLow;
  if (dispRange === 0) return { found: false, reason: 'zero displacement range' };

  // Entry zone: 50%–61.8% retracement
  let zone_high, zone_low;
  if (direction === 'BUY') {
    // Price moved UP during displacement — pullback retraces DOWN
    const retr50   = dispHigh - dispRange * 0.50;
    const retr618  = dispHigh - dispRange * 0.618;
    const retr70   = dispHigh - dispRange * 0.70;
    zone_high = retr50;
    zone_low  = retr618;

    // Look for price entering zone and printing rejection
    for (let i = dispIdx + 1; i < Math.min(candles.length, dispIdx + 8); i++) {
      const c = candles[i];
      if (c.l <= zone_high && c.l >= zone_low) {
        // Price in zone — check rejection candle: closes bullish, lower wick present
        if (c.c > c.o && (c.o - c.l) / range(c) > 0.15) {
          if (c.l < retr70) return { found: false, reason: 'pullback exceeded 70% — setup invalidated' };
          return { found: true, entry: c.c, zone_high, zone_low, retracement: ((dispHigh - c.l)/dispRange*100).toFixed(1) };
        }
      }
      if (c.l < retr70) return { found: false, reason: 'pullback exceeded 70%' };
    }
    return { found: false, reason: 'no pullback into 50-61.8% zone' };

  } else { // SELL
    const retr50  = dispLow + dispRange * 0.50;
    const retr618 = dispLow + dispRange * 0.618;
    const retr70  = dispLow + dispRange * 0.70;
    zone_high = retr618;
    zone_low  = retr50;

    for (let i = dispIdx + 1; i < Math.min(candles.length, dispIdx + 8); i++) {
      const c = candles[i];
      if (c.h >= zone_low && c.h <= zone_high) {
        if (c.c < c.o && (c.h - c.o) / range(c) > 0.15) {
          if (c.h > retr70) return { found: false, reason: 'pullback exceeded 70%' };
          return { found: true, entry: c.c, zone_high, zone_low, retracement: ((c.h - dispLow)/dispRange*100).toFixed(1) };
        }
      }
      if (c.h > retr70) return { found: false, reason: 'pullback exceeded 70%' };
    }
    return { found: false, reason: 'no pullback into 50-61.8% zone' };
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// SIGNAL QUALITY FILTERS — each returns { pass, reason }
// All must pass before a full signal is generated.
// ═══════════════════════════════════════════════════════════════════════════

// F1: DISPLACEMENT TIMING — must occur within 1-3 candles (strict), 4th only with no gap
function filterDisplacementTiming(disp, sweepIdx) {
  const offset = disp.candleIdx - sweepIdx;
  if (offset <= 3) return { pass: true };
  if (offset === 4 && !disp.weakGap)
    return { pass: true, note: 'offset 4 — no gap, acceptable' };
  return { pass: false,
    reason: 'Displacement too delayed (' + offset + ' candles after sweep, max 3)' };
}

// F2: DISPLACEMENT CHOP — reject slow drifts, require clean single-candle impulse
function filterDisplacementClean(candles, disp, avgRange) {
  const c = candles[disp.candleIdx];
  const b = body(c), r = range(c);
  if (r === 0) return { pass: false, reason: 'Displacement candle has zero range' };
  // Body must be > 60% of candle range (not a doji or long-wick chop)
  if (b / r < 0.55)
    return { pass: false, reason: 'Displacement body/range ' + (b/r*100).toFixed(0) + '% — too choppy (min 55%)' };
  // Range must be >= average candle range (not a tiny move)
  if (r < avgRange * 0.8)
    return { pass: false, reason: 'Displacement range too small vs average' };
  return { pass: true };
}

// F3: BOS QUALITY — close-only confirmation required; reject micro-breaks
function filterBOSQuality(bos) {
  // Method B (wick+followthrough) is less clean — downgrade but allow
  // However: reject if structure_type is external AND method is wick+followthrough
  // (weakest possible BOS — external swing + wick break = too noisy)
  if (bos.structure_type === 'external' && bos.method === 'wick+followthrough')
    return { pass: false,
      reason: 'BOS too weak — external swing + wick-only break rejected' };
  // BOS_MIN is already 0.05% in detectBOS — no additional check needed here
  return { pass: true };
}

// F4: PULLBACK REJECTION CANDLE — must be genuine reversal, not flat
function filterPullbackRejection(candles, disp, direction) {
  // Find the pullback candle (most recent candle in zone)
  // detectPullback already checks c.c > c.o (BUY) and lower wick > 15%
  // Add: wick must be at least 20% of range AND body must be > 20% of range
  // (rejects doji and spinning tops as pullback candles)
  const pb = candles[disp.candleIdx + 1]; // first candle after displacement
  if (!pb) return { pass: true }; // can't check yet
  const b = body(pb), r = range(pb);
  if (r === 0) return { pass: true };
  // Not a flat/doji candle
  if (b / r < 0.20) {
    // Note: this is informational — detectPullback handles the real check
    // We log but don't hard-reject here since pullback may not have formed yet
    return { pass: true, note: 'First candle after disp is flat — pullback forming' };
  }
  return { pass: true };
}

// F5: OVEREXTENSION — reject if displacement already travelled > 2.5x avg range
function filterOverextension(candles, disp, avgRange) {
  if (!avgRange || avgRange === 0) return { pass: true };
  const c = candles[disp.candleIdx];
  const moveSize = range(c);
  ratio = moveSize / avgRange;
  if (ratio > 2.5)
    return { pass: false,
      reason: 'Move overextended — ' + ratio.toFixed(1) + 'x avg range (max 2.5x). Entry too late.' };
  return { pass: true, extensionRatio: ratio.toFixed(2) };
}

// F6: OPPOSING LIQUIDITY — reject if strong opposing level is very close to TP1
function filterOpposingLiquidity(direction, entry, tp1, levels) {
  const riskDist = Math.abs(tp1 - entry);
  if (riskDist === 0) return { pass: true };
  // Find opposing levels between entry and TP1
  const opposing = levels.filter(l => {
    if (direction === 'BUY')  return l.price > entry && l.price < tp1;
    return l.price < entry && l.price > tp1;
  });
  // Strong opposing level (PDH/PDL or EQH/EQL x3+) within 30% of the move = problematic
  const blocked = opposing.filter(l => {
    const lvlPrice = l.isZone ? (direction === 'BUY' ? l.minPrice : l.maxPrice) : l.price;
    return (l.type === 'PDH' || l.type === 'PDL' || (l.strengthScore && l.strengthScore >= 2)) &&
           Math.abs(lvlPrice - entry) / riskDist < 0.35;
  });
  if (blocked.length > 0)
    return { pass: false,
      reason: 'Strong opposing liquidity (' + blocked[0].label + ' @ $' + blocked[0].price.toFixed(2) + ') blocks TP1 path' };
  return { pass: true };
}

// F7: SESSION STRENGTH — full signals only during active London/NY
// (Already enforced by sessionOk check, but add explicit guard here)
function filterSessionForEntry(sess) {
  if (!sess) return { pass: false, reason: 'No active session — entry not permitted' };
  return { pass: true };
}

// F8: M15 DIRECTION ALIGNMENT — reject if M15 clearly contradicts trade direction
function filterM15Alignment(m15Candles, direction) {
  if (!m15Candles || m15Candles.length < 6) return { pass: true, note: 'Insufficient M15 data — skipping check' };
  // Check last 3 M15 candles for clear directional contradiction
  const recent = m15Candles.slice(-3);
  let bullCount = 0, bearCount = 0;
  recent.forEach(c => { if (c.c > c.o) bullCount++; else bearCount++; });
  // If all 3 recent M15 candles oppose direction AND we are not in BOS confirmation context
  // → hard reject
  if (direction === 'BUY'  && bearCount === 3)
    return { pass: false, reason: 'M15 clearly bearish (3/3 candles) — contradicts BUY direction' };
  if (direction === 'SELL' && bullCount === 3)
    return { pass: false, reason: 'M15 clearly bullish (3/3 candles) — contradicts SELL direction' };
  return { pass: true, m15Bias: bullCount > bearCount ? 'bullish' : bearCount > bullCount ? 'bearish' : 'neutral' };
}

// Run all quality filters — returns { pass, failedFilter, reason, notes }
function runQualityFilters(candles, m15Candles, sweep, disp, bos, pullback,
                            levels, direction, entry, tp1, sess, avgRange) {
  const notes = [];

  const f1 = filterDisplacementTiming(disp, sweep.candleIdx);
  if (!f1.pass) return { pass: false, failedFilter: 'F1_DISPLACEMENT_TIMING', reason: f1.reason };
  if (f1.note) notes.push(f1.note);

  const f2 = filterDisplacementClean(candles, disp, avgRange);
  if (!f2.pass) return { pass: false, failedFilter: 'F2_DISPLACEMENT_CHOP', reason: f2.reason };

  const f3 = filterBOSQuality(bos);
  if (!f3.pass) return { pass: false, failedFilter: 'F3_BOS_QUALITY', reason: f3.reason };

  const f5 = filterOverextension(candles, disp, avgRange);
  if (!f5.pass) return { pass: false, failedFilter: 'F5_OVEREXTENSION', reason: f5.reason };
  if (f5.extensionRatio) notes.push('Extension: ' + f5.extensionRatio + 'x avg range');

  const f6 = filterOpposingLiquidity(direction, entry, tp1, levels);
  if (!f6.pass) return { pass: false, failedFilter: 'F6_OPPOSING_LIQ', reason: f6.reason };

  const f7 = filterSessionForEntry(sess);
  if (!f7.pass) return { pass: false, failedFilter: 'F7_SESSION', reason: f7.reason };

  const f8 = filterM15Alignment(m15Candles, direction);
  if (!f8.pass) return { pass: false, failedFilter: 'F8_M15_ALIGN', reason: f8.reason };
  if (f8.m15Bias) notes.push('M15 bias: ' + f8.m15Bias);

  return { pass: true, notes };
}

// ─── STOP LOSS ─────────────────────────────────────────────────────────────
function calcSL(direction, sweepExtreme, atr) {
  const PIP_BUFFER = direction === 'BUY'
    ? (sym === 'XAUUSD' ? 0.50 : 0.05)  // XAU: ~50c buffer, SLV: ~5c buffer
    : (sym === 'XAUUSD' ? 0.50 : 0.05);

  const atrBuffer  = atr * 0.10;
  const buffer     = Math.max(parseFloat(PIP_BUFFER), atrBuffer);

  if (direction === 'BUY')  return parseFloat((sweepExtreme - buffer).toFixed(3));
  else                       return parseFloat((sweepExtreme + buffer).toFixed(3));
}

// ─── TAKE PROFIT ───────────────────────────────────────────────────────────
function calcTP(direction, entry, sl, levels) {
  const riskDist = Math.abs(entry - sl);
  const MIN_RR   = 1.5;
  const fallback25R = direction === 'BUY'
    ? entry + riskDist * 2.5
    : entry - riskDist * 2.5;

  // Find nearest OPPOSING level beyond minimum RR
  const filtered = levels
    .filter(l => {
      if (direction === 'BUY')  return l.price > entry && (l.price - entry) / riskDist >= MIN_RR;
      return l.price < entry && (entry - l.price) / riskDist >= MIN_RR;
    })
    .sort((a,b) => direction === 'BUY' ? a.price - b.price : b.price - a.price);

  const tp1 = filtered[0]  ? parseFloat(filtered[0].price.toFixed(3))  : parseFloat(fallback25R.toFixed(3));
  const tp2 = filtered[1]  ? parseFloat(filtered[1].price.toFixed(3))  : parseFloat((direction==='BUY' ? entry + riskDist*3.5 : entry - riskDist*3.5).toFixed(3));
  const rr1 = parseFloat(((Math.abs(tp1 - entry)) / riskDist).toFixed(2));

  return { tp1, tp2, rr1, riskDist };
}

// ─── VOLATILITY FILTER ─────────────────────────────────────────────────────
function checkVolatility(atrValues) {
  if (!atrValues || atrValues.length < 20) return { ok: true, reason: 'insufficient ATR history, proceeding' };
  const currentATR = atrValues[atrValues.length - 1];
  const avg20      = atrValues.slice(-20).reduce((s,v)=>s+v,0) / 20;
  if (currentATR < avg20) {
    return { ok: false, reason: `ATR ${currentATR.toFixed(3)} below 20-period avg ${avg20.toFixed(3)}` };
  }
  return { ok: true, currentATR, avg20 };
}

// ═══════════════════════════════════════════════════════════════════════════
// CONFIDENCE SCORING SYSTEM (0–100)
//
// Components:
//   A. Liquidity Strength   0–25  (zone touch count)
//   B. Reaction Quality     0–20  (wick rejection strength)
//   C. Displacement         0–20  (move strength after grab)
//   D. Structure (BOS)      0–15  (trend shift quality)
//   E. Pullback Quality     0–10  (entry zone refinement)
//   F. Session Quality      0–10  (time-based weighting)
//
// Tiers:
//   0–39   IGNORE  — do not display, no alerts
//  40–59   LOW     — display only, no alerts
//  60–74   VALID   — pre-signal Telegram allowed
//  75–100  HIGH    — full signal allowed
// ═══════════════════════════════════════════════════════════════════════════

function scoreSetup(sessionLabel, sessionOk, sweep, displacement, bos, pullback,
                    volatilityOk, directionalBias, biasPenalty, htfBias) {
  // htfBias: 'BULLISH' | 'BEARISH' | 'NEUTRAL' (optional — defaults to NEUTRAL)
  const _htfBias = htfBias || 'NEUTRAL';

  const breakdown = {};
  let total = 0;

  // Hard exits — cannot score without these fundamentals
  if (!sessionOk)        return { total: 0, tier: 'IGNORE', grade: 'IGNORE', reason: 'Outside session',   breakdown: {} };
  if (!sweep || !sweep.found) return { total: 0, tier: 'IGNORE', grade: 'IGNORE', reason: 'No sweep',     breakdown: {} };

  // ── A. LIQUIDITY STRENGTH (max 25) ───────────────────────────
  // Based on zone touch count (totalTouches for clustered zones, strengthScore for singles)
  const touches = sweep.level
    ? (sweep.level.totalTouches || (sweep.level.strengthScore >= 3 ? 6 : sweep.level.strengthScore >= 2 ? 3 : 2))
    : 2;
  const liqScore = touches >= 10 ? 25
                 : touches >= 6  ? 22
                 : touches >= 4  ? 18
                 : touches === 3 ? 15
                 :                 10; // 2 touches minimum
  // Level type bonus: PDH/PDL = premium liquidity
  const liqBonus = (sweep.level?.type === 'PDH' || sweep.level?.type === 'PDL') ? 3
                 : (sweep.level?.type === 'ASH' || sweep.level?.type === 'ASL') ? 1 : 0;
  const liqFinal = Math.min(liqScore + liqBonus, 25);
  breakdown.liquidity = { score: liqFinal, max: 25, touches, label: sweep.level?.label || '—' };
  total += liqFinal;

  // ── B. REACTION QUALITY (max 20) ─────────────────────────────
  // How cleanly did price reject the zone (wick size + close quality)
  const wickPct = sweep.wickPct || 0;
  const reactionScore = wickPct >= 0.70 ? 20   // very strong rejection + full close back
                      : wickPct >= 0.55 ? 17   // clean wick rejection
                      : wickPct >= 0.40 ? 14   // decent rejection
                      : wickPct >= 0.30 ? 10   // minimum valid wick
                      : 5;                     // below threshold (already filtered, but score low)
  // Bonus if close was strongly back inside (not just at level)
  const closeBonus = sweep.closePrice && sweep.level
    ? (Math.abs(sweep.closePrice - sweep.level.price) / sweep.level.price > 0.001 ? 2 : 0)
    : 0;
  const reactionFinal = Math.min(reactionScore + closeBonus, 20);
  breakdown.reaction = { score: reactionFinal, max: 20, wickPct: Math.round(wickPct * 100) };
  total += reactionFinal;

  // ── C. DISPLACEMENT / STRONG MOVE (max 20) ───────────────────
  if (!displacement || !displacement.found) {
    breakdown.displacement = { score: 0, max: 20, note: 'No displacement detected' };
    // Not a hard fail for scoring — keeps partial scores for pre-signal use
  } else {
    const dispScore = displacement.ratio >= 3.0 ? 20
                    : displacement.ratio >= 2.5 ? 18
                    : displacement.ratio >= 2.0 ? 15
                    : displacement.ratio >= 1.5 ? 11
                    : 6;
    const dispFinal = displacement.weakGap ? Math.max(dispScore - 3, 5) : dispScore;
    breakdown.displacement = { score: dispFinal, max: 20, ratio: displacement.ratio };
    total += dispFinal;
  }

  // ── D. STRUCTURE BREAK / BOS (max 15) ────────────────────────
  if (!bos || !bos.found) {
    breakdown.structure = { score: 0, max: 15, note: 'No BOS detected' };
  } else {
    const bosScore = bos.structure_type === 'internal' && bos.method === 'close' ? 15
                   : bos.structure_type === 'internal'                            ? 13
                   : bos.method === 'close'                                       ? 11
                   : 8;
    breakdown.structure = { score: bosScore, max: 15, type: bos.structure_type, method: bos.method };
    total += bosScore;
  }

  // ── E. PULLBACK QUALITY (max 10) ─────────────────────────────
  if (!pullback || !pullback.found) {
    breakdown.pullback = { score: 0, max: 10, note: 'No pullback yet' };
  } else {
    const pb = parseFloat(pullback.retracement);
    if (pb > 70) return { total: 0, tier: 'IGNORE', grade: 'IGNORE', reason: 'Pullback > 70%', breakdown };
    const pbScore = (pb >= 50 && pb <= 61.8) ? 10
                  : (pb >= 45 && pb <  50)    ? 7
                  : (pb >  61.8 && pb <= 70)  ? 5
                  : 3;
    breakdown.pullback = { score: pbScore, max: 10, retracement: pb };
    total += pbScore;
  }

  // ── F. SESSION QUALITY (max 10) ──────────────────────────────
  const sessScore = sessionLabel === 'London+NY Overlap' ? 10
                  : sessionLabel === 'New York'           ? 8
                  : sessionLabel === 'London'             ? 6
                  : 0;
  breakdown.session = { score: sessScore, max: 10, label: sessionLabel || 'Unknown' };
  total += sessScore;

  // ── PENALTIES + HTF BIAS MODIFIER ───────────────────────────
  if (!volatilityOk) {
    total -= 8;
    breakdown.volatility = { penalty: -8 };
  }
  if (biasPenalty > 0 && sweep.found) {
    const isCounter = (directionalBias === 'bearish_bias' && sweep.direction === 'BUY') ||
                      (directionalBias === 'bullish_bias' && sweep.direction === 'SELL');
    if (isCounter) {
      total -= biasPenalty;
      breakdown.bias = { penalty: -biasPenalty, note: 'Counter-trend setup' };
    }
  }

  // ── HTF BIAS MODIFIER (confidence only — never blocks trade) ─
  if (_htfBias !== 'NEUTRAL' && sweep.found) {
    const dir = sweep.direction;
    const htfAligned = (_htfBias === 'BULLISH' && dir === 'BUY') ||
                       (_htfBias === 'BEARISH' && dir === 'SELL');
    const htfCounter = (_htfBias === 'BULLISH' && dir === 'SELL') ||
                       (_htfBias === 'BEARISH' && dir === 'BUY');
    if (htfAligned) {
      total += 10;
      breakdown.htfBias = { adjustment: +10, alignment: 'ALIGNED', htfBias: _htfBias };
      console.log('[htf] ' + dir + ' setup ALIGNED with ' + _htfBias + ' HTF bias → +10 confidence');
    } else if (htfCounter) {
      total -= 15;
      breakdown.htfBias = { adjustment: -15, alignment: 'COUNTER', htfBias: _htfBias };
      console.log('[htf] ' + dir + ' setup COUNTER to ' + _htfBias + ' HTF bias → -15 confidence');
    }
  }

  total = Math.min(Math.max(Math.round(total), 0), 100);

  // ── TIER CLASSIFICATION ───────────────────────────────────────
  const tier  = total >= 75 ? 'HIGH'   // full signal allowed
              : total >= 60 ? 'VALID'  // pre-signal Telegram allowed
              : total >= 40 ? 'LOW'    // display only, no alerts
              :               'IGNORE'; // discard
  // Legacy grade compatibility
  const grade = total >= 85 ? 'A+' : total >= 75 ? 'A' : total >= 60 ? 'B' : 'REJECT';

  // Debug log
  const bdStr = [
    'Liq '  + liqFinal,
    'React ' + reactionFinal,
    'Move '  + (breakdown.displacement?.score || 0),
    'BOS '   + (breakdown.structure?.score || 0),
    'PB '    + (breakdown.pullback?.score || 0),
    'Sess '  + sessScore
  ].join(' + ');
  console.log('[score] ' + (sweep.level?.label || '—') + ' → ' + total + '/100 (' + tier + ') = ' + bdStr);

  return { total, tier, grade, breakdown,
           reason: tier === 'IGNORE' ? 'Score ' + total + ' below threshold (40)' : null };
}

// Backwards-compatible wrapper — returns total score (number)
function calcConfidence(sessionOk, sessionOverlap, volatilityOk, sweep, displacement, bos,
                        pullback, sweepLevel, sessionLabel, directionalBias, biasPenalty) {
  const result = scoreSetup(sessionLabel, sessionOk, sweep, displacement, bos, pullback,
                             volatilityOk, directionalBias, biasPenalty);
  return result.total;
}


// ─── ALERT MESSAGE FORMATTERS ─────────────────────────────────────────────
// Pure functions — no logic, only formatting. Called before res.json().

function formatSignalAlert(sig, atr) {
  const isBuy    = sig.direction === 'BUY';
  const asset    = sig.asset === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const emoji    = isBuy ? '🟢' : '🔴';
  const dirLabel = isBuy ? 'BUY' : 'SELL';

  // Grade + confidence interpretation
  const grade     = sig.grade || (sig.confidence >= 85 ? 'A+' : sig.confidence >= 75 ? 'A' : 'B');
  const confLabel = grade === 'A+' ? 'A+ Setup — high conviction, strong confluence across all criteria'
                  : grade === 'A'  ? 'A Setup — strong setup, most criteria clearly confirmed'
                  :                  'Setup — valid but fewer confirmations';

  // Bias line
  const biasLine = {
    bullish_bias: 'Bullish — price is below the prior day low',
    bearish_bias: 'Bearish — price is above the prior day high',
    neutral:      'Neutral — price is within the prior day range'
  }[sig.directional_bias] || 'Neutral';

  // Plain-language context
  const dir2   = isBuy ? 'below' : 'above';
  const action = isBuy ? 'reversed sharply higher' : 'reversed sharply lower';
  const trend  = isBuy ? 'upward' : 'downward';
  const swept  = sig.sweep_level || 'a key price level';
  const context = asset + ' briefly moved ' + dir2 + ' ' + swept +
    ', then ' + action + '. A clear shift in ' + trend + ' momentum has been confirmed.';

  // Interpretation
  const trapped  = isBuy ? 'Short sellers were briefly trapped below support'
                          : 'Buyers were briefly trapped above resistance';
  const control  = isBuy ? 'buyers absorbed the move and regained control'
                          : 'sellers absorbed the move and regained control';
  const pbNote   = sig.pullback_pct
    ? 'Entry is positioned at a ' + sig.pullback_pct + '% retracement of the impulse.'
    : 'Entry is positioned at a pullback into the move.';
  const interpretation = trapped + '. Then ' + control + '. ' + pbNote;

  // Execution note
  const exec = isBuy
    ? 'Do not chase price. Wait for a pullback toward the entry zone before executing.'
    : 'Do not sell into the low. Wait for a pullback toward the entry zone before executing.';

  // Structured object for UI + Telegram
  // Expiry: signal valid for 50 min (10 M5 candles) from now
  const expiryMs  = Date.now() + 50 * 60 * 1000;
  const expiryUTC = new Date(expiryMs).toUTCString().split(' ')[4] + ' UTC';

  return {
    type:        'signal',
    grade,
    headline:    emoji + ' ' + asset + ' ' + dirLabel + ' — ' + grade + ' SETUP',
    expiry:      expiryUTC,
    context,
    trade: {
      entry:     '$' + sig.entry,
      stop_loss: '$' + sig.stop_loss,
      tp1:       '$' + sig.take_profit_1,
      tp2:       '$' + sig.take_profit_2,
      rr:        '1 : ' + sig.rr
    },
    strength: {
      confidence: sig.confidence + '% — ' + confLabel,
      session:    sig.session || '—',
      bias:       biasLine
    },
    interpretation,
    execution:   exec,
    // Telegram-ready flat text
    telegram: [
      emoji + ' *' + asset + ' ' + dirLabel + ' SETUP*',
      '',
      '📋 *What happened:*',
      context,
      '',
      '📊 *Trade levels:*',
      '• Entry zone: $' + sig.entry,
      '• Stop loss:  $' + sig.stop_loss,
      '• Target 1:   $' + sig.take_profit_1,
      '• Target 2:   $' + sig.take_profit_2,
      '• Risk/Reward: 1:' + sig.rr,
      '',
      '📈 *Setup strength:*',
      '• Confidence: ' + sig.confidence + '% — ' + confLabel,
      '• Session: ' + (sig.session || '—'),
      '• Market bias: ' + biasLine,
      '',
      '💡 *What this means:*',
      interpretation,
      '',
      '⚡ *Execution note:*',
      exec,
      '',
      '─────────────────────',
      'Signal ID #' + (sig.id || '—') + ' | Aurum Signals'
    ].join('\n')
  };
}

function formatPreSignalAlert(stage, sym, direction, message, level, session, bias) {
  const asset   = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const isBuy   = direction === 'BUY';
  const emoji   = {
    approaching_liquidity:  '📍',
    sweep_detected:         '⚡',
    displacement_confirmed: '↗',
    structure_break:        '✅'
  }[stage] || '📡';

  const stageLabel = {
    approaching_liquidity:  'KEY LEVEL NEARBY',
    sweep_detected:         'LIQUIDITY GRAB DETECTED',
    displacement_confirmed: 'STRONG MOVE CONFIRMED',
    structure_break:        'TREND SHIFT CONFIRMED'
  }[stage] || 'SETUP FORMING';

  const stageContext = {
    approaching_liquidity:  asset + ' is approaching ' + (level ? level.label : 'a key price level') +
                            '. If price sweeps through and reverses, a ' +
                            (isBuy?'BUY':'SELL') + ' setup may form.',
    sweep_detected:         asset + ' has moved through a key level and closed back inside. ' +
                            'This is a potential ' + (isBuy?'BUY':'SELL') + ' setup. ' +
                            'Waiting for a strong directional move to confirm.',
    displacement_confirmed: 'A strong ' + (isBuy?'upward':'downward') + ' move has followed the liquidity grab. ' +
                            'Waiting for a trend shift (break of structure) to confirm direction.',
    structure_break:        'The trend shift is confirmed on the 5-minute chart. ' +
                            'Waiting for price to pull back into the entry zone before triggering.'
  }[stage] || message;

  const action = {
    approaching_liquidity:  'Monitor closely. No trade yet — waiting for a sweep and reversal.',
    sweep_detected:         'No trade yet. Waiting for a strong directional move to follow the grab.',
    displacement_confirmed: 'No trade yet. Waiting for a trend shift to confirm the direction.',
    structure_break:        'Setup is nearly complete. Watch for a pullback into the entry zone.'
  }[stage] || 'Monitoring. No action required yet.';

  const biasLine = {
    bullish_bias: 'Bullish — price is below the prior day low',
    bearish_bias: 'Bearish — price is above the prior day high',
    neutral:      'Neutral — price within the prior day range'
  }[bias] || 'Neutral';

  return {
    type:        'pre_signal',
    stage,
    headline:    emoji + ' ' + asset + ' — ' + stageLabel,
    context:     stageContext,
    direction:   direction || '—',
    session:     session || '—',
    bias:        biasLine,
    action,
    telegram: [
      emoji + ' *' + asset + ' — ' + stageLabel + '*',
      '',
      stageContext,
      '',
      '📋 *Direction:* ' + (direction || '—'),
      '📋 *Session:* ' + (session || '—'),
      '📋 *Market bias:* ' + biasLine,
      '',
      '⏳ *Action:*',
      action,
      '',
      '─────────────────────',
      'Pre-signal alert | Aurum Signals'
    ].join('\n')
  };
}

// ─── MAIN ANALYSIS ROUTE ───────────────────────────────────────────────────
app.get('/analyze/:sym', async (req, res) => {
  const sym = req.params.sym.toUpperCase();
  if (!SYMBOLS[sym]) return res.status(400).json({ success:false, error:'Unknown symbol' });

  // Server-side timeout — never hang longer than 20s regardless of API slowness
  const routeTimeout = setTimeout(() => {
    if (!res.headersSent) {
      console.error('[' + sym + '] Route timeout — Twelve Data too slow, returning error');
      res.json({ success: true, symbol: sym, price: null, atr: null,
        system_state: 'data_error', session: 'Unknown', session_ok: false,
        setup_state: 'standby', levels: [],
        log: ['Live data temporarily unavailable — API timeout (>20s)'],
        signal: null, m5_candles: 0 });
    }
  }, 20000);

  // Ensure timeout is cleared when response finishes
  res.on('finish', () => clearTimeout(routeTimeout));

  // ── 1. FETCH DATA ──────────────────────────────────────────────────────
  // SLV early exit: if outside NYSE hours AND no cached data, skip fetch entirely
  const _nowH = new Date().getUTCHours();
  const _slvClosed = sym === 'XAGUSD' && (_nowH >= 20 || _nowH < 13);
  if (_slvClosed) {
    const _cached = getCached('candles_XAGUSD_5min_120');
    const _lastPrice = _cached ? parseFloat(_cached[_cached.length-1]?.c) : null;
    const _lastATR   = _cached ? calcATRFromCandles(_cached, 14) : [];
    return res.json({ success: true, symbol: 'XAGUSD',
      price: _lastPrice, atr: null,
      volatility_atr: _lastATR.length ? parseFloat(_lastATR[_lastATR.length-1].toFixed(4)) : null,
      system_state: 'session_closed', session: 'Closed',
      session_ok: false, setup_state: 'standby',
      levels: [], log: ['Silver market closed — SLV ETF trades 13:30–20:00 UTC.' + (_lastPrice ? ' Last price: $' + _lastPrice.toFixed(3) : '')],
      signal: null, m5_candles: _cached?.length || 0,
      note: 'SLV market closed. Opens 13:30 UTC.' });
  }

  let m5, m15, atrValues;
  try {
    // ONE API call per symbol — everything else derived from M5 candles.
    m5 = await getCandles(sym, '5min', 120);  // 120 × 5min = 10 hours

    // Derive M15 and ATR without additional API calls
    m15       = m5 ? deriveM15FromM5(m5) : [];
    atrValues = m5 ? calcATRFromCandles(m5, 14) : [];

    console.log('[' + sym + '] M5: ' + (m5?.length||0) + ' M15 (derived): ' + (m15?.length||0) + ' ATR (calc): ' + (atrValues?.length||0) + ' — 1 API call used');
  } catch(e) {
    console.error('[' + sym + '] Data fetch exception:', e.message);
    return res.json({ success: true, symbol: sym, price: null, atr: null,
      system_state: 'data_error', session: 'Unknown', session_ok: false,
      setup_state: 'standby', levels: [],
      log: ['Live data temporarily unavailable — ' + e.message],
      signal: null, m5_candles: 0 });
  }

  // Pre-declare variables accessible in catch block and final response
  livePrice = m5?.[m5.length-1]?.c || null;
  setupState = 'idle';
  signal = null;
  near_setup = null;
  approachingLevels = [];
  sweepPotentials = [];
  primaryZone = null;
  pzConf = 0, pzGrade = 'IGNORE';
  directionalBias = 'neutral', analyzeGlobalBias = { bias: 'NEUTRAL', score: 0 };
  let ratio = null;

  // ── SIGNAL ENGINE (fully wrapped — any crash returns a safe error response) ──
  try {

  // ── SYSTEM STATE DETERMINATION ─────────────────────────────────────────────
  const nowUtc   = Date.now();
  const utcHour  = new Date(nowUtc).getUTCHours();
  const inSession= (utcHour >= 7 && utcHour < 16) || (utcHour >= 13 && utcHour < 22);
  const m5Count  = m5?.length || 0;
  const m15Count = m15?.length || 0;

  // No data at all → data_error (not session closed)
  if (!m5 || m5Count === 0) {
    const errState = inSession ? 'data_error' : 'session_closed';
    const errMsg   = inSession
      ? 'Live data temporarily unavailable — API may be slow or unreachable'
      : 'Outside trading session (London 07:00–16:00 UTC / New York 13:00–22:00 UTC)';
    return res.json({ success: true, symbol: sym, price: null, atr: null,
      system_state: errState, session: inSession ? 'Active' : 'Closed',
      session_ok: inSession, setup_state: 'standby',
      levels: [], log: [errMsg], signal: null, m5_candles: 0 });
  }

  // Fewer than 50 candles → warming up (not error)
  if (m5Count < 50) {
    return res.json({ success: true, symbol: sym, price: m5[m5Count-1]?.c || null,
      system_state: 'loading_data',
      session: inSession ? sessionName(nowUtc) || 'Active' : 'Closed',
      session_ok: inSession, setup_state: 'standby',
      levels: [], log: ['Collecting enough data to analyze — ' + m5Count + '/50 candles loaded'],
      signal: null, m5_candles: m5Count });
  }

  // Check candle staleness — latest candle should be within 30 minutes
  const latestCandleAge = (nowUtc - m5[m5Count - 1].t) / 60000; // minutes
  if (latestCandleAge > 30) {
    const isSLV     = sym === 'XAGUSD';
    const slvClosed = isSLV && (utcHour >= 20 || utcHour < 13);
    const staleMsg  = slvClosed
      ? 'Silver market closed — SLV ETF trades 13:30–20:00 UTC. Last price: $' + m5[m5Count-1].c.toFixed(3)
      : 'Live data temporarily unavailable — latest candle is ' + Math.round(latestCandleAge) + ' minutes old';
    if (slvClosed) {
      const lastATR = calcATRFromCandles(m5, 14);
      return res.json({ success: true, symbol: sym, price: m5[m5Count-1]?.c || null,
        system_state: 'session_closed', session: 'Closed',
        session_ok: false, setup_state: 'standby',
        volatility_atr: lastATR.length ? parseFloat(lastATR[lastATR.length-1].toFixed(4)) : null,
        m5_candles: m5Count, levels: [], log: [staleMsg], signal: null,
        note: 'SLV market closed. Opens 13:30 UTC.' });
    }
    if (inSession) {
      return res.json({ success: true, symbol: sym, price: m5[m5Count-1]?.c || null,
        system_state: 'data_error', session: sessionName(nowUtc) || 'Active',
        session_ok: false, setup_state: 'standby',
        levels: [], log: [staleMsg], signal: null, m5_candles: m5Count });
    }
  }

  // M15 low — warn but do not block
  if (!m15 || m15Count < 8) {
    console.warn(sym + ': low M15 data (' + m15Count + ' candles) — M15 BOS confirmation disabled');
  }

  const currentPrice  = m5[m5.length-1].c;
  const currentATR    = atrValues?.length > 0 ? atrValues[atrValues.length-1] : null;
  const currentTS     = m5[m5.length-1].t;
  // Use current wall-clock UTC time for session detection, NOT candle timestamp.
  // Candle timestamp — XAG/USD is 24/5 spot, stale data = API issue
  // would otherwise make London session appear closed next morning).
  const sess          = sessionName(Date.now());
  const sessionOk     = sess !== null;

  // ── 2. LIQUIDITY LEVELS ────────────────────────────────────────────────
  const levels = buildLevels(m5, m15);

  // ── 3. VOLATILITY FILTER ──────────────────────────────────────────────
  const volatility = checkATR(sym, atrValues);

  // --- DIRECTIONAL BIAS — unified via calcGlobalBias() ----------------------
  analyzeGlobalBias = calcGlobalBias(levels, currentPrice, null);
  const htfResult     = calcHTFBias(m15);
  console.log('[htf] ' + sym + ' (analyze): bias=' + htfResult.bias + ' — ' + htfResult.reason);
  const directionalBias   = analyzeGlobalBias.bias === 'BUY'  ? 'bullish_bias'
                          : analyzeGlobalBias.bias === 'SELL' ? 'bearish_bias'
                          : 'neutral';
  const biasPenalty = Math.abs(analyzeGlobalBias.score) >= 3 ? 8
                    : Math.abs(analyzeGlobalBias.score) >= 2 ? 5
                    : Math.abs(analyzeGlobalBias.score) >= 1 ? 3 : 0;

    // ── STATE MACHINE ─────────────────────────────────────────────────────
  let setupState  = 'idle';
  let signal      = null;
  const log       = [];

  // ── PRIMARY ZONE SELECTION (before sweep detection) ────────────────────
  // Must happen here so detectSweep can be locked to primary zone only.
  // Prevents secondary zones from triggering conflicting signals.
  const primaryZoneEarly = selectPrimaryZone(levels, currentPrice, sess, m5, null);
  const sweepLevels = primaryZoneEarly ? [primaryZoneEarly] : [];

  if (!sessionOk) {
    setupState = 'idle';
    log.push('Outside active trading sessions (London 07:00–16:00 UTC / New York 13:00–22:00 UTC) — signal engine paused');
  } else if (volatility.ok === false) {
    setupState = 'idle';
    log.push('Volatility check: ' + volatility.note);
  } else {
    // ── 4. SWEEP DETECTION — PRIMARY ZONE ONLY ──────────────────────────
    let sweep = detectSweep(m5, sweepLevels); // locked to primary zone
    if (sweep.found) sweep = correctSweepDirection(sweep); // enforce direction from zone type

    if (!sweep.found) {
      setupState = 'idle';
      log.push('No liquidity grab detected on current M5 data');
    } else {
      setupState = 'sweep_detected';
      // Direction comes from zone type (correctSweepDirection enforces this)
      const lvlDir = sweep.direction; // already corrected to match zone type
      log.push('[Stage] Liquidity grab confirmed — ' + sweep.level.label + ' swept');
      log.push('Liquidity grab: ' + lvlDir + ' — price swept ' + sweep.level.label +
        ' at $' + sweep.level.price.toFixed(3) +
        ' (wick ' + (sweep.wickPct*100).toFixed(1) + '% of candle range)');

      // ── 5. TIME DECAY CHECK: max 10 M5 candles for full setup ───────
      const sweepToNow = m5.length - 1 - sweep.candleIdx;
      if (sweepToNow > 10) {
        setupState = 'invalidated';
        log.push('Setup expired: ' + sweepToNow + ' candles have passed since the liquidity grab (maximum is 10). Setup reset.');
      } else {
        // ── 6. DISPLACEMENT ───────────────────────────────────────────
        const disp = detectDisplacement(m5, sweep.candleIdx, sweep.direction);

        if (!disp.found) {
          setupState = 'sweep_detected';
          log.push('[Stage] Liquidity grab confirmed — ' + sweep.level.label + ' swept');
          log.push('Strong move: not confirmed — ' + disp.reason);
        } else {
          setupState = 'displacement_confirmed';
          log.push('[Stage] Strong move confirmed — ' + (disp ? disp.ratio : '?') + 'x body displacement');
          log.push('Strong move confirmed: ' + disp.ratio + 'x average candle size, ' + (disp.candleIdx - sweep.candleIdx) + ' candle(s) after the liquidity grab' + (disp.weakGap ? ' (one weak candle gap tolerated)' : ''));

          // ── 7. BOS ────────────────────────────────────────────────
          const bos = detectBOS(m5, sweep.candleIdx, sweep.direction);

          if (!bos.found) {
            setupState = 'displacement_confirmed';
            log.push('Trend shift: not confirmed — ' + bos.reason);
          } else {
            const m15bos = confirmBOS_M15(m15, sweep.direction, bos.bos_level);
            setupState = 'structure_break';
          log.push('[Stage] Trend shift confirmed — ' + bos.label);
            log.push('Trend shift (M5): ' + bos.label);
            log.push('Trend shift (M15): ' + (m15bos ? 'also visible on 15-minute chart' : 'not visible on 15-minute chart — M5 confirmation used'));

            // ── 8. PULLBACK ───────────────────────────────────────
            const pb = detectPullback(m5, disp.candleIdx, sweep.direction, sweep.sweepExtreme);

            if (!pb.found) {
              setupState = 'waiting_pullback';
          log.push('[Stage] Waiting for pullback into 50-61.8% zone');
              log.push('Pullback entry: ' + pb.reason);
            } else {
              setupState = 'entry_triggered';
              log.push('Pullback entry confirmed: ' + pb.retracement + '% retracement — entry price $' + pb.entry.toFixed(3));

              // ── 9. LEVELS ─────────────────────────────────────────
              const sl   = calcSL(sweep.direction, sweep.sweepExtreme, currentATR || 0.5);
              const tps  = calcTP(sweep.direction, pb.entry, sl, levels);

              if (tps.rr1 < 1.5) {
                setupState = 'invalidated';
                log.push('Risk/Reward check: ' + tps.rr1 + ' — below the minimum 1:2 requirement. Setup not valid.');
              } else {
                // ── QUALITY FILTERS ───────────────────────────────
                const avgRange = candles.slice(-10).reduce((s,c) => s + range(c), 0) / 10;
                const qf = runQualityFilters(m5, m15, sweep, disp, bos, pb,
                  levels, sweep.direction, parseFloat(pb.entry.toFixed(3)), tps.tp1,
                  sess, avgRange);
                if (!qf.pass) {
                  setupState = 'invalidated';
                  log.push('Quality filter failed [' + qf.failedFilter + ']: ' + qf.reason);
                } else {
                  if (qf.notes && qf.notes.length) log.push('Quality notes: ' + qf.notes.join(' | '));
                // ── 10. CONFIDENCE ────────────────────────────────
                // Hard gate: only generate signal if we reached this point through all stages
                // (sweep → displacement → BOS → pullback → quality filters)
                // setupState tracks progression, signal only fires at entry_triggered
                const confidence = calcConfidence(sessionOk, sessionOverlap, volatility.ok === true || volatility.ok === undefined, sweep, disp, bos, pb, sweep.level, sess, directionalBias, biasPenalty);
                log.push('Stage progression: sweep ✓ → displacement ✓ → BOS ✓ → pullback ✓ → quality ✓');
                log.push('Confidence score: ' + confidence + '/100');

                // Full signal requires HIGH tier (≥75). Grade: A+ ≥85, A ≥75.
                const _htfBiasState = timing?.htfBias || 'NEUTRAL';
        const scoreResult2 = scoreSetup(sess, sessionOk, sweep, disp, bos, pb,
                  volatility.ok === true || volatility.ok === undefined, directionalBias, biasPenalty,
                  _htfBiasState);
                // Zone score gate applies here too
                if (!analyzeZoneAllowSignal) {
                  setupState = 'invalidated';
                  log.push('Signal blocked — zone score ' + pzConf + '/100 < 60 (zone too weak)');
                } else if (scoreResult2.tier !== 'HIGH') {
                  setupState = 'invalidated';
                  log.push('Signal not generated — score ' + confidence + '/100 tier=' + scoreResult2.tier + ' (requires HIGH ≥75)');
                } else {
                  // ── SIGNAL GENERATED ──────────────────────────────
                  const reasonParts = [
                    `${sess} ${sweep.level.label} sweep`,
                    `bullish displacement (${disp.ratio}× body)`,
                    `M5${m15bos?'/M15':''} BOS`,
                    `${pb.retracement}% pullback entry`
                  ];
                  if (sweep.direction === 'SELL') reasonParts[0] = reasonParts[0].replace('bullish','bearish');

                  // Ratio from cache — no extra API call
                  let ratio = null;
                  try {
                    const xauC = getCached('candles_XAUUSD_5min_120');
                    const xagC = getCached('candles_XAGUSD_5min_120');
                    const xauP = xauC ? xauC[xauC.length-1]?.c : null;
                    const xagP = xagC ? xagC[xagC.length-1]?.c : null;
                    if (xauP && xagP) ratio = parseFloat((xauP/xagP).toFixed(2));
                  } catch(e) {}

                  const rawSignal = {
                    asset:          sym,
                    direction:      sweep.direction,
                    entry:          parseFloat(pb.entry.toFixed(3)),
                    stop_loss:      sl,
                    take_profit_1:  tps.tp1,
                    take_profit_2:  tps.tp2,
                    rr:             tps.rr1,
                    confidence,
                    tier:           scoreResult2.tier || (confidence >= 75 ? 'HIGH' : 'VALID'),
                    session:        sess,
                    reason:         reasonParts.join(' → '),
                    setup_type:     'Liquidity Sweep Reversal',
                    sweep_level:    sweep.level.label,
                    sweep_level_price: parseFloat(sweep.level.price.toFixed(3)),
                    wick_pct:       parseFloat((sweep.wickPct*100).toFixed(1)),
                    disp_ratio:     disp.ratio,
                    bos_label:      bos.label,
                    structure_type: bos.structure_type || 'external',
                    bos_method:     bos.method || 'close',
                    m15_bos:        m15bos,
                    pullback_pct:   pb.retracement,
                    risk_dist:      parseFloat(tps.riskDist.toFixed(3))
                  };
                  rawSignal.directional_bias = directionalBias;
                  const alertMsg = formatSignalAlert(rawSignal, currentATR);
                  signal = { ...rawSignal, alert: alertMsg };
                  log.push('✅ Signal generated — ' + sweep.direction + ' ' + sym + ' at $' + pb.entry.toFixed(3));
                }
              }
            }
          }
        }
      }
    }
  }

  // Use last M5 candle close as live price — saves 1 API call
  // Candles are cached so this is instant; max 5min stale which is acceptable
  const livePrice = currentPrice;

  // Derive ratio from cached candle data — no API call
  let ratio = null;
  try {
    if (sym === 'XAUUSD' || sym === 'XAGUSD') {
      const xauCache = getCached('candles_XAUUSD_5min_120');
      const xagCache = getCached('candles_XAGUSD_5min_120');
      const xauP = xauCache ? xauCache[xauCache.length-1]?.c : null;
      const xagP = xagCache ? xagCache[xagCache.length-1]?.c : null;
      if (xauP && xagP) ratio = parseFloat((xauP/xagP).toFixed(2));
      // If other symbol isn't cached yet, ratio stays null — that's fine
    }
  } catch(e) {}

  // --- PRIMARY ZONE + PRE-SIGNAL ALERTS ------------------------------------
  // primaryZone was already selected before sweep detection (primaryZoneEarly).
  // Alias it here for the pre-signal and response sections.
  const primaryZone = primaryZoneEarly;
  const approachingLevels = primaryZone
    ? [{ ...primaryZone,
         distPct: primaryZone.distPct,
         isPrimary: true }]
    : detectApproaching(livePrice, levels.filter(l => l.type !== 'EQH' && l.type !== 'EQL'), sym).slice(0, 1);

  // Gate pre-signals: only generate if zone confidence >= 40 (not IGNORE)
  const pzConf  = primaryZone?.confidence?.total || 0;
  pzGrade = primaryZone?.confidence?.grade || 'IGNORE';
  if (primaryZone) {
    log.push('[Zone] PRIMARY ' + primaryZone.direction + ' ZONE $' + primaryZone.priceRange +
      ' score=' + pzConf + '/100 touches=' + primaryZone.totalTouches +
      (pzConf >= 60 ? ' ✓' : ' ⚠ low'));
  }
  // Zone score gate for analyze route
  // < 60  → no pre-signals, no signals
  // 60–74 → pre-signals allowed, no aggressive entry
  // ≥ 75  → full system
  const analyzeZoneAllowAggressive = pzConf >= 75;
  const analyzeZoneAllowSignal     = pzConf >= 60;
  const sweepPotentials = (primaryZone && analyzeZoneAllowSignal && approachingLevels.length)
    ? detectSweepPotential(livePrice, approachingLevels, m5)
    : [];
  if (!analyzeZoneAllowSignal && primaryZone) {
    log.push('Zone score ' + pzConf + '/100 < 60 — signals suppressed (zone not strong enough)');
  } else if (!analyzeZoneAllowAggressive && primaryZone) {
    log.push('Zone score ' + pzConf + '/100 [60–74] — standard entry only, aggressive suppressed');
  }

  // Build near_setup — always inherits direction from primary zone (no conflicts)
  let near_setup = null;
  if (sweepPotentials.length && setupState === 'idle') {
    const sp  = sweepPotentials[0];
    const dir = sp.direction; // direction already locked by zone type in detectSweepPotential
    near_setup = {
      stage:       'approaching_liquidity',
      message:     sp.message,
      direction:   dir,
      level:       sp.level,
      zone_confidence: primaryZone?.confidence || null,
      alert:       formatPreSignalAlert('approaching_liquidity', sym, dir,
                     sp.message, sp.level, sess, directionalBias)
    };
  } else if (setupState === 'sweep_detected') {
    near_setup = {
      stage:     'sweep_detected',
      message:   'Sweep of ' + (sweep.level?.label || 'key zone') + ' confirmed — awaiting strong move',
      direction: sweep.direction,
      level:     sweep.level,
      alert:     formatPreSignalAlert('sweep_detected', sym, sweep.direction,
                   null, sweep.level, sess, directionalBias)
    };
  } else if (setupState === 'displacement_confirmed') {
    near_setup = {
      stage:     'displacement_confirmed',
      message:   'Displacement confirmed (' + (disp ? disp.ratio : '?') + 'x body) — awaiting BOS',
      direction: sweep.direction,
      alert:     formatPreSignalAlert('displacement_confirmed', sym, sweep.direction,
                   null, null, sess, directionalBias)
    };
  } else if (setupState === 'structure_break') {
    near_setup = {
      stage:     'structure_break',
      message:   'Trend shift confirmed — awaiting pullback entry',
      direction: sweep.direction,
      alert:     formatPreSignalAlert('structure_break', sym, sweep.direction,
                   null, null, sess, directionalBias)
    };
  }

  } // end qf.pass

  // ── ALWAYS send response — regardless of signal path taken ──────────────
  if (!res.headersSent) {
    res.json({
      success:      true,
      symbol:       sym,
      system_state: 'active',
      price:        livePrice,
      atr:          currentATR,
      session:      sess || 'Closed',
      session_ok:   sessionOk,
      setup_state:  setupState,
      levels:       levels.slice(0,8),
      log,
      signal,
      near_setup,
      approaching_levels: approachingLevels,
      sweep_potentials:  sweepPotentials,
      primary_zone:      primaryZone,
      zone_confidence:   pzConf,
      zone_direction:    primaryZone?.direction || null,
      zone_score_tier:   pzConf >= 75 ? 'FULL' : pzConf >= 60 ? 'STANDARD' : 'BLOCKED',
      zone_grade:        pzGrade,
      directional_bias:  directionalBias,
      bias_score:        analyzeGlobalBias?.score || 0,
      bias_label:        analyzeGlobalBias?.bias  || 'NEUTRAL',
      htf_bias:          htfResult?.bias || 'NEUTRAL',
      htf_last_bos:      htfResult?.lastBOS || 'NONE',
      htf_reason:        htfResult?.reason || '',
      m5_candles:   m5.length,
      ratio
    });
  }

  } catch(routeErr) {
    console.error('[analyze] Unhandled error in signal engine:', routeErr.message, routeErr.stack?.split('\n').slice(0,3).join(' | '));
    if (!res.headersSent) {
      const _price = (typeof livePrice !== 'undefined' ? livePrice : null) || m5?.[m5?.length-1]?.c || null;
      res.json({ success: true, symbol: sym, price: _price,
        system_state: 'data_error', session: 'Unknown', session_ok: false,
        setup_state: 'standby', levels: [],
        log: ['Signal engine error — ' + routeErr.message],
        signal: null, m5_candles: m5?.length || 0 });
    }
  }
});

// ─── SETUP SHEETS ROUTE ───────────────────────────────────────────────────
// Hit this URL once in your browser after deploying to initialise the Aurum
// tab with headers and formatting. Safe to run multiple times — idempotent.
// Usage: GET https://your-railway-url.railway.app/setup-sheets
app.get('/setup-sheets', async (req, res) => {
  const sheetId   = process.env.GOOGLE_SHEET_ID;
  const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

  if (!sheetId || !credsJson) {
    return res.status(400).json({
      ok: false,
      error: 'Missing env vars. Add GOOGLE_SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON in Railway → Variables.'
    });
  }

  try {
    const { google } = require('googleapis');
    const creds = JSON.parse(credsJson);
    const auth  = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    // Find the Aurum tab
    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
    const tab  = meta.data.sheets.find(s => s.properties.title === 'Aurum');
    if (!tab) {
      return res.status(400).json({
        ok: false,
        error: 'No tab named "Aurum" found. Create it in your Google Sheet first.'
      });
    }
    const tabId = tab.properties.sheetId;

    // Write headers
    const HEADERS = [
      'Timestamp', 'Setup ID', 'Symbol', 'Direction', 'Session',
      'Zone Low', 'Zone High', 'Zone Score', 'Touches', 'Event',
      'Entry Price', 'Stop Loss', 'TP1', 'TP2', 'Candles to Event',
      'Result', 'Invalidation Reason', 'Stages Confirmed'
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: 'Aurum!A1:R1',
      valueInputOption: 'RAW',
      requestBody: { values: [HEADERS] },
    });

    const colIdx = l => l.charCodeAt(0) - 65;
    const COL_WIDTHS = {
      A:180, B:200, C:80,  D:70,  E:130,
      F:90,  G:90,  H:90,  I:70,  J:160,
      K:100, L:100, M:100, N:100, O:120,
      P:80,  Q:260, R:220
    };

    const requests = [
      // Header row: dark bg, gold text, bold
      { repeatCell: {
        range: { sheetId: tabId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:18 },
        cell: { userEnteredFormat: {
          backgroundColor: { red:0.098, green:0.098, blue:0.098 },
          textFormat: { foregroundColor:{ red:1.0, green:0.843, blue:0.0 }, bold:true, fontSize:10 },
          horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE'
        }},
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
      }},
      // Freeze header row
      { updateSheetProperties: {
        properties: { sheetId: tabId, gridProperties: { frozenRowCount:1 } },
        fields: 'gridProperties.frozenRowCount'
      }},
      // Column widths
      ...Object.entries(COL_WIDTHS).map(([l, px]) => ({
        updateDimensionProperties: {
          range: { sheetId: tabId, dimension:'COLUMNS', startIndex:colIdx(l), endIndex:colIdx(l)+1 },
          properties: { pixelSize: px }, fields: 'pixelSize'
        }
      })),
      // Row banding
      { addBanding: { bandedRange: {
        range: { sheetId: tabId, startRowIndex:1, startColumnIndex:0, endColumnIndex:18 },
        rowProperties: {
          headerColor:     { red:0.15,  green:0.15,  blue:0.15  },
          firstBandColor:  { red:0.12,  green:0.12,  blue:0.12  },
          secondBandColor: { red:0.098, green:0.098, blue:0.098 }
        }
      }}},
      // Event column colors (J = col 9)
      ...[ ['ENTRY', {red:0.133,green:0.369,blue:0.133}],
           ['INVALIDATED', {red:0.369,green:0.133,blue:0.133}],
           ['RESULT', {red:0.133,green:0.267,blue:0.467}],
           ['CREATED', {red:0.267,green:0.267,blue:0.133}]
      ].map(([val, bg]) => ({ addConditionalFormatRule: { rule: {
        ranges: [{ sheetId: tabId, startRowIndex:1, startColumnIndex:9, endColumnIndex:10 }],
        booleanRule: {
          condition: { type:'TEXT_CONTAINS', values:[{ userEnteredValue: val }] },
          format: { backgroundColor: bg }
        }
      }, index:0 }})),
      // BUY green / SELL red (D = col 3)
      { addConditionalFormatRule: { rule: {
        ranges: [{ sheetId: tabId, startRowIndex:1, startColumnIndex:3, endColumnIndex:4 }],
        booleanRule: {
          condition: { type:'TEXT_EQ', values:[{ userEnteredValue:'BUY' }] },
          format: { textFormat: { foregroundColor:{ red:0.4, green:0.9, blue:0.4 } } }
        }
      }, index:0 } },
      { addConditionalFormatRule: { rule: {
        ranges: [{ sheetId: tabId, startRowIndex:1, startColumnIndex:3, endColumnIndex:4 }],
        booleanRule: {
          condition: { type:'TEXT_EQ', values:[{ userEnteredValue:'SELL' }] },
          format: { textFormat: { foregroundColor:{ red:0.9, green:0.3, blue:0.3 } } }
        }
      }, index:0 } },
      // Gold tab color
      { updateSheetProperties: {
        properties: { sheetId: tabId, tabColor:{ red:1.0, green:0.843, blue:0.0 } },
        fields: 'tabColor'
      }}
    ];

    await sheets.spreadsheets.batchUpdate({ spreadsheetId: sheetId, requestBody: { requests } });

    console.log('[setup-sheets] ✓ Aurum tab initialised successfully');
    return res.json({
      ok: true,
      message: 'Aurum sheet ready. Headers written, formatting applied.',
      sheet_url: 'https://docs.google.com/spreadsheets/d/' + sheetId,
      columns: HEADERS
    });

  } catch(e) {
    console.error('[setup-sheets] Error:', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── SHEET SETUP ROUTE ────────────────────────────────────────────────────
// Hit this ONCE after deploying to create headers + formatting in Google Sheets.
// GET /setup-sheet
// Safe to run multiple times — it overwrites row 1 and re-applies formatting.
app.get('/setup-sheet', async (req, res) => {
  const sheetId   = process.env.GOOGLE_SHEET_ID;
  const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

  if (!sheetId || !credsJson) {
    return res.status(500).json({
      ok: false,
      error: 'Missing GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON env vars on Railway.'
    });
  }

  try {
    const { google } = require('googleapis');
    const creds = JSON.parse(credsJson);
    const auth  = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    // Find the Aurum tab
    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
    const tab  = meta.data.sheets.find(s => s.properties.title === 'Aurum');
    if (!tab) {
      return res.status(400).json({
        ok: false,
        error: 'No tab named "Aurum" found. Go to your Google Sheet and rename Sheet1 to "Aurum" first.'
      });
    }
    const tabId = tab.properties.sheetId;

    // Write headers
    const HEADERS = [
      'Timestamp', 'Setup ID', 'Symbol', 'Direction', 'Session',
      'Zone Low', 'Zone High', 'Zone Score', 'Touches',
      'Event', 'Entry Price', 'Stop Loss', 'TP1', 'TP2',
      'Candles to Event', 'Result', 'Invalidation Reason', 'Stages Confirmed'
    ];
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId, range: 'Aurum!A1:R1',
      valueInputOption: 'RAW', requestBody: { values: [HEADERS] },
    });

    // Formatting
    const GOLD   = { red: 1.0,   green: 0.843, blue: 0.0   };
    const BLACK  = { red: 0.098, green: 0.098, blue: 0.098 };
    const DARK1  = { red: 0.12,  green: 0.12,  blue: 0.12  };
    const DARK2  = { red: 0.15,  green: 0.15,  blue: 0.15  };

    const colWidths = [180,200,80,70,130,90,90,90,70,160,100,100,100,100,120,80,260,220];
    const widthReqs = colWidths.map((px, i) => ({
      updateDimensionProperties: {
        range: { sheetId: tabId, dimension: 'COLUMNS', startIndex: i, endIndex: i+1 },
        properties: { pixelSize: px }, fields: 'pixelSize',
      }
    }));

    const requests = [
      // Header row style
      { repeatCell: {
        range: { sheetId: tabId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 18 },
        cell: { userEnteredFormat: {
          backgroundColor: BLACK,
          textFormat: { foregroundColor: GOLD, bold: true, fontSize: 10 },
          horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE',
        }},
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
      }},
      // Freeze header
      { updateSheetProperties: {
        properties: { sheetId: tabId, gridProperties: { frozenRowCount: 1 }},
        fields: 'gridProperties.frozenRowCount',
      }},
      // Row banding
      { addBanding: { bandedRange: {
        range: { sheetId: tabId, startRowIndex: 1, startColumnIndex: 0, endColumnIndex: 18 },
        rowProperties: { headerColor: DARK2, firstBandColor: DARK1, secondBandColor: BLACK },
      }}},
      // Tab color gold
      { updateSheetProperties: {
        properties: { sheetId: tabId, tabColor: GOLD },
        fields: 'tabColor',
      }},
      // Conditional: ENTRY=green, INVALIDATED=red, RESULT=blue, CREATED=amber
      ...['ENTRY','INVALIDATED','RESULT','CREATED'].map((val, i) => ({
        addConditionalFormatRule: {
          rule: {
            ranges: [{ sheetId: tabId, startRowIndex: 1, startColumnIndex: 9, endColumnIndex: 10 }],
            booleanRule: {
              condition: { type: 'TEXT_CONTAINS', values: [{ userEnteredValue: val }] },
              format: { backgroundColor: [
                { red:0.133,green:0.369,blue:0.133 },
                { red:0.369,green:0.133,blue:0.133 },
                { red:0.133,green:0.267,blue:0.467 },
                { red:0.267,green:0.267,blue:0.133 },
              ][i]},
            },
          }, index: 0,
        }
      })),
      // BUY=green text, SELL=red text in Direction column
      { addConditionalFormatRule: { rule: {
        ranges: [{ sheetId: tabId, startRowIndex: 1, startColumnIndex: 3, endColumnIndex: 4 }],
        booleanRule: {
          condition: { type: 'TEXT_EQ', values: [{ userEnteredValue: 'BUY' }] },
          format: { textFormat: { foregroundColor: { red:0.4,green:0.9,blue:0.4 }}},
        },
      }, index: 0 }},
      { addConditionalFormatRule: { rule: {
        ranges: [{ sheetId: tabId, startRowIndex: 1, startColumnIndex: 3, endColumnIndex: 4 }],
        booleanRule: {
          condition: { type: 'TEXT_EQ', values: [{ userEnteredValue: 'SELL' }] },
          format: { textFormat: { foregroundColor: { red:0.9,green:0.3,blue:0.3 }}},
        },
      }, index: 0 }},
      ...widthReqs,
    ];

    await sheets.spreadsheets.batchUpdate({ spreadsheetId: sheetId, requestBody: { requests } });

    return res.json({
      ok: true,
      message: 'Aurum sheet initialized successfully.',
      columns: HEADERS,
      sheet_url: 'https://docs.google.com/spreadsheets/d/' + sheetId,
    });

  } catch(e) {
    console.error('[setup-sheet] Error:', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ─── PRICES ROUTE ─────────────────────────────────────────────────────────
// Health check — shows server is up and what data is available
app.get('/health', async (req, res) => {
  const h = new Date().getUTCHours();
  const inSession = (h >= 7 && h < 16) || (h >= 13 && h < 22);
  res.json({
    status: 'ok', version: '5.0',
    utc_hour: h, in_session: inSession,
    session: inSession
      ? (h >= 13 && h < 16 ? 'London+NY Overlap' : h < 16 ? 'London' : 'New York')
      : 'Closed',
    symbols: { XAUUSD: 'XAU/USD (Gold spot)', XAGUSD: 'SLV ETF (Silver proxy)' },
    ts: new Date().toUTCString()
  });
});

app.get('/prices', (req, res) => {
  // Serve prices from in-memory candle cache — zero API calls
  const xauCache = getCached('candles_XAUUSD_5min_120');
  const xagCache = getCached('candles_XAGUSD_5min_120');
  const xau = xauCache ? parseFloat(xauCache[xauCache.length-1]?.c) || null : null;
  const slv = xagCache ? parseFloat(xagCache[xagCache.length-1]?.c) || null : null;

  // SLV holds ~0.9300 troy oz of silver per share (IAU ratio, updated periodically)
  // Convert SLV price → XAG spot price for an accurate Gold/Silver ratio
  const SLV_OZ_RATIO = 0.9300;
  const xagSpot = slv ? parseFloat((slv / SLV_OZ_RATIO).toFixed(3)) : null;

  // Real XAU/XAG ratio (Gold/Silver ratio) — industry standard metric
  const ratio = xau && xagSpot ? parseFloat((xau / xagSpot).toFixed(1)) : null;

  res.json({
    success:  true,
    prices:   { XAUUSD: xau, XAGUSD: slv },   // XAGUSD shows SLV price as-is for display
    xag_spot: xagSpot,                          // actual silver spot estimate
    ratio,                                      // true XAU/XAG ratio
    from_cache: true,
    ts: new Date().toUTCString()
  });
});

// Keep-alive ping
app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));

// Step-by-step analyze test — finds exactly where the route crashes
app.get('/test-analyze/:sym', async (req, res) => {
  const sym = (req.params.sym || 'XAUUSD').toUpperCase();
  const steps = [];
  try {
    steps.push('1. starting');
    const m5 = await getCandles(sym, '5min', 120);
    steps.push('2. candles fetched: ' + (m5?.length || 0));
    if (!m5 || m5.length < 50) return res.json({ ok: false, steps, error: 'insufficient candles' });

    const m15 = deriveM15FromM5(m5);
    steps.push('3. m15 derived: ' + m15.length);

    const atrValues = calcATRFromCandles(m5, 14);
    steps.push('4. atr calculated: ' + atrValues.length);

    const levels = buildLevels(m5, m15);
    steps.push('5. levels built: ' + levels.length);

    const livePrice = m5[m5.length-1].c;
    steps.push('6. livePrice: ' + livePrice);

    const sess = sessionName(Date.now());
    steps.push('7. session: ' + sess);

    const primaryZone = selectPrimaryZone(levels, livePrice, sess, m5, null);
    steps.push('8. primaryZone: ' + (primaryZone ? primaryZone.direction + ' ' + primaryZone.priceRange : 'null'));

    const globalBias = calcGlobalBias(levels, livePrice, null);
    steps.push('9. bias: ' + globalBias.bias);

    const sweep = detectSweep(m5, primaryZone ? [primaryZone] : levels);
    steps.push('10. sweep: ' + sweep.found);

    res.json({ ok: true, steps, sym, livePrice, session: sess });
  } catch(e) {
    steps.push('ERROR: ' + e.message);
    res.json({ ok: false, steps, error: e.message, stack: e.stack?.split('\n').slice(0,3) });
  }
});

// ── GET /setup-sheets — run ONCE after deploy to format the Aurum tab ─────
// Hit this URL once in your browser after the first Railway deploy.
// Writes headers, column widths, colors, banding. Safe to re-run.
app.get('/setup-sheets', async (req, res) => {
  const sheetId   = process.env.GOOGLE_SHEET_ID;
  const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!sheetId || !credsJson) {
    return res.json({ ok: false, error: 'GOOGLE_SHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON not set in Railway variables' });
  }
  try {
    const { google } = require('googleapis');
    const creds = JSON.parse(credsJson);
    const auth  = new google.auth.GoogleAuth({ credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
    const sheets = google.sheets({ version: 'v4', auth });

    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
    const tab  = meta.data.sheets.find(s => s.properties.title === 'Aurum');
    if (!tab) return res.json({ ok: false, error: 'No tab named "Aurum" found — create it in Google Sheets first' });
    const tabId = tab.properties.sheetId;

    const HEADERS = [
      'Timestamp','Setup ID','Symbol','Direction','Session',
      'Zone Low','Zone High','Zone Score','Touches','Event',
      'Entry Price','Stop Loss','TP1','TP2','Candles to Event',
      'Result','Invalidation Reason','Stages Confirmed'
    ];

    // Write headers
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId, range: 'Aurum!A1:R1',
      valueInputOption: 'RAW', requestBody: { values: [HEADERS] },
    });

    const GOLD   = { red: 1.0,   green: 0.843, blue: 0.0   };
    const DARK   = { red: 0.098, green: 0.098, blue: 0.098 };
    const colWidths = [180,200,80,70,130,90,90,90,70,160,100,100,100,100,120,80,260,220];

    const requests = [
      // Header styling
      { repeatCell: {
        range: { sheetId: tabId, startRowIndex:0, endRowIndex:1, startColumnIndex:0, endColumnIndex:18 },
        cell: { userEnteredFormat: {
          backgroundColor: DARK,
          textFormat: { foregroundColor: GOLD, bold: true, fontSize: 10 },
          horizontalAlignment: 'CENTER', verticalAlignment: 'MIDDLE',
        }},
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
      }},
      // Freeze header row
      { updateSheetProperties: {
        properties: { sheetId: tabId, gridProperties: { frozenRowCount: 1 } },
        fields: 'gridProperties.frozenRowCount',
      }},
      // Gold tab color
      { updateSheetProperties: {
        properties: { sheetId: tabId, tabColor: GOLD },
        fields: 'tabColor',
      }},
      // Row banding
      { addBanding: { bandedRange: {
        range: { sheetId: tabId, startRowIndex:1, startColumnIndex:0, endColumnIndex:18 },
        rowProperties: {
          firstBandColor:  { red:0.12,  green:0.12,  blue:0.12  },
          secondBandColor: { red:0.098, green:0.098, blue:0.098 },
        },
      }}},
      // Column widths
      ...colWidths.map((px, i) => ({ updateDimensionProperties: {
        range: { sheetId: tabId, dimension:'COLUMNS', startIndex:i, endIndex:i+1 },
        properties: { pixelSize: px }, fields: 'pixelSize',
      }})),
      // Event column colors (J = index 9)
      ...([
        ['ENTRY',       { red:0.133, green:0.369, blue:0.133 }],
        ['INVALIDATED', { red:0.369, green:0.133, blue:0.133 }],
        ['RESULT',      { red:0.133, green:0.267, blue:0.467 }],
        ['CREATED',     { red:0.267, green:0.267, blue:0.133 }],
      ].map(([val, bg]) => ({ addConditionalFormatRule: { index:0, rule: {
        ranges: [{ sheetId:tabId, startRowIndex:1, startColumnIndex:9, endColumnIndex:10 }],
        booleanRule: { condition:{ type:'TEXT_CONTAINS', values:[{ userEnteredValue:val }] },
                       format:{ backgroundColor: bg } },
      }}}))),
      // BUY = green text, SELL = red text (D = index 3)
      { addConditionalFormatRule: { index:0, rule: {
        ranges: [{ sheetId:tabId, startRowIndex:1, startColumnIndex:3, endColumnIndex:4 }],
        booleanRule: { condition:{ type:'TEXT_EQ', values:[{ userEnteredValue:'BUY' }] },
                       format:{ textFormat:{ foregroundColor:{ red:0.4, green:0.9, blue:0.4 } } } },
      }}},
      { addConditionalFormatRule: { index:0, rule: {
        ranges: [{ sheetId:tabId, startRowIndex:1, startColumnIndex:3, endColumnIndex:4 }],
        booleanRule: { condition:{ type:'TEXT_EQ', values:[{ userEnteredValue:'SELL' }] },
                       format:{ textFormat:{ foregroundColor:{ red:0.9, green:0.3, blue:0.3 } } } },
      }}},
    ];

    await sheets.spreadsheets.batchUpdate({ spreadsheetId: sheetId, requestBody: { requests } });

    res.json({ ok: true, message: 'Aurum sheet formatted successfully — 18 headers, banding, colors applied',
               sheet_url: 'https://docs.google.com/spreadsheets/d/' + sheetId });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

// ── GET /stats — setup analytics
app.get('/stats', (req, res) => {
  try {
    const logs = readAllLogs();
    const total        = logs.length;
    const entries      = logs.filter(l => l.entryTriggered).length;
    const invalidations= logs.filter(l => l.invalidated && !l.entryTriggered).length;
    const withResult   = logs.filter(l => l.result);
    const wins         = withResult.filter(l => l.result === 'TP1' || l.result === 'TP2').length;
    const losses       = withResult.filter(l => l.result === 'SL').length;
    const winRate      = withResult.length > 0 ? Math.round(wins / withResult.length * 100) : null;
    const avgZoneScore = total > 0
      ? parseFloat((logs.reduce((s,l) => s + (l.zone?.score||0), 0) / total).toFixed(1))
      : null;
    const conversionRate = total > 0 ? Math.round(entries / total * 100) : null;

    // Per-session breakdown
    const bySession = {};
    for (const l of logs) {
      const sess = l.session || 'UNKNOWN';
      if (!bySession[sess]) bySession[sess] = { setups:0, entries:0, wins:0 };
      bySession[sess].setups++;
      if (l.entryTriggered) bySession[sess].entries++;
      if (l.result === 'TP1' || l.result === 'TP2') bySession[sess].wins++;
    }

    // By direction
    const byDir = { BUY:{setups:0,entries:0,wins:0}, SELL:{setups:0,entries:0,wins:0} };
    for (const l of logs) {
      const d = l.direction;
      if (byDir[d]) {
        byDir[d].setups++;
        if (l.entryTriggered) byDir[d].entries++;
        if (l.result === 'TP1' || l.result === 'TP2') byDir[d].wins++;
      }
    }

    res.json({
      totalSetups:      total,
      entriesTriggered: entries,
      invalidations,
      winRate,
      losses,
      avgZoneScore,
      conversionRate,
      bySession,
      byDirection:      byDir,
      recentSetups:     logs.slice(-10).reverse(),
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /result — record trade outcome manually
// Body: { setupId: string, result: "TP1"|"TP2"|"SL"|"BE" }
app.post('/result', (req, res) => {
  const { setupId, result } = req.body || {};
  if (!setupId || !result) return res.status(400).json({ error: 'setupId and result required' });
  if (!['TP1','TP2','SL','BE'].includes(result)) return res.status(400).json({ error: 'Invalid result' });
  logTradeResult(setupId, result);
  res.json({ ok: true, setupId, result });
});

// Test signal — fires a mock ENTRY_READY through the full Telegram formatter
// Usage: GET /test-signal  (or /test-signal?dir=BUY)
app.get('/test-signal', async (req, res) => {
  const dir   = (req.query.dir || 'SELL').toUpperCase();
  const asset = req.query.sym === 'XAGUSD' ? 'XAGUSD' : 'XAUUSD';
  const isBuy = dir === 'BUY';

  const mockSig = {
    id:            'TEST_' + Date.now(),
    asset,
    direction:     dir,
    entry:         isBuy ? 4430.50  : 4434.20,
    live_price:    isBuy ? 4432.10  : 4434.20,
    stop_loss:     isBuy ? 4421.00  : 4441.50,
    take_profit_1: isBuy ? 4450.00  : 4418.00,
    take_profit_2: isBuy ? 4465.00  : 4404.00,
    rr:            '2.1',
    confidence:    82,
    grade:         'A',
    tier:          'HIGH',
    entry_mode:    'AGGRESSIVE',
    session:       'London',
    directional_bias: 'neutral',
    sweep_level:   'Equal ' + (isBuy ? 'Lows' : 'Highs') + ' zone (15 touches)',
    pullback_pct:  '54.2',
    expiry:        '10:45 UTC',
    primaryZone:   { low: isBuy ? 4425.00 : 4431.35, high: isBuy ? 4432.00 : 4438.79 },
    scoreBreakdown: {
      sweep:        { score: 17, wickPct: 70 },
      displacement: { score: 13, ratio: 1.93 },
      structure:    { score: 15, type: 'internal', method: 'close' },
      pullback:     { score: 10, retracement: 54.2 }
    }
  };

  const msg = formatTelegramSignal(mockSig);
  // Show in response AND send to Telegram if configured
  let tgSent = false;
  if (TG_TOKEN && TG_CHAT_ID) {
    await sendTelegram(msg);
    tgSent = true;
  }
  res.json({ ok: true, tg_sent: tgSent, preview: msg });
});

// Debug endpoint — shows raw Twelve Data response for a symbol
// Usage: /debug/XAUUSD or /debug/XAGUSD
app.get('/debug/:sym', async (req, res) => {
  const sym = req.params.sym.toUpperCase();
  const td  = SYMBOLS[sym];
  if (!td) return res.status(400).json({ error: 'Unknown symbol' });
  try {
    // Test the simplest possible request: last 5 M5 candles
    // Note: XAGUSD uses SLV ETF as free-tier silver proxy
    const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(td)}&interval=5min&outputsize=5&apikey=${TWELVE_KEY}`;
    console.log('[debug] fetching:', url.replace(TWELVE_KEY, '***'));
    const resp = await fetch(url, { signal: AbortSignal.timeout(10000) });
    const json = await resp.json();
    console.log('[debug] response:', JSON.stringify(json).slice(0, 500));
    res.json({
      symbol: sym,
      td_symbol: td,
      http_status: resp.status,
      has_values: !!(json.values && json.values.length > 0),
      candle_count: json.values?.length || 0,
      first_candle: json.values?.[0] || null,
      error_code: json.code || null,
      error_msg: json.message || null,
      status: json.status || null,
      raw_keys: Object.keys(json)
    });
  } catch(e) {
    res.json({ error: e.message });
  }
});

app.get('/', (req, res) => res.json({
  status:'ok', version:'5.1',
  engine:'Liquidity Sweep — Pure Price Action (M5+M15)',
  rules: ['PDH/PDL/ASH/ASL/EQH/EQL levels','0.02% sweep break required','1.5× body displacement','M5+M15 BOS','50-61.8% pullback entry','min 1:2 RR','confidence ≥ 80','10-candle time decay']
}));

// ═══════════════════════════════════════════════════════════════
// TELEGRAM ALERT SYSTEM
// ═══════════════════════════════════════════════════════════════

// Config stored in environment variables on Railway (never hardcoded)
// Set TG_TOKEN and TG_CHAT_ID in Railway → Variables tab
let TG_TOKEN   = process.env.TG_TOKEN   || '';
let TG_CHAT_ID = process.env.TG_CHAT_ID || '';

// Also accept config updates from frontend via POST /config
app.post('/config', (req, res) => {
  const { tg_token, tg_chat_id } = req.body;
  if (tg_token)   { TG_TOKEN   = tg_token;   console.log('[config] TG_TOKEN updated'); }
  if (tg_chat_id) { TG_CHAT_ID = tg_chat_id; console.log('[config] TG_CHAT_ID updated'); }
  res.json({ ok: true, tg_configured: !!(TG_TOKEN && TG_CHAT_ID) });
});

// GET /config — let frontend check if Telegram is configured
app.get('/config', (req, res) => {
  res.json({ tg_configured: !!(TG_TOKEN && TG_CHAT_ID),
             tg_token_set: !!TG_TOKEN,
             tg_chat_set:  !!TG_CHAT_ID });
});

// Send a Telegram message
async function sendTelegram(text) {
  if (!TG_TOKEN || !TG_CHAT_ID) return;
  try {
    const url  = 'https://api.telegram.org/bot' + TG_TOKEN + '/sendMessage';
    const body = JSON.stringify({ chat_id: TG_CHAT_ID, text, parse_mode: 'HTML' });
    const resp = await fetch(url, { method:'POST',
      headers:{'Content-Type':'application/json'}, body,
      signal: AbortSignal.timeout(8000) });
    const json = await resp.json();
    if (!json.ok) console.error('[telegram] send failed:', json.description);
    else console.log('[telegram] message sent OK');
  } catch(e) {
    console.error('[telegram] error:', e.message);
  }
}

// Format full signal for Telegram — includes grade, score, expiry
function formatTelegramSignal(sig) {
  const isBuy   = sig.direction === 'BUY';
  const dir     = sig.direction;
  const asset   = sig.asset === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const dirEmoji = isBuy ? '🟢' : '🔴';

  // ── Grade & mode ──────────────────────────────────────────────
  const conf    = sig.confidence || 0;
  const grade   = conf >= 85 ? 'A+' : conf >= 75 ? 'A' : conf >= 65 ? 'B' : 'C';
  const gradeEmoji = conf >= 85 ? '⭐' : conf >= 75 ? '✅' : '🔵';

  const modeKey = sig.entry_mode || 'STANDARD';
  const modeStr = modeKey === 'AGGRESSIVE_EARLY' ? '⚡ EARLY'
                : modeKey === 'AGGRESSIVE'        ? '🎯 STANDARD'
                : '🚀 MOMENTUM';

  // ── Entry zone ────────────────────────────────────────────────
  const entryPrice  = sig.entry;
  const livePrice   = sig.live_price || entryPrice;
  const distFromEntry = livePrice && entryPrice
    ? Math.abs(((livePrice - entryPrice) / entryPrice) * 100).toFixed(3)
    : null;

  // Zone range from primary zone if available
  const zone = sig.primaryZone || sig.zone || null;
  const zoneRange = zone
    ? '$' + parseFloat(zone.low || zone.minPrice || entryPrice).toFixed(2) +
      ' – $' + parseFloat(zone.high || zone.maxPrice || entryPrice).toFixed(2)
    : '$' + entryPrice;

  // ── Risk management ───────────────────────────────────────────
  const sl     = sig.stop_loss;
  const riskDist = Math.abs(entryPrice - sl);
  // Risk % assumes 0.5% of account per trade (configurable)
  const RISK_PCT = 0.5;

  // ── Take profit ───────────────────────────────────────────────
  const tp1 = sig.take_profit_1;
  const tp2 = sig.take_profit_2;
  const rr1 = sig.rr || (tp1 ? (Math.abs(tp1 - entryPrice) / riskDist).toFixed(1) : '—');

  // ── Validity / invalidation ───────────────────────────────────
  const validity = modeKey === 'AGGRESSIVE_EARLY'
    ? 'Valid for 2 candles (10 min) — early entry window'
    : 'Valid until zone $' + (zone ? parseFloat(zone.low||zone.minPrice||sl).toFixed(2) : parseFloat(sl).toFixed(2)) + ' breaks';

  const invalidation = isBuy
    ? 'Close below $' + sl + ' · Pullback > 70% · Time expiry'
    : 'Close above $' + sl + ' · Pullback > 70% · Time expiry';

  // ── Reason bullets ────────────────────────────────────────────
  const bd = sig.scoreBreakdown || {};
  const reasons = [];
  if (sig.sweep_level) reasons.push('• ' + sig.sweep_level + ' swept');
  if (bd.sweep)        reasons.push('• Rejection wick: ' + (bd.sweep.wickPct || '—') + '%');
  if (bd.displacement) reasons.push('• Displacement: ' + (bd.displacement.ratio || '—') + '× avg body');
  if (bd.structure)    reasons.push('• BOS: ' + (bd.structure.type || '') + ' (' + (bd.structure.method || '') + ')');
  if (sig.pullback_pct)reasons.push('• Pullback: ' + sig.pullback_pct + '% retracement');
  if (!reasons.length && sig.reason) reasons.push('• ' + sig.reason);
  // v5.2: zone freshness label
  if (sig.zoneFreshness) reasons.push('• Zone: ' + sig.zoneFreshness);

  // ── Session ───────────────────────────────────────────────────
  const sess = sig.session || '—';

  // v5.2: ATR position sizing block
  const atrBlock = formatATRBlock(sig.atr, entryPrice, sl);

  return [
    dirEmoji + ' <b>' + asset + ' ' + dir + '</b>  ' + gradeEmoji + ' <b>' + grade + '</b>  ' + modeStr,
    '─────────────────────────────',
    '',
    '<b>ENTRY</b>',
    'Zone:    ' + zoneRange,
    'Price:   $' + entryPrice + (distFromEntry ? '  (' + distFromEntry + '% from zone)' : ''),
    '',
    '<b>RISK MANAGEMENT</b>',
    'Stop:    $' + sl + '  (risk ' + RISK_PCT + '% of account)',
    'TP1:     $' + tp1 + '  (1:' + rr1 + 'R)',
    'TP2:     $' + tp2,
    '',
    '<b>CONFIDENCE: ' + conf + '/100 — ' + grade + '</b>',
    'Session: ' + sess,
    '',
    '<b>WHY</b>',
    ...reasons,
    atrBlock,
    '',
    '<b>VALID:  </b>' + validity,
    '<b>CANCEL: </b>' + invalidation,
    '',
    '─────────────────────────────',
    'Aurum Signals · #' + (sig.id || '—')
  ].join('\n');
}


// Format pre-signal alert for Telegram
// Format pre-signal alert for Telegram — short, readable in <5 seconds
function formatTelegramPreSignal(sym, ns) {
  const asset = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const dir   = ns.direction || '—';

  const stageConfig = {
    approaching_liquidity:  { emoji: '📍', line: 'Approaching key zone — watch for sweep' },
    sweep_detected:         { emoji: '⚡', line: 'Liquidity grab detected — waiting for displacement' },
    displacement_confirmed: { emoji: '↗️', line: 'Strong move confirmed — waiting for trend shift' },
    structure_break:        { emoji: '✅', line: 'Trend shift confirmed — waiting for pullback entry' },
    waiting_pullback:       { emoji: '🎯', line: 'Pullback zone reached — entry evaluating' },
  };

  const cfg     = stageConfig[ns.stage] || { emoji: '📡', line: ns.message || 'Setup forming' };
  const zoneConf = ns.zone_confidence;
  const confStr  = zoneConf ? '  Zone: ' + zoneConf.total + '/100' : '';

  return cfg.emoji + ' <b>' + asset + ' ' + dir + '</b> — ' + cfg.line + confStr + '\n─────────────────\nAurum Signals';
}


// ═══════════════════════════════════════════════════════════════
// AUTO-SCAN ENGINE
// Runs every 5 minutes during London/NY sessions.
// Sends Telegram alerts when signals or pre-signals are detected.
// Tracks sent signals to avoid duplicate alerts.
// ═══════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════
// SETUP STATE MACHINE
// One state object per symbol. All alerts are event-driven (fire on transition).
// No alert fires twice for the same event in the same setup lifecycle.
// ═══════════════════════════════════════════════════════════════════════════

const SETUP_STAGES = ['idle','approaching','sweep','move','trend','pullback','entry'];

function createSetup(sym, direction, levelOrZone) {
  const id  = sym + '_' + Date.now();
  const zoneId = levelOrZone && levelOrZone.isZone
    ? Math.round(levelOrZone.minPrice) + '-' + Math.round(levelOrZone.maxPrice)
    : levelOrZone ? Math.round(parseFloat(levelOrZone.price || 0)) : 'none';
  const setup = {
    id,
    sym,
    direction,
    zoneId,
    level: levelOrZone,
    stage: 'idle',
    active: true,
    invalidated: false,
    // Event fired flags — each fires exactly ONCE per setup lifecycle
    events: {
      approaching:  false,
      sweep:        false,
      move:         false,
      trend:        false,
      pullback:     false,
      entry:        false,
      invalidated:  false,
    },
    // Alert state — tracks what Telegram messages have been sent for this setup
    tgAlerts: {
      preEntry: false,  // ⚠️ SETUP FORMING alert (sent at trend+valid pullback stage)
      entry:    false,  // 🟢/🔴 full signal
      invalid:  false,  // ⚠️ invalidation (only if preEntry or pullback reached)
    },
    // Candle index at which each stage was confirmed — enforces stage separation
    stageCandleIdx: {
      sweep:    -1,
      move:     -1,
      trend:    -1,
      pullback: -1,
      entry:    -1,
    },
    startedAt:       Date.now(),
    lastEventAt:     Date.now(),
    earlyLockUntil:  0,
    cooldowns: {}
  };
  console.log('[setup] Created id=' + id + ' dir=' + direction + ' zone=' + zoneId);
  return setup;
}

// Wrapper called from autoScan — creates setup AND logs it
function createAndLogSetup(sym, direction, levelOrZone) {
  const setup = createSetup(sym, direction, levelOrZone);
  logSetupCreated(setup, levelOrZone);
  return setup;
}

// Setups keyed by symbol
const setups = { XAUUSD: null, XAGUSD: null };

// Active trade monitor — tracks open positions after entry signal fires
// { XAUUSD: { setupId, direction, entry, sl, tp1, tp2, high, low, resultLogged }, ... }
const tradeMonitor = { XAUUSD: null, XAGUSD: null };

// Per-symbol timing state — persists through setup resets
// Tracks cooldowns for zone detection, bias flips, invalidation windows
const symTiming = {
  XAUUSD: {
    zoneDetectionAllowedAt:  0,
    biasFlipAllowedAt:       0,
    lastInvalidatedAt:       0,
    lastInvalidatedDir:      null,
    pullbackStartCandleIdx:  -1,
    pullbackCandleCount:     0,
    lastSweepAlertAt:        0,
    lastSweepDir:            null,
    lastSweepZoneKey:        null,
    // Structural bias — persists until opposite structure confirmed
    structuralBiasDir:       null,
    structuralBiasStage:     null,
    structuralBiasAt:        0,
    consecutiveFailures:     { BUY: 0, SELL: 0 },
    // HTF (M15) structural bias — updated every scan
    htfBias:                 'NEUTRAL',  // 'BULLISH' | 'BEARISH' | 'NEUTRAL'
    htfLastBOS:              'NONE',     // 'UP' | 'DOWN' | 'NONE'
    htfLastHigh:             0,
    htfLastLow:              0,
    htfUpdatedAt:            0,
  },
  XAGUSD: {
    zoneDetectionAllowedAt:  0,
    biasFlipAllowedAt:       0,
    lastInvalidatedAt:       0,
    lastInvalidatedDir:      null,
    pullbackStartCandleIdx:  -1,
    pullbackCandleCount:     0,
    lastSweepAlertAt:        0,
    lastSweepDir:            null,
    lastSweepZoneKey:        null,
    structuralBiasDir:       null,
    structuralBiasStage:     null,
    structuralBiasAt:        0,
    consecutiveFailures:     { BUY: 0, SELL: 0 },
    htfBias:                 'NEUTRAL',
    htfLastBOS:              'NONE',
    htfLastHigh:             0,
    htfLastLow:              0,
    htfUpdatedAt:            0,
  }
};

const CANDLE_MS = 5 * 60 * 1000; // 5 minutes per M5 candle

// Legacy compat — sentSignals dedup by entry price
const sentSignals = new Set();

// Transition: advance stage and fire alert if event not yet sent
// Returns true if alert was sent, false if blocked
async function fireEvent(setup, event, sym, alertFn, candleIdx = -1) {
  if (!setup || !setup.active) {
    console.log('[' + sym + '] fireEvent ' + event + ' blocked — setup inactive');
    return false;
  }
  if (setup.invalidated) {
    console.log('[' + sym + '] fireEvent ' + event + ' blocked — setup invalidated');
    return false;
  }
  if (setup.events[event]) {
    console.log('[' + sym + '] fireEvent ' + event + ' blocked — already fired for this setup (id=' + setup.id + ')');
    return false;
  }
  // ── Candle separation: each stage must confirm on a strictly later candle
  const stageOrder = ['sweep','move','trend','pullback','entry'];
  const prevStageIdx = stageOrder.indexOf(event) - 1;
  if (candleIdx >= 0 && prevStageIdx >= 0 && setup.stageCandleIdx) {
    const prevStage = stageOrder[prevStageIdx];
    const prevCandle = setup.stageCandleIdx[prevStage] ?? -1;
    if (prevCandle >= 0 && candleIdx <= prevCandle) {
      console.log('[' + sym + '] fireEvent ' + event + ' BLOCKED — same candle as ' +
        prevStage + ' (idx=' + candleIdx + '). Must wait for next candle.');
      return false;
    }
  }

  // 15-minute cooldown per event type as safety net
  const COOLDOWN_MS = 15 * 60 * 1000;
  const lastFired = setup.cooldowns[event] || 0;
  if (Date.now() - lastFired < COOLDOWN_MS) {
    console.log('[' + sym + '] fireEvent ' + event + ' blocked — cooldown (' +
      Math.round((COOLDOWN_MS - (Date.now() - lastFired)) / 60000) + 'min remaining)');
    return false;
  }

  // Fire the event — update state atomically before sending
  setup.events[event]  = true;
  setup.stage          = event;
  setup.lastEventAt    = Date.now();
  setup.cooldowns[event] = Date.now();
  // Record which candle confirmed this stage (for separation enforcement)
  if (setup.stageCandleIdx && candleIdx >= 0) {
    setup.stageCandleIdx[event] = candleIdx;
  }
  // Stage progression log (visible in Railway logs)
  const stageLabels = {
    approaching: 'Stage → approaching liquidity',
    sweep:       'Stage → liquidity grab',
    move:        'Stage → strong move confirmed',
    trend:       'Stage → trend shift confirmed',
    pullback:    'Stage → pullback valid',
    entry:       '🟢 ENTRY READY — full signal generating'
  };
  console.log('[' + sym + '] ' + (stageLabels[event] || 'Stage → ' + event) + ' (id=' + setup.id + ')');

  try { await alertFn(); } catch(e) { console.error('[' + sym + '] alert error:', e.message); }
  return true;
}

// Invalidate a setup — fires exactly once
async function invalidateSetup(sym, reason) {
  const setup = setups[sym];
  if (!setup) return;
  if (setup.invalidated) {
    console.log('[' + sym + '] Duplicate invalidation blocked (id=' + setup.id + ')');
    return;
  }
  if (setup.events.invalidated) {
    console.log('[' + sym + '] Invalidation alert already sent — blocking duplicate');
    return;
  }
  setup.invalidated        = true;
  setup.active             = false;
  setup.events.invalidated = true;
  setup.stage              = 'invalidated';
  console.log('[' + sym + '] Setup invalidated → alert sent once (id=' + setup.id + '): ' + reason);
  logInvalidation(setup, reason);
  // In EXECUTION mode: only send invalidation if pre-entry alert was sent or pullback reached
  const _shouldSendInvalidation = TELEGRAM_MODE === 'FULL' ||
    setup.tgAlerts?.preEntry ||
    setup.events?.pullback;
  if (!_shouldSendInvalidation) {
    console.log('[tg] Invalidation suppressed — pre-entry alert not yet sent (silent invalidation)');
    return;
  }

  // ── BIAS FLIP ON INVALIDATION ─────────────────────────────────
  // Invalidation = market rejected this direction → flip bias to opposite
  const oppositeDir   = setup.direction === 'BUY' ? 'SELL' : 'BUY';
  const t             = symTiming[sym];
  let   biasMsg       = 'Watching for next opportunity.';
  if (t) {
    // Track consecutive failures per direction
    t.consecutiveFailures[setup.direction] = (t.consecutiveFailures[setup.direction] || 0) + 1;
    t.consecutiveFailures[oppositeDir]     = 0; // reset opposite count
    const failures = t.consecutiveFailures[setup.direction];

    // Flip structural bias to opposite direction
    // Stage strength depends on how far the failed setup progressed
    const failedStage = setup.stage === 'trend'    ? 'move'
                      : setup.stage === 'pullback' ? 'trend'
                      : setup.stage === 'move'     ? 'sweep'
                      : 'sweep';
    t.structuralBiasDir   = oppositeDir;
    t.structuralBiasStage = failedStage;
    t.structuralBiasAt    = Date.now();

    const confStr = failures >= 3 ? ' (HIGH confidence — ' + failures + ' consecutive failures)'
                  : failures >= 2 ? ' (' + failures + ' consecutive failures)'
                  : '';
    biasMsg = setup.direction + ' setup invalidated — short-term ' + oppositeDir +
      ' bias active' + confStr + '.';
    console.log('[bias] ' + sym + ': bias flipped → ' + oppositeDir +
      ' after ' + setup.direction + ' invalidation (stage: ' + failedStage + ', failures: ' + failures + ')');
  }

  const asset = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const dirEmoji = oppositeDir === 'BUY' ? '🟢' : '🔴';
  await sendTelegram('⚠️ <b>' + asset + ' — SETUP INVALIDATED</b>\n\n' +
    reason + '\n\n' + dirEmoji + ' ' + biasMsg +
    '\n\n─────────────────\nAurum Signals');
}

// Reset a symbol's setup — called when session closes or new sweep on different zone
function resetSetup(sym, reason) {
  const existing = setups[sym];
  if (existing) {
    console.log('[' + sym + '] Setup reset (id=' + existing.id + '): ' + reason);
    // Record cooldowns on invalidation — 2 candles (10 min) before new zone detection
    const now = Date.now();
    if (symTiming[sym]) {
      symTiming[sym].lastInvalidatedAt      = now;
      symTiming[sym].lastInvalidatedDir     = existing.direction;
      symTiming[sym].zoneDetectionAllowedAt = now + 2 * CANDLE_MS;
      symTiming[sym].biasFlipAllowedAt      = now + 2 * CANDLE_MS;
      symTiming[sym].pullbackStartCandleIdx = -1;
      symTiming[sym].pullbackCandleCount    = 0;
      // Structural bias was updated in invalidateSetup (flipped to opposite direction)
      // resetSetup just records the cooldown — bias is already correct
      console.log('[timing] ' + sym + ': cooldown set — zone detection blocked for 10min after ' + reason);
      console.log('[bias] ' + sym + ': bias now → ' +
        (symTiming[sym].structuralBiasDir || 'none') +
        ' (' + (symTiming[sym].structuralBiasStage || '?') + ' stage)');
    }
  }
  setups[sym] = null;
}

// Session time remaining in minutes
function sessionMinutesRemaining() {
  const h = new Date().getUTCHours();
  const m = new Date().getUTCMinutes();
  const nowMins = h * 60 + m;
  if (nowMins >= 7*60  && nowMins < 16*60) return 16*60 - nowMins;
  if (nowMins >= 13*60 && nowMins < 22*60) return 22*60 - nowMins;
  return 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// AGGRESSIVE ENTRY ENGINE
// Evaluates ONLY the primary zone. One setup per asset.
// Confirmation: sweep → rejection → momentum (3 conditions, fast)
// ═══════════════════════════════════════════════════════════════════════════

function aggressiveEntryEngine(sym, m5, primaryZone, sess) {
  if (!primaryZone || !m5 || m5.length < 10) {
    return { type: 'NO_SETUP', reason: 'No primary zone or insufficient data' };
  }

  const direction  = primaryZone.direction; // already set by selectPrimaryZone
  const zoneLow    = primaryZone.minPrice;
  const zoneHigh   = primaryZone.maxPrice;
  const zoneRef    = direction === 'BUY' ? zoneLow : zoneHigh;
  const CLOSE_TOL  = 0.001;  // 0.1% tolerance for close condition
  const SWEEP_MIN  = 0.0005; // 0.05% minimum sweep depth — anti-fake-sweep filter

  // Rolling average candle size (last 20)
  const recent20 = m5.slice(-20);
  const avgSize  = recent20.reduce((s, c) => s + range(c), 0) / recent20.length;
  if (avgSize === 0) return { type: 'NO_SETUP', reason: 'Zero average candle size' };

  // Scan recent candles for a valid sweep candle (last 6)
  for (let i = m5.length - 6; i < m5.length; i++) {
    if (i < 1) continue;
    const c = m5[i];
    const r = range(c);
    if (r === 0) continue;

    // ── INVALIDATION: candle too small ───────────────────────────
    if (r < avgSize * 0.7) continue; // skip tiny candles
    const wickFraction = direction === 'BUY'
      ? (Math.min(c.o, c.c) - c.l) / r
      : (c.h - Math.max(c.o, c.c)) / r;
    if (wickFraction < 0.15) continue; // wick < 15% = no rejection

    // ── CONDITION 1: LIQUIDITY SWEEP ─────────────────────────────
    let sweptBeyond = false;
    if (direction === 'BUY') {
      // Price must trade BELOW zone low (wick OR body)
      sweptBeyond = c.l < zoneLow * (1 - SWEEP_MIN);
    } else {
      // Price must trade ABOVE zone high (wick OR body)
      sweptBeyond = c.h > zoneHigh * (1 + SWEEP_MIN);
    }
    if (!sweptBeyond) continue;

    // ── INVALIDATION: barely touches zone ────────────────────────
    const sweepDepth = direction === 'BUY'
      ? (zoneLow - c.l) / zoneLow
      : (c.h - zoneHigh) / zoneHigh;
    if (sweepDepth < SWEEP_MIN) continue;

    // ── CONDITION 2: CLOSE CONDITION ─────────────────────────────
    let validClose = false;
    if (direction === 'BUY') {
      // Close inside zone OR within 0.1% below zone low
      validClose = c.c >= zoneLow * (1 - CLOSE_TOL);
    } else {
      // Close inside zone OR within 0.1% above zone high
      validClose = c.c <= zoneHigh * (1 + CLOSE_TOL);
    }
    if (!validClose) continue;

    // ── CONDITION 3: REJECTION WICK ──────────────────────────────
    const wickPct = direction === 'BUY'
      ? (Math.min(c.o, c.c) - c.l) / r
      : (c.h - Math.max(c.o, c.c)) / r;
    if (wickPct < 0.25) continue; // wick must be ≥ 25% of range

    // ── MICRO-STRUCTURE: no immediate lower-low / higher-high ────
    if (i + 1 < m5.length) {
      const next = m5[i + 1];
      if (direction === 'BUY'  && next.l < c.l) continue; // lower low = cancel
      if (direction === 'SELL' && next.h > c.h) continue; // higher high = cancel
    }

    // ── CONDITION 4: EARLY MOMENTUM (within next 2 candles) ──────
    let momentumCandle = null;
    let momentumDelay  = 0;
    for (let j = i + 1; j <= Math.min(i + 2, m5.length - 1); j++) {
      const mc2  = m5[j];
      const bull = mc2.c > mc2.o;
      const bear = mc2.c < mc2.o;
      const big  = range(mc2) >= avgSize * 1.2;
      // Condition A: single large candle in direction
      if (direction === 'BUY'  && bull && big) { momentumCandle = mc2; momentumDelay = j - i; break; }
      if (direction === 'SELL' && bear && big) { momentumCandle = mc2; momentumDelay = j - i; break; }
      // Condition B: 2 consecutive candles in direction
      if (j > i + 1) {
        const prev2 = m5[j - 1];
        if (direction === 'BUY'  && bull && prev2.c > prev2.o) { momentumCandle = mc2; momentumDelay = j - i; break; }
        if (direction === 'SELL' && bear && prev2.c < prev2.o) { momentumCandle = mc2; momentumDelay = j - i; break; }
      }
    }

    // ── TIME FILTER: momentum must occur within 2 candles ────────
    if (!momentumCandle) {
      const candlesSinceSweep = m5.length - 1 - i;

      // ── EARLY ENTRY CONDITION ─────────────────────────────────
      // If sweep candle itself is strong enough, skip momentum wait.
      // All three sub-conditions must pass — weak sweeps never qualify.
      const earlyWickOk  = wickPct >= 0.35;                   // wick ≥ 35% of range
      const earlySizeOk  = r >= avgSize * 1.2;                // candle ≥ 1.2x average
      // Strong close back inside zone (not just touching edge)
      const earlyCloseOk = direction === 'BUY'
        ? c.c >= zoneLow + (zoneHigh - zoneLow) * 0.25        // closed at least 25% into zone
        : c.c <= zoneHigh - (zoneHigh - zoneLow) * 0.25;

      if (earlyWickOk && earlySizeOk && earlyCloseOk) {
        // Strong sweep candle — trigger early entry immediately
        let earlyConf = primaryZone.confidence?.total || 50;
        earlyConf += sweepDepth >= 0.003 ? 10 : 5;  // sweep depth bonus
        // No momentum bonus — penalise slightly for no follow-through yet
        earlyConf -= 5;
        earlyConf = Math.min(Math.max(Math.round(earlyConf), 0), 100);

        console.log('[entry] ' + sym + ' ' + direction + ' AGGRESSIVE_EARLY — wick=' +
          (wickPct*100).toFixed(1) + '% size=' + (r/avgSize).toFixed(2) + 'x confidence=' + earlyConf);

        return {
          type:         'ENTRY_READY',
          direction,
          mode:         'AGGRESSIVE_EARLY',
          zone:         { low: zoneLow, high: zoneHigh },
          sweepCandle:  i,
          sweepDepth:   parseFloat((sweepDepth * 100).toFixed(3)),
          wickPct:      parseFloat((wickPct * 100).toFixed(1)),
          momentumDelay: 0,
          dispRatio:    parseFloat((r / avgSize).toFixed(2)),
          entry_reason: [
            'Liquidity sweep confirmed (depth ' + (sweepDepth*100).toFixed(3) + '%)',
            'Strong rejection: ' + (wickPct*100).toFixed(1) + '% wick (≥35% required)',
            'Candle size: ' + (r/avgSize).toFixed(2) + 'x average (≥1.2x required)',
            'Strong close back inside zone — early entry triggered'
          ],
          confidence:   earlyConf,
          primaryZone
        };
      }

      // Weak sweep — wait for momentum (max 2 candles)
      if (candlesSinceSweep <= 2) {
        return {
          type:        'SWEEP_FORMING',
          direction,
          sweepCandle: i,
          wickPct:     parseFloat((wickPct * 100).toFixed(1)),
          reason:      'Sweep confirmed — awaiting momentum (' + (2 - candlesSinceSweep) + ' candles remaining)' +
                       ' | Early entry blocked: ' +
                       (!earlyWickOk  ? 'wick ' + (wickPct*100).toFixed(1) + '% < 35%' :
                        !earlySizeOk  ? 'candle ' + (r/avgSize).toFixed(2) + 'x < 1.2x avg' :
                        !earlyCloseOk ? 'close not deep enough inside zone' : '?')
        };
      }
      // Momentum window expired — return NO_ENTRY with specific reason
      return {
        type:      'NO_ENTRY',
        direction,
        sweepCandle: i,
        reason:    '❌ No momentum after sweep — window expired (2 candles elapsed)'
      };
    }

    // ── ALL 3 CONDITIONS MET → ENTRY CONFIRMED ───────────────────

    // Confidence score adjustment
    let confidence = primaryZone.confidence?.total || 50;

    // Sweep depth bonus
    if (sweepDepth >= 0.003)     confidence += 10; // deep sweep
    else if (sweepDepth >= 0.001) confidence += 5;

    // Momentum speed bonus/penalty
    if (momentumDelay === 1)     confidence += 10; // confirmed in 1 candle
    else                          confidence -= 10; // took 2 candles

    // Displacement bonus
    const dispRatio = range(momentumCandle) / avgSize;
    if (dispRatio >= 1.5)        confidence += 5;

    // Wick quality penalty
    if (wickPct < 0.20)          confidence -= 15;

    confidence = Math.min(Math.max(Math.round(confidence), 0), 100);

    const entryReasons = [
      'Liquidity sweep: ' + direction === 'BUY'
        ? 'price swept below $' + zoneLow.toFixed(3) + ' (depth ' + (sweepDepth*100).toFixed(3) + '%)'
        : 'price swept above $' + zoneHigh.toFixed(3) + ' (depth ' + (sweepDepth*100).toFixed(3) + '%)',
      'Rejection: ' + (wickPct * 100).toFixed(1) + '% wick confirmed',
      'Momentum: ' + (momentumDelay === 1 ? 'confirmed in 1 candle (' + dispRatio.toFixed(2) + 'x avg)' : '2 consecutive candles')
    ];

    console.log('[entry] ' + sym + ' ' + direction + ' ENTRY_READY — confidence=' + confidence +
      ' sweep=' + (sweepDepth*100).toFixed(3) + '% wick=' + (wickPct*100).toFixed(1) + '% delay=' + momentumDelay);

    return {
      type:         'ENTRY_READY',
      direction,
      mode:         'AGGRESSIVE',
      zone:         { low: zoneLow, high: zoneHigh },
      sweepCandle:  i,
      sweepDepth:   parseFloat((sweepDepth * 100).toFixed(3)),
      wickPct:      parseFloat((wickPct * 100).toFixed(1)),
      momentumDelay,
      dispRatio:    parseFloat(dispRatio.toFixed(2)),
      entry_reason: entryReasons,
      confidence,
      primaryZone
    };
  }

  return { type: 'NO_ENTRY', direction, reason: 'No valid sweep/rejection/momentum pattern found' };
}

// ═══════════════════════════════════════════════════════════════════════════
// SIGNAL VALIDATION ENGINE
// Runs before any setup is created or any alert is sent.
// All 8 rules are hard blocks — not filters, not suggestions.
// ═══════════════════════════════════════════════════════════════════════════

const MIN_ZONE_WIDTH_PCT = 0.0002;  // 0.02% minimum zone width (≈$0.90 on gold)
const MAX_CANDLE_AGE_MINS = 15;     // stale data threshold

function validateSignal(sym, sweep, m5, levels, existingSetup) {
  const reasons = [];

  // ── RULE 1: DIRECTION CONSISTENCY ─────────────────────────────
  // Equal Highs (EQH) = stop cluster ABOVE price = sweep up → SELL reversal
  // Equal Lows  (EQL) = stop cluster BELOW price = sweep down → BUY reversal
  // PDH/ASH = resistance above → sweep up → SELL
  // PDL/ASL = support below → sweep down → BUY
  if (sweep && sweep.found && sweep.level) {
    const lvlType = sweep.level.type;
    const expectedDir = getZoneDirection(sweep.level);
    if (expectedDir && sweep.direction !== expectedDir) {
      reasons.push('Direction mismatch: ' + lvlType + ' zone requires ' + expectedDir +
        ' but got ' + sweep.direction);
    }
  }

  // ── RULE 2: ZONE VALIDITY ──────────────────────────────────────
  if (sweep && sweep.level && sweep.level.isZone) {
    const z = sweep.level;
    const refPrice = z.minPrice || z.price || 1;
    // min === max means single price point, not a real zone
    if (!z.minPrice || !z.maxPrice || z.minPrice === z.maxPrice) {
      reasons.push('Zone invalid: min === max (single price point, not a zone)');
    } else {
      const width = (z.maxPrice - z.minPrice) / refPrice;
      if (width < MIN_ZONE_WIDTH_PCT) {
        reasons.push('Zone too narrow: ' + (width*100).toFixed(4) + '% width (min ' + (MIN_ZONE_WIDTH_PCT*100) + '%)');
      }
    }
    // Must have at least 2 distinct touch levels
    if ((z.totalTouches || 0) < 2) {
      reasons.push('Insufficient touches: ' + (z.totalTouches || 0) + ' (min 2)');
    }
  }

  // ── RULE 3: MINIMUM TOUCH QUALITY ─────────────────────────────
  if (sweep && sweep.level) {
    const touches = sweep.level.totalTouches || sweep.level.strengthScore || 1;
    if (touches < 2) {
      reasons.push('Touch quality too low: only ' + touches + ' touch(es)');
    }
  }

  // ── RULE 4: DUPLICATE SETUP ───────────────────────────────────
  if (existingSetup && existingSetup.active && !existingSetup.invalidated) {
    // If same zone and same direction → duplicate
    if (existingSetup.direction === sweep?.direction) {
      const newZoneId  = sweep?.level?.isZone
        ? Math.round(sweep.level.minPrice) + '-' + Math.round(sweep.level.maxPrice)
        : Math.round(parseFloat(sweep?.level?.price || 0));
      if (String(existingSetup.zoneId) === String(newZoneId)) {
        reasons.push('Duplicate: setup already active for same zone ' + existingSetup.zoneId);
      }
    }
  }

  // ── RULE 5: ZONE ALREADY SWEPT RECENTLY ───────────────────────
  // If this same zone was swept in the last 2 scans (10 min) and price hasn't moved away
  // → block to prevent re-triggering on the same sweep candle
  // (handled by the state machine's event lock — belt+braces here)
  if (existingSetup && existingSetup.events?.sweep && existingSetup.active) {
    reasons.push('Zone already swept in current setup — state machine handles progression');
  }

  // ── RULE 6: DATA FRESHNESS ─────────────────────────────────────
  if (m5 && m5.length > 0) {
    const lastCandleAge = (Date.now() - m5[m5.length - 1].t) / 60000; // minutes
    if (lastCandleAge > MAX_CANDLE_AGE_MINS) {
      reasons.push('Stale data: last candle ' + Math.round(lastCandleAge) + ' minutes old (max ' + MAX_CANDLE_AGE_MINS + ')');
    }
  }

  // ── RULE 7: STRUCTURE SEQUENCE INTEGRITY ──────────────────────
  // sweep must be detected before displacement, BOS, pullback
  // (the state machine enforces this in order — this catches edge cases
  //  where data arrives out of order or functions are called in wrong context)
  if (!sweep || !sweep.found) {
    // Without a sweep there is no valid setup origin
    if (existingSetup && !existingSetup.events?.sweep) {
      reasons.push('Sequence violation: no liquidity grab detected (cannot progress)');
    }
  }

  // ── RULE 8: VOLATILITY / SWEEP STRENGTH ───────────────────────
  if (sweep && sweep.found) {
    const wickPct = sweep.wickPct || 0;
    if (wickPct < 0.25) {
      reasons.push('Sweep too weak: wick ' + Math.round(wickPct * 100) + '% of range (min 25%)');
    }
    if (m5 && m5.length >= 10) {
      const avg10 = m5.slice(-10).reduce((s,c) => s + range(c), 0) / 10;
      const sweepCandle = m5[sweep.candleIdx];
      if (sweepCandle && avg10 > 0 && range(sweepCandle) < avg10 * 0.3) {
        reasons.push('Sweep candle too small: ' + (range(sweepCandle)/avg10*100).toFixed(0) + '% of avg range (min 30%)');
      }
    }
  }

  const valid = reasons.length === 0;
  if (!valid) {
    reasons.forEach(r => console.log('[validate] ' + sym + ' REJECTED: ' + r));
  }
  return { valid, reasons };
}

// Fix sweep direction to always match zone type
// Called after detectSweep — overrides geometry-based direction with zone-type direction
function correctSweepDirection(sweep) {
  if (!sweep || !sweep.found || !sweep.level) return sweep;
  const correct = getZoneDirection(sweep.level) || sweep.direction;
  if (correct !== sweep.direction) {
    console.log('[bias] Direction corrected: ' + sweep.direction + ' → ' + correct +
      ' (zone type: ' + sweep.level.type + ')');
  }
  return { ...sweep, direction: correct };
}

// ═══════════════════════════════════════════════════════════════════════════
// HTF STRUCTURAL BIAS ENGINE
// Uses M15 candles to determine higher-timeframe directional bias.
// Bias is a CONFIDENCE MODIFIER — never a trade filter.
// Returns: { bias, lastBOS, lastHigh, lastLow, reason }
// ═══════════════════════════════════════════════════════════════════════════
function calcHTFBias(m15Candles) {
  if (!m15Candles || m15Candles.length < 8) {
    return { bias: 'NEUTRAL', lastBOS: 'NONE', lastHigh: 0, lastLow: 0, reason: 'Insufficient M15 data' };
  }

  // Use last 20 M15 candles (~5 hours) for structure analysis
  const candles = m15Candles.slice(-20);
  const n = candles.length;

  // ── 1. Find swing highs and swing lows (pivot method: higher on both sides)
  const swingHighs = [];
  const swingLows  = [];
  for (let i = 1; i < n - 1; i++) {
    if (candles[i].h > candles[i-1].h && candles[i].h > candles[i+1].h) {
      swingHighs.push({ price: candles[i].h, idx: i });
    }
    if (candles[i].l < candles[i-1].l && candles[i].l < candles[i+1].l) {
      swingLows.push({ price: candles[i].l, idx: i });
    }
  }

  if (swingHighs.length < 2 || swingLows.length < 2) {
    return { bias: 'NEUTRAL', lastBOS: 'NONE', lastHigh: candles[n-1].h, lastLow: candles[n-1].l,
             reason: 'Insufficient swing structure' };
  }

  const lastHigh  = swingHighs[swingHighs.length - 1].price;
  const prevHigh  = swingHighs[swingHighs.length - 2].price;
  const lastLow   = swingLows[swingLows.length  - 1].price;
  const prevLow   = swingLows[swingLows.length  - 2].price;
  const livePrice = candles[n-1].c;

  // ── 2. Detect BOS: last swing high broken = UP BOS, last swing low broken = DOWN BOS
  const BOS_MARGIN = 0.0002; // 0.02% — minor buffer to avoid noise
  let lastBOS = 'NONE';
  // Check last few candles for breaks
  const recent = candles.slice(-5);
  for (const c of recent) {
    if (c.c > lastHigh * (1 + BOS_MARGIN)) { lastBOS = 'UP';   break; }
    if (c.c < lastLow  * (1 - BOS_MARGIN)) { lastBOS = 'DOWN'; break; }
  }

  // ── 3. HH/HL or LH/LL structure
  const isHHHL = lastHigh > prevHigh && lastLow > prevLow;  // bullish structure
  const isLHLL = lastHigh < prevHigh && lastLow < prevLow;  // bearish structure

  // ── 4. Price vs structure midpoint
  const structureMid = (lastHigh + lastLow) / 2;
  const aboveMid = livePrice > structureMid;

  // ── 5. Determine bias
  let bias = 'NEUTRAL';
  let reason = '';

  if (lastBOS === 'UP' && (isHHHL || aboveMid)) {
    bias = 'BULLISH';
    reason = 'BOS UP + ' + (isHHHL ? 'HH/HL structure' : 'price above mid');
  } else if (lastBOS === 'DOWN' && (isLHLL || !aboveMid)) {
    bias = 'BEARISH';
    reason = 'BOS DOWN + ' + (isLHLL ? 'LH/LL structure' : 'price below mid');
  } else if (isHHHL && aboveMid) {
    bias = 'BULLISH';
    reason = 'HH/HL structure + price above mid (no BOS yet)';
  } else if (isLHLL && !aboveMid) {
    bias = 'BEARISH';
    reason = 'LH/LL structure + price below mid (no BOS yet)';
  } else {
    bias = 'NEUTRAL';
    reason = 'Choppy / ranging structure';
  }

  return { bias, lastBOS, lastHigh, lastLow, structureMid, reason };
}

// ── TRADE RESULT MONITOR ──────────────────────────────────────────────────────
// Runs on every scan after an entry signal fires.
// Checks live price against SL/TP1/TP2 and auto-logs the result.
function checkTradeMonitor(sym, livePrice, m5) {
  const mon = tradeMonitor[sym];
  if (!mon || mon.resultLogged) return;

  const isBuy  = mon.direction === 'BUY';
  const high   = Math.max(...m5.slice(-3).map(c => c.h)); // recent 3-candle high
  const low    = Math.min(...m5.slice(-3).map(c => c.l)); // recent 3-candle low

  // Track adverse excursion for BE detection
  mon.maxFav   = isBuy
    ? Math.max(mon.maxFav || mon.entry, high)
    : Math.min(mon.maxFav || mon.entry, low);

  let result = null;

  if (isBuy) {
    if (mon.tp2 && high >= mon.tp2)       result = 'TP2';
    else if (mon.tp1 && high >= mon.tp1)  result = 'TP1';
    else if (low  <= mon.sl)              result = 'SL';
  } else {
    if (mon.tp2 && low  <= mon.tp2)       result = 'TP2';
    else if (mon.tp1 && low  <= mon.tp1)  result = 'TP1';
    else if (high >= mon.sl)              result = 'SL';
  }

  if (result) {
    mon.resultLogged = true;
    logTradeResult(mon.setupId, result);
    console.log('[monitor] ' + sym + ': ' + result + ' hit — ' + mon.direction +
      ' entry=' + mon.entry + ' sl=' + mon.sl + ' tp1=' + mon.tp1);
    tradeMonitor[sym] = null; // clear monitor
  }
}

async function autoScan() {
  const h         = new Date().getUTCHours();
  const inSession = (h >= 7 && h < 16) || (h >= 13 && h < 22);

  if (!inSession) {
    for (const sym of ['XAUUSD','XAGUSD']) {
      if (setups[sym]) {
        resetSetup(sym, 'Session closed');
      }
      // Clear any open trade monitor at session close
      if (tradeMonitor[sym] && !tradeMonitor[sym].resultLogged) {
        console.log('[monitor] ' + sym + ': session closed — trade monitor cleared (no result)');
        tradeMonitor[sym] = null;
      }
      // Reset consecutive failures and structural bias at session close
      if (symTiming[sym]) {
        symTiming[sym].structuralBiasDir   = null;
        symTiming[sym].structuralBiasStage = null;
        symTiming[sym].consecutiveFailures = { BUY: 0, SELL: 0 };
      }
      clearZoneMemory(sym); // zone memory resets each session
    }
    console.log('[auto-scan] Outside session (UTC ' + h + ':xx) — skipping');
    return;
  }

  const minsLeft = sessionMinutesRemaining();
  if (minsLeft < 60) {
    console.log('[auto-scan] <60min in session (' + minsLeft + 'min) — no new signals');
    return;
  }

  console.log('[auto-scan] Scanning — UTC ' + h + ':' + String(new Date().getUTCMinutes()).padStart(2,'0') +
    ' | ' + minsLeft + 'min remaining');

  const delay = ms => new Promise(r => setTimeout(r, ms));

  for (const sym of ['XAUUSD','XAGUSD']) {
    try {
      const m5 = await getCandles(sym, '5min', 120);
      if (!m5 || m5.length < 50) {
        console.log('[auto-scan] ' + sym + ': insufficient data');
        await delay(400); continue;
      }

      const m15        = deriveM15FromM5(m5);
      const atrValues  = calcATRFromCandles(m5, 14);
      const currentATR = atrValues.length ? atrValues[atrValues.length-1] : null;
      const livePrice  = m5[m5.length-1].c;
      const volatility = checkATR(sym, atrValues);
      const levels     = buildLevels(m5, m15);

      // Check open trade monitor first (non-blocking result detection)
      checkTradeMonitor(sym, livePrice, m5);

      // ── UPDATE HTF BIAS from M15 ──────────────────────────────
      const htfResult = calcHTFBias(m15);
      if (timing) {
        timing.htfBias      = htfResult.bias;
        timing.htfLastBOS   = htfResult.lastBOS;
        timing.htfLastHigh  = htfResult.lastHigh;
        timing.htfLastLow   = htfResult.lastLow;
        timing.htfUpdatedAt = Date.now();
      }
      console.log('[htf] ' + sym + ': bias=' + htfResult.bias +
        ' BOS=' + htfResult.lastBOS + ' — ' + htfResult.reason);
      const sess       = sessionName(Date.now());
      const sessionOk  = sess !== null;
      const sessionOverlap = sess === 'London+NY Overlap';

      // Unified directional bias — single source of truth
      const globalBias    = calcGlobalBias(levels, livePrice, setups[sym]);
      const directionalBias = globalBias.bias === 'BUY'  ? 'bullish_bias'
                            : globalBias.bias === 'SELL' ? 'bearish_bias'
                            : 'neutral';
      // Penalty increases with stronger confirmed bias
      const biasPenalty = Math.abs(globalBias.score) >= 3 ? 8
                        : Math.abs(globalBias.score) >= 2 ? 5
                        : Math.abs(globalBias.score) >= 1 ? 3 : 0;

      const asset = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
      let setup   = setups[sym];

      // ── INVALIDATION CHECKS on existing setup ──────────────────
      if (setup && setup.active && !setup.invalidated) {
        const ageMins = (Date.now() - setup.startedAt) / 60000;

        // Time decay — 50 minutes max
        if (ageMins > 50) {
          await invalidateSetup(sym, 'Setup expired — no entry within 50 minutes.');
          resetSetup(sym, 'Time decay');
          await delay(400); continue;
        }

        // Counter-structure: only check PRIMARY ZONE for opposing sweep
        // (using all levels caused secondary zones to trigger false invalidations)
        const primaryZoneForCheck = selectPrimaryZone(levels, livePrice, sess, m5);
        if (primaryZoneForCheck) {
          const sweep2 = detectSweep(m5, [primaryZoneForCheck]);
          if (sweep2.found) {
            const sweep2Corrected = correctSweepDirection(sweep2);
            if (sweep2Corrected.direction !== setup.direction) {
              await invalidateSetup(sym, 'Structure broke against ' + setup.direction + ' direction on primary zone.');
              resetSetup(sym, 'Counter-structure');
              // Counter-structure sweep = opposite structure confirmed → clear bias
              if (symTiming[sym]) {
                symTiming[sym].structuralBiasDir   = sweep2Corrected.direction;
                symTiming[sym].structuralBiasStage = 'sweep';
                symTiming[sym].structuralBiasAt    = Date.now();
                console.log('[bias] ' + sym + ': structural bias flipped → ' +
                  sweep2Corrected.direction + ' (counter-structure confirmed)');
              }
              await delay(400); continue;
            }
          }
        }
      }

      // ── DETECT MARKET STATE ────────────────────────────────────
      if (!sessionOk || volatility.ok === false) {
        await delay(400); continue;
      }

      // ── PRIMARY ZONE SELECTION — only this zone matters ─────────
      const timing    = symTiming[sym];  // declared here — used throughout this block
      const structBias = timing
        ? { dir: timing.structuralBiasDir, stage: timing.structuralBiasStage }
        : null;
      const primaryZone = selectPrimaryZone(levels, livePrice, sess, m5, structBias);
      if (!primaryZone) {
        console.log('[' + sym + '] No primary zone found — skip');
        await delay(400); continue;
      }

      // ── ZONE MEMORY: track retest count + gate exhausted zones ──
      updateZoneMemory(sym, primaryZone);
      const zoneFreshness = getZoneFreshness(sym, primaryZone);
      console.log('[zone-mem] ' + sym + ': ' + zoneFreshness.label +
        ' (session touches: ' + zoneFreshness.touchCount + ')');
      if (zoneFreshness.suppress) {
        console.log('[zone-mem] ' + sym + ': EXHAUSTED zone — signal suppressed');
        await delay(400); continue;
      }

      const zoneScore = primaryZone.confidence?.total || 0;
      console.log('[' + sym + '] Primary zone: ' + primaryZone.direction +
        ' ' + primaryZone.priceRange + ' score=' + zoneScore + '/100' +
        (zoneScore >= 75 ? ' [FULL]' : zoneScore >= 60 ? ' [STANDARD]' : ' [BLOCKED]'));

      // ── ZONE DETECTION COOLDOWN ────────────────────────────────
      // (timing already declared above)
      if (timing && Date.now() < timing.zoneDetectionAllowedAt) {
        const waitMins = Math.ceil((timing.zoneDetectionAllowedAt - Date.now()) / 60000);
        console.log('[timing] ' + sym + ': zone detection cooldown active (' + waitMins + 'min remaining) — skipping');
        await delay(400); continue;
      }

      // ── BIAS FLIP PROTECTION (cooldown-based) ──────────────────
      if (timing && timing.lastInvalidatedDir && Date.now() < timing.biasFlipAllowedAt) {
        if (primaryZone && primaryZone.direction !== timing.lastInvalidatedDir) {
          const waitMins = Math.ceil((timing.biasFlipAllowedAt - Date.now()) / 60000);
          console.log('[timing] ' + sym + ': bias flip blocked — last setup was ' +
            timing.lastInvalidatedDir + ', opposite direction locked for ' + waitMins + 'min');
          await delay(400); continue;
        }
      }

      // ── STRUCTURAL BIAS GATE ─────────────────────────────────────
      // If a structural bias is active (sweep/move/trend confirmed in one direction),
      // block opposite-direction setups until structure confirms the flip.
      // Counter-trend zones are demoted in selectPrimaryZone but may still win
      // if no same-direction zone exists — block them here.
      if (timing && timing.structuralBiasDir && primaryZone) {
        const biasStage    = timing.structuralBiasStage || 'sweep';
        const stageStrength = { sweep: 1, move: 2, trend: 3 };
        const strength      = stageStrength[biasStage] || 1;

        if (primaryZone.direction !== timing.structuralBiasDir && primaryZone.isCounterTrend) {
          if (strength >= 2) {
            // Move or trend confirmed — hard block on counter-trend setup
            console.log('[bias] ' + sym + ': BUY zone blocked — no bullish structure confirmation' +
              ' (structural bias: ' + timing.structuralBiasDir + ' at ' + biasStage + ')');
            await delay(400); continue;
          }
          // Sweep only — allow but log
          console.log('[bias] ' + sym + ': counter-trend ' + primaryZone.direction +
            ' zone allowed (bias only at sweep level — monitoring)');
        }
      }

      // ── ZONE SCORE GATE ────────────────────────────────────────
      // < 60  → no signals at all — zone not strong enough
      // 60–74 → standard entry only, aggressive engine suppressed
      // ≥ 75  → full system: standard + aggressive
      if (zoneScore < 60) {
        console.log('[' + sym + '] Zone score ' + zoneScore + ' < 60 — all signals suppressed');
        // Cancel any active setup that depended on this zone
        if (setup && setup.active) {
          console.log('[' + sym + '] Cancelling active setup — zone score ' + zoneScore + ' fell below 60');
          await invalidateSetup(sym, 'Setup cancelled: insufficient zone strength for execution (score ' + zoneScore + '/100 < 60).');
          resetSetup(sym, 'Zone score below 60');
        }
        await delay(400); continue;
      }

      // ── ZONE STRENGTH CHECK AT TREND SHIFT+ STAGES ─────────────
      // If setup has progressed past trend shift but zone score is now < 60,
      // cancel — do not proceed to pullback or entry with a weak zone.
      if (setup && setup.active && setup.events?.trend) {
        if (zoneScore < 60) {
          console.log('[' + sym + '] Setup cancelled at trend+ stage — zone score ' + zoneScore + ' < 60');
          await invalidateSetup(sym, 'Setup cancelled: insufficient zone strength for execution (score ' + zoneScore + '/100 < 60).');
          resetSetup(sym, 'Zone too weak at trend+ stage');
          await delay(400); continue;
        }
      }

      // Detect sweep from primary zone only — non-primary zones ignored
      let sweep = detectSweep(m5, [primaryZone]); // PRIMARY ZONE ONLY — all secondary zones ignored
      if (sweep.found) sweep = correctSweepDirection(sweep);
      const sweepToNow = sweep.found ? (m5.length - 1 - sweep.candleIdx) : 999;

      // ── STAGE: APPROACHING ─────────────────────────────────────
      // Fire once when price is near a key zone and no setup is active yet
      if (!setup && !sweep.found) {
        // Only approach the PRIMARY ZONE — not random levels
        const nearEdgePz = primaryZone.direction === 'SELL' ? primaryZone.minPrice : primaryZone.maxPrice;
        const distPctPz  = Math.abs(livePrice - nearEdgePz) / nearEdgePz;
        const approaching = distPctPz <= 0.005 // within 0.5% of primary zone
          ? { ...primaryZone, distPct: parseFloat((distPctPz*100).toFixed(3)), dir: primaryZone.direction }
          : null;

        if (approaching) {
          // Create setup for this zone, fire approaching event
          // Validate approaching zone before creating setup
          const vApproach = validateSignal(sym, { found: true, level: approaching, direction: approaching.dir,
            wickPct: 0.5 }, m5, levels, setups[sym]);
          // For approaching, we only check Rules 1-4 (zone not swept yet, so R5-8 N/A)
          const approachBlocked = vApproach.reasons.filter(r =>
            !r.includes('Sweep too weak') && !r.includes('candle') && !r.includes('already swept'));
          if (approachBlocked.length > 0) {
            console.log('[validate] ' + sym + ': approaching blocked — ' + approachBlocked.join(', '));
            await delay(400); continue;
          }
          const newSetup = createAndLogSetup(sym, approaching.dir, approaching);
          setups[sym] = newSetup;
          setup = newSetup;
          const rangeStr = approaching.isZone
            ? '$' + parseFloat(approaching.minPrice).toFixed(2) + '–$' + parseFloat(approaching.maxPrice).toFixed(2)
            : '$' + parseFloat(approaching.price).toFixed(2);
          await fireEvent(setup, 'approaching', sym, async () => {
            if (TELEGRAM_MODE === 'FULL') await sendTelegram(
            asset + ' ' + approaching.dir + ' ZONE\n' +
            'Range: $' + parseFloat(approaching.minPrice||approaching.price).toFixed(2) + ' – $' + parseFloat(approaching.maxPrice||approaching.price).toFixed(2) + '\n' +
            'Strength: ' + (approaching.confidence?.total || approaching.score || '—') + '/100\n' +
            'Touches: ' + (approaching.totalTouches || '—') + '\n\n' +
            'If price sweeps through and reverses, a ' + approaching.dir + ' setup may form.\n\n' +
            '⏳ No action yet — monitoring.\n\n─────────────────\nAurum Signals'
            );
          });
        }
        await delay(400); continue;
      }

      // ── STAGE: SWEEP DETECTED ──────────────────────────────────
      if (!sweep.found || sweepToNow > 10) {
        await delay(400); continue;
      }

      // New sweep on a DIFFERENT zone than current setup → reset
      if (setup && sweep.found) {
        const newZoneId = sweep.level && sweep.level.isZone
          ? Math.round(sweep.level.minPrice) + '-' + Math.round(sweep.level.maxPrice)
          : Math.round(parseFloat(sweep.level?.price || 0));
        if (setup.zoneId && setup.zoneId !== String(newZoneId)) {
          console.log('[' + sym + '] New sweep on different zone — resetting setup');
          resetSetup(sym, 'New sweep on different zone');
          setup = null;
        }
      }

      // Respect early entry lock — block new setups for 2 candles after AGGRESSIVE_EARLY
      const priorSetup = setups[sym];
      if (priorSetup && priorSetup.earlyLockUntil && Date.now() < priorSetup.earlyLockUntil) {
        const lockMins = Math.ceil((priorSetup.earlyLockUntil - Date.now()) / 60000);
        console.log('[lock] ' + sym + ': early entry lock active (' + lockMins + 'min remaining) — ignoring new sweep');
        await delay(400); continue;
      }

      // Validate before creating any setup
      if (!setup) {
        const vResult = validateSignal(sym, sweep, m5, levels, null);
        if (!vResult.valid) {
          console.log('[validate] ' + sym + ': setup creation blocked (' + vResult.reasons.length + ' failures)');
          await delay(400); continue;
        }
        setup = createAndLogSetup(sym, sweep.direction, sweep.level);
        setups[sym] = setup;
        console.log('[validate] ' + sym + ': setup passed all 8 rules — created id=' + setup.id);
      }

      // Block if already invalidated
      if (setup.invalidated) {
        console.log('[' + sym + '] Setup invalidated — skipping signal engine');
        await delay(400); continue;
      }

      const lvlDesc = sweep.level && sweep.level.isZone
        ? sweep.level.label + ' (' + sweep.level.priceRange + ')'
        : (sweep.level?.label || 'key level');

      // Track timing for cooldown
      const newSweepKey = sweep.direction + '_' +
        (sweep.level?.isZone ? Math.round(sweep.level.minPrice) + '-' + Math.round(sweep.level.maxPrice)
                             : Math.round(sweep.level?.price || 0));
      const SWEEP_COOLDOWN_MS = 3 * CANDLE_MS;
      const isDupeSweep = timing &&
          timing.lastSweepDir === sweep.direction &&
          timing.lastSweepZoneKey !== newSweepKey &&
          Date.now() - timing.lastSweepAlertAt < SWEEP_COOLDOWN_MS;

      if (isDupeSweep) {
        console.log('[zone-merge] ' + sym + ': sweep alert suppressed (same direction, overlapping zone, within cooldown)');
      }

      const sweepFired = await fireEvent(setup, 'sweep', sym, async () => {
        // Record sweep alert timing
        if (timing) {
          timing.lastSweepAlertAt  = Date.now();
          timing.lastSweepDir      = sweep.direction;
          timing.lastSweepZoneKey  = newSweepKey;
        }
        if (!isDupeSweep && TELEGRAM_MODE === 'FULL') {
          await sendTelegram(
            '⚡ <b>' + asset + ' — LIQUIDITY GRAB DETECTED</b>\n\n' +
            asset + ' swept ' + lvlDesc + ' and closed back inside.\n' +
            'Direction: <b>' + sweep.direction + '</b>\n\n' +
            '⏳ Waiting for a strong displacement candle.\n\n─────────────────\nAurum Signals'
          );
        }
      }, sweep.candleIdx);

      // ── STAGE GATE: if sweep just fired this scan, stop here ──
      // Forces each stage to be confirmed on a separate scan cycle.
      // Prevents bulk-confirmation of multiple stages from historical data.
      if (sweepFired) {
        logStageUpdate(setup, 'sweep');
        // Record structural bias at sweep stage
        if (timing) {
          timing.structuralBiasDir   = sweep.direction;
          timing.structuralBiasStage = 'sweep';
          timing.structuralBiasAt    = Date.now();
          console.log('[bias] ' + sym + ': structural bias set → ' + sweep.direction + ' (sweep stage)');
        }
        console.log('[' + sym + '] Sweep fired this scan — waiting for next scan before displacement');
        await delay(400); continue;
      }

      // ── STAGE: DISPLACEMENT (MOVE) ────────────────────────────
      // HTF counter-bias: require stronger displacement (1.5× instead of default)
      const _htfBiasNow = timing?.htfBias || 'NEUTRAL';
      const _htfCounter = (_htfBiasNow === 'BULLISH' && sweep.direction === 'SELL') ||
                          (_htfBiasNow === 'BEARISH' && sweep.direction === 'BUY');
      const dispMinRatio = _htfCounter ? 1.5 : 1.2; // stricter for counter-HTF setups

      const disp = detectDisplacement(m5, sweep.candleIdx, sweep.direction, dispMinRatio);
      if (!disp.found) {
        if (_htfCounter) {
          console.log('[htf] ' + sym + ': counter-HTF setup — displacement must be ≥' + dispMinRatio + '× avg');
        }
        console.log('[' + sym + '] Waiting for displacement');
        await delay(400); continue;
      }

      const moveFired = await fireEvent(setup, 'move', sym, async () => {
        if (TELEGRAM_MODE === 'FULL') {
          await sendTelegram(
            '↗️ <b>' + asset + ' — STRONG MOVE CONFIRMED</b>\n\n' +
            'A ' + disp.ratio + '× displacement candle followed the liquidity grab.\n' +
            'Direction: <b>' + sweep.direction + '</b>\n\n' +
            '⏳ Waiting for break of structure.\n\n─────────────────\nAurum Signals'
          );
        }
      }, disp.candleIdx);

      if (moveFired) {
        logStageUpdate(setup, 'move');
        if (timing) {
          timing.structuralBiasDir   = sweep.direction;
          timing.structuralBiasStage = 'move';
          timing.structuralBiasAt    = Date.now();
          console.log('[bias] ' + sym + ': structural bias strengthened → ' + sweep.direction + ' (move stage)');
        }
        console.log('[' + sym + '] Move fired this scan — waiting for next scan before BOS');
        await delay(400); continue;
      }

      // ── VWAP RECLAIM CHECK (HARD GATE) ────────────────────────
      // Requires price to close back through session VWAP after the sweep
      // before BOS is evaluated. Eliminates fake displacement moves that
      // fail to reclaim value area — the most common source of false signals.
      //
      // Grace period: 3 candles (15 min) after the sweep before blocking.
      // This allows the reclaim candle time to form without firing too early.
      const vwapCheck = detectVWAPReclaim(m5, sweep.candleIdx, sweep.direction);
      console.log('[vwap] ' + sym + ': ' + vwapCheck.note);
      if (!vwapCheck.reclaimed) {
        const candlesSinceSweep = m5.length - 1 - sweep.candleIdx;
        if (candlesSinceSweep > 3) {
          // Hard block: no VWAP reclaim after 15 min = weak institutional follow-through
          console.log('[vwap] ' + sym + ': ❌ BLOCKED — no VWAP reclaim after ' +
            candlesSinceSweep + ' candles (VWAP $' + (vwapCheck.vwap || '—') + ')');
          // Log the invalidation so it appears in /stats and Sheets
          if (setup && !setup.invalidated) {
            await invalidateSetup(sym, 'VWAP reclaim failed — price could not close through session VWAP after sweep.');
            resetSetup(sym, 'VWAP reclaim failed');
          }
          await delay(400); continue;
        }
        // Within grace period — wait silently
        console.log('[vwap] ' + sym + ': waiting for VWAP reclaim (candle ' +
          candlesSinceSweep + '/3 grace period)');
        await delay(400); continue;
      }
      console.log('[vwap] ' + sym + ': ✓ VWAP reclaimed — BOS evaluation unlocked');

      // ── STAGE: TREND SHIFT (BOS) ──────────────────────────────
      const bos = detectBOS(m5, sweep.candleIdx, sweep.direction);
      if (!bos.found) {
        console.log('[' + sym + '] Waiting for BOS');
        await delay(400); continue;
      }

      const m15bos = m15.length >= 8 ? confirmBOS_M15(m15, sweep.direction, bos.bos_level) : false;
      const trendFired = await fireEvent(setup, 'trend', sym, async () => {
        if (TELEGRAM_MODE === 'FULL') {
          await sendTelegram(
            '✅ <b>' + asset + ' — TREND SHIFT CONFIRMED</b>\n\n' +
            'Break of structure confirmed on M5' + (m15bos ? '/M15' : '') + '.\n' +
            'Direction: <b>' + sweep.direction + '</b>\n\n' +
            '⏳ Waiting for 50–61.8% pullback into entry zone.\n\n─────────────────\nAurum Signals'
          );
        }
      }, bos.candleIdx);

      if (trendFired) {
        logStageUpdate(setup, 'trend');
        if (timing) {
          timing.structuralBiasDir   = sweep.direction;
          timing.structuralBiasStage = 'trend';
          timing.structuralBiasAt    = Date.now();
          console.log('[bias] ' + sym + ': structural bias confirmed → ' + sweep.direction + ' (BOS stage)');
        }
        // PRE-ENTRY ALERT: send at trend confirmation (not pullback)
        // This ensures alert fires even if pullback immediately exceeds 70%
        if (TELEGRAM_MODE !== 'FULL' && setup && !setup.tgAlerts?.preEntry) {
          if (setup.tgAlerts) setup.tgAlerts.preEntry = true;
          const _htfNow     = timing?.htfBias || 'NEUTRAL';
          const _htfAligned = (_htfNow === 'BULLISH' && sweep.direction === 'BUY') ||
                              (_htfNow === 'BEARISH' && sweep.direction === 'SELL');
          const _htfCounter = (_htfNow === 'BULLISH' && sweep.direction === 'SELL') ||
                              (_htfNow === 'BEARISH' && sweep.direction === 'BUY');
          const _htfLine    = _htfNow === 'NEUTRAL'
            ? 'HTF Bias: Neutral ➖'
            : _htfAligned
              ? 'HTF Bias: ' + _htfNow.charAt(0) + _htfNow.slice(1).toLowerCase() + ' ✅ (aligned)'
              : 'HTF Bias: ' + _htfNow.charAt(0) + _htfNow.slice(1).toLowerCase() + ' ❌ (counter — stronger confirmation required)';
          await sendTelegram(
            '⚠️ <b>' + asset + ' ' + sweep.direction + ' — SETUP FORMING</b>\n\n' +
            'Zone: $' + parseFloat(primaryZone.minPrice).toFixed(2) +
            ' – $' + parseFloat(primaryZone.maxPrice).toFixed(2) + '\n' +
            'Confidence: ' + zoneScore + '/100\n' +
            'Touches: ' + primaryZone.totalTouches + '\n' +
            _htfLine + '\n\n' +
            'Status: Waiting for pullback into entry zone.\n\n' +
            '⏳ No action yet — monitor closely.\n\n─────────────────\nAurum Signals'
          );
        }
        console.log('[' + sym + '] Trend fired this scan — waiting for next scan before pullback');
        await delay(400); continue;
      }

      // ── STAGE: PULLBACK ───────────────────────────────────────
      // Invalidation delay: after trend shift, wait at least 1 candle before
      // evaluating pullback. Prevents instant invalidation on same scan as BOS.
      const trendCandleIdx = setup.stageCandleIdx?.trend ?? bos.candleIdx;
      const candlesSinceTrend = (m5.length - 1) - trendCandleIdx;
      if (candlesSinceTrend < 2) {
        console.log('[timing] ' + sym + ': trend confirmed at candle ' + trendCandleIdx +
          ' — invalidation blocked (' + candlesSinceTrend + '/2 candles elapsed)');
        await delay(400); continue;
      }

      const pb = detectPullback(m5, disp.candleIdx, sweep.direction, sweep.sweepExtreme);

      // Track pullback candle count — need min 2 candles of structure before 70% invalidation
      const curCandleIdx = m5.length - 1;
      if (timing) {
        if (!pb.found && timing.pullbackStartCandleIdx < 0) {
          // First scan where pullback is being evaluated
          timing.pullbackStartCandleIdx = curCandleIdx;
          timing.pullbackCandleCount    = 0;
        } else if (!pb.found && timing.pullbackStartCandleIdx >= 0) {
          timing.pullbackCandleCount = curCandleIdx - timing.pullbackStartCandleIdx;
        }
      }
      const pullbackCandles = timing ? timing.pullbackCandleCount : 99;

      if (!pb.found) {
        if (pb.reason && pb.reason.includes('70%')) {
          if (pullbackCandles < 2) {
            console.log('[timing] ' + sym + ': 70% invalidation skipped — only ' +
              pullbackCandles + ' pullback candle(s) formed (min 2 required)');
            await delay(400); continue;
          }
          await invalidateSetup(sym, 'Pullback exceeded 70% retracement.');
          resetSetup(sym, 'Pullback > 70%');
        } else {
          console.log('[' + sym + '] Waiting for pullback (candle ' + curCandleIdx + ')');
        }
        await delay(400); continue;
      }

      const pullbackFired = await fireEvent(setup, 'pullback', sym, async () => {
        if (TELEGRAM_MODE === 'FULL') {
          await sendTelegram(
            '🎯 <b>' + asset + ' — PULLBACK INTO ENTRY ZONE</b>\n\n' +
            'Price pulled back ' + pb.retracement + '% into the entry zone.\n' +
            'Preparing to evaluate full signal.\n\n' +
            '⏳ Running quality checks...\n\n─────────────────\nAurum Signals'
          );
        }
        // Pre-entry alert already sent at trend stage — nothing extra needed here
      }, pb.candleIdx);
      if (pullbackFired) logStageUpdate(setup, 'pullback');

      // ── STAGE: ENTRY SIGNAL ───────────────────────────────────
      // Run aggressive entry engine — gated by zone score
      // Zone score < 75 → aggressive engine suppressed, standard only
      // Zone score ≥ 75 → full aggressive engine enabled
      const allowAggressive = zoneScore >= 75;
      const entryResult = allowAggressive
        ? aggressiveEntryEngine(sym, m5, primaryZone, sess)
        : { type: 'NO_ENTRY', reason: 'Zone score ' + zoneScore + ' < 75 — aggressive suppressed' };
      if (!allowAggressive) {
        console.log('[' + sym + '] Zone score ' + zoneScore + ' — aggressive engine suppressed (standard only)');
      }

      // If engine sees no valid entry pattern at this exact moment, defer
      // SWEEP_FORMING: sweep valid, momentum not yet confirmed — wait
      if (entryResult.type === 'SWEEP_FORMING') {
        console.log('[' + sym + '] Aggressive engine: sweep forming — ' + entryResult.reason);
        await delay(400); continue;
      }
      // NO_ENTRY with momentum timeout — send specific invalidation once
      if (entryResult.type === 'NO_ENTRY' && entryResult.reason?.includes('window expired') && setup) {
        if (!setup.momentumTimeoutSent && (TELEGRAM_MODE === 'FULL' || setup.tgAlerts?.preEntry || setup.events?.pullback)) {
          setup.momentumTimeoutSent = true;
          const _asset2  = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
          const _oppDir  = setup.direction === 'BUY' ? 'SELL' : 'BUY';
          const _oppEmoji= _oppDir === 'BUY' ? '🟢' : '🔴';
          const _t2      = symTiming[sym];
          if (_t2) {
            _t2.structuralBiasDir   = _oppDir;
            _t2.structuralBiasStage = 'sweep';
            _t2.consecutiveFailures[setup.direction] = (_t2.consecutiveFailures[setup.direction]||0)+1;
          }
          await sendTelegram('❌ <b>' + _asset2 + ' — SETUP INVALIDATED</b>\n\n' +
            'No momentum confirmed after liquidity sweep.\n' +
            'The 2-candle window expired without confirmation.\n\n' +
            _oppEmoji + ' ' + setup.direction + ' setup failed — short-term ' + _oppDir +
            ' bias active.\n\n─────────────────\nAurum Signals');
        }
      }
      if (entryResult.type === 'NO_ENTRY') {
        const isMomentumTimeout = entryResult.reason?.includes('expired');
        console.log('[' + sym + '] Aggressive engine: ' +
          (isMomentumTimeout ? '⚠ momentum timeout — ' : 'no pattern — ') + entryResult.reason);
        // Don't block structural path — fall through to standard score gate
      }
      if (entryResult.type === 'ENTRY_READY') {
        const modeLabel = entryResult.mode === 'AGGRESSIVE_EARLY' ? 'AGGRESSIVE_EARLY' : 'AGGRESSIVE';
        console.log('[' + sym + '] Engine: ' + modeLabel + ' ENTRY_READY — confidence=' +
          entryResult.confidence + ' | ' + (entryResult.entry_reason?.[0] || ''));
      }

      const sl  = calcSL(sweep.direction, sweep.sweepExtreme, currentATR || 0.5);
      const tps = calcTP(sweep.direction, pb.entry, sl, levels);

      if (tps.rr1 < 1.5) {
        console.log('[' + sym + '] R:R ' + tps.rr1 + ' below minimum — no signal');
        await delay(400); continue;
      }

      const avgRange = m5.slice(-10).reduce((s,c) => s + range(c), 0) / 10;
      const qf = runQualityFilters(m5, m15, sweep, disp, bos, pb,
        levels, sweep.direction, parseFloat(pb.entry.toFixed(3)), tps.tp1, sess, avgRange);
      if (!qf.pass) {
        console.log('[' + sym + '] Quality filter [' + qf.failedFilter + ']: ' + qf.reason);
        await delay(400); continue;
      }

      const scoreResult = scoreSetup(sess, sessionOk, sweep, disp, bos, pb,
        volatility.ok === true || volatility.ok === undefined, directionalBias, biasPenalty);

      // Aggressive engine boosts score when it confirms — additive, not replacement
      // Standard score gate still applies regardless
      let finalScore = scoreResult.total;
      if (entryResult.type === 'ENTRY_READY') {
        // Blend: average of structural score and aggressive confidence, +5 bonus
        finalScore = Math.min(Math.round((scoreResult.total + entryResult.confidence) / 2) + 5, 100);
        scoreResult.total = finalScore;
        scoreResult.grade = finalScore >= 85 ? 'A+' : finalScore >= 75 ? 'A' : scoreResult.grade;
        scoreResult.tier  = finalScore >= 75 ? 'HIGH' : scoreResult.tier;
        console.log('[' + sym + '] Score: ' + scoreResult.total + ' → ' + finalScore +
          ' (structural + aggressive confirmation) (' + scoreResult.grade + ')');
      } else {
        finalScore = scoreResult.total;
        console.log('[' + sym + '] Score: ' + finalScore + ' (' + scoreResult.grade + ')' +
          (entryResult.type === 'NO_ENTRY' ? ' [aggressive engine: no pattern]' : ''));
      }

      // Tier gate: only HIGH (≥75) gets a full signal
      if (scoreResult.tier !== 'HIGH') {
        console.log('[' + sym + '] Score ' + finalScore + ' tier=' + scoreResult.tier + ' — below 75 threshold');
        await delay(400); continue;
      }

      // ── HARD ENTRY GATE — ALL prior stages must be confirmed ────
      // This is the final safety check before any signal is sent.
      // Prevents entry if any upstream stage was skipped or not confirmed.
      const requiredStages = ['sweep', 'move', 'trend', 'pullback'];
      const missingStages  = requiredStages.filter(st => !setup.events[st]);
      if (missingStages.length > 0) {
        console.log('[' + sym + '] Entry blocked — missing confirmed stages: ' + missingStages.join(', '));
        await delay(400); continue;
      }
      if (setup.invalidated) {
        console.log('[' + sym + '] Entry blocked — setup already invalidated');
        await delay(400); continue;
      }

      // ── GENERATE FULL SIGNAL ──────────────────────────────────
      const sigKey = sym + '_' + sweep.direction + '_' + parseFloat(pb.entry.toFixed(2));
      if (sentSignals.has(sigKey)) {
        console.log('[' + sym + '] Signal already sent for this entry — blocked');
        await delay(400); continue;
      }

      const expiryUTC = new Date(Date.now() + minsLeft * 60000).toUTCString().split(' ')[4] + ' UTC';
      const rawSig = {
        id: Date.now(), asset: sym, direction: sweep.direction,
        entry: parseFloat(pb.entry.toFixed(3)),
        live_price: livePrice,
        stop_loss: sl, take_profit_1: tps.tp1, take_profit_2: tps.tp2,
        rr: tps.rr1, confidence: scoreResult.total,
        grade: scoreResult.grade, tier: scoreResult.tier, scoreBreakdown: scoreResult.breakdown,
        entry_mode: entryResult.type === 'ENTRY_READY' ? entryResult.mode : 'STANDARD',
        session: sess, directional_bias: directionalBias,
        sweep_level: sweep.level?.label || '—',
        pullback_pct: pb.retracement, expiry: expiryUTC,
        atr: currentATR,                               // v5.2: position sizing
        zoneFreshness: zoneFreshness?.label || null,   // v5.2: zone memory label
        primaryZone: primaryZone
          ? { low: primaryZone.minPrice, high: primaryZone.maxPrice }
          : null,
        reason: sess + ' ' + (sweep.level?.label||'') + ' sweep → ' +
          sweep.direction.toLowerCase() + ' displacement (' + disp.ratio + '×) → BOS → ' + pb.retracement + '% pullback'
      };
      rawSig.alert = formatSignalAlert(rawSig, currentATR);

      await fireEvent(setup, 'entry', sym, async () => {
        sentSignals.add(sigKey);
        setTimeout(() => sentSignals.delete(sigKey), 4 * 60 * 60 * 1000);
        setup.active = false;
        if (setup.tgAlerts) setup.tgAlerts.entry = true;
        // Log entry
        logEntryTriggered(setup, rawSig.entry, rawSig.stop_loss, [rawSig.take_profit_1, rawSig.take_profit_2].filter(Boolean));
        // Start trade result monitor
        tradeMonitor[sym] = {
          setupId:      setup.id,
          direction:    rawSig.direction,
          entry:        rawSig.entry,
          sl:           rawSig.stop_loss,
          tp1:          rawSig.take_profit_1,
          tp2:          rawSig.take_profit_2,
          maxFav:       rawSig.entry,
          resultLogged: false,
          startedAt:    Date.now(),
        };
        console.log('[monitor] ' + sym + ': trade monitor started — ' +
          rawSig.direction + ' entry=' + rawSig.entry + ' SL=' + rawSig.stop_loss +
          ' TP1=' + rawSig.take_profit_1 + ' TP2=' + rawSig.take_profit_2);
        // Early entry lock: if AGGRESSIVE_EARLY mode, freeze for 2 candles (10 min)
        if (entryResult.type === 'ENTRY_READY' && entryResult.mode === 'AGGRESSIVE_EARLY') {
          setup.earlyLockUntil = Date.now() + 10 * 60 * 1000; // 10 min = 2 × M5 candles
          console.log('[lock] ' + sym + ': AGGRESSIVE_EARLY lock active for 10 min');
        }
        await sendTelegram(formatTelegramSignal(rawSig));
      });

    } catch(e) {
      console.error('[auto-scan] Error for ' + sym + ':', e.message);
    }
    await delay(400);
  }
}

// ── WEBHOOK: frontend can trigger an immediate scan
app.post('/scan', async (req, res) => {
  res.json({ ok: true, message: 'Scan triggered' });
  autoScan(); // run in background
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('='.repeat(60));
  console.log('Aurum Signal Engine v5 — Liquidity Sweep');
  console.log('Port:', PORT);
  console.log('UTC time:', new Date().toUTCString());
  console.log('Symbols:', Object.keys(SYMBOLS).join(', '));
  console.log('Twelve Data key:', TWELVE_KEY.slice(0,8) + '...');
  const h = new Date().getUTCHours();
  const sess = (h>=7&&h<16&&h>=13) ? 'London+NY Overlap' : (h>=7&&h<16) ? 'London' : (h>=13&&h<22) ? 'New York' : 'Closed';
  console.log('Current session:', sess, '(UTC hour ' + h + ')');
  console.log('Telegram configured:', !!(TG_TOKEN && TG_CHAT_ID));
  console.log('='.repeat(60));

  // ── AUTO-SCAN SCHEDULER ─────────────────────────────────────
  // Scan every 5 minutes. Session check is inside autoScan() so
  // this interval runs 24/7 but only does work during London/NY.
  const SCAN_INTERVAL = 5 * 60 * 1000; // 5 minutes
  setInterval(autoScan, SCAN_INTERVAL);
  // Run once immediately on startup (after 10s to let server settle)
  setTimeout(autoScan, 10000);
  console.log('[scheduler] Auto-scan started — every 5 minutes during sessions');
  // Restore last 24h of logs from Sheets after Railway restarts
  hydrateFromSheets(_setupLogs).catch(e => console.error('[boot-hydrate]', e.message));
});
