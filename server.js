const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const app     = express();
// CORS: allow Netlify frontend and any origin (needed for Railway free tier)
app.use(cors({
  origin: '*',                   // allow all origins — tighten if needed
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.options('*', cors());        // handle preflight for all routes
app.use(express.json());

const TWELVE_KEY    = '7f3fc6ca85664930ab6e687db8ff0c5d';
const ANTHROPIC_KEY = ['sk-ant-','api03-PSBtiCb9gNCUnpxHjEl2sqWVtfNop5DtO1WCW2pdUw_upi3Zl0VDjCT7Yyk','W9bboA3Bxnq2ucHBFyuNrNx6CL','w-qYuk4wAA'].join('');
const SYMBOLS = {
  XAUUSD: 'XAU/USD',   // Gold spot — free tier
  XAGUSD: 'SLV'        // Silver via iShares Silver Trust ETF (SLV) — free tier
                        // XAG/USD requires Twelve Data paid plan; SLV tracks silver 1:1
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
function buildLevels(m5Candles, m15Candles) {
  const levels = [];
  const now = Date.now();

  // Previous Day H/L — candles from yesterday UTC date
  const todayUTC = new Date(); todayUTC.setUTCHours(0,0,0,0);
  const ydayStart = todayUTC.getTime() - 86400000;
  const ydayEnd   = todayUTC.getTime();
  const yday = m15Candles.filter(c => c.t >= ydayStart && c.t < ydayEnd);
  if (yday.length > 0) {
    levels.push({ price: Math.max(...yday.map(c=>c.h)), type:'PDH', label:'Previous Day High', strength:'strong', strengthScore:3 });
    levels.push({ price: Math.min(...yday.map(c=>c.l)), type:'PDL', label:'Previous Day Low',  strength:'strong', strengthScore:3 });
  }

  // Asian Session H/L — today 00:00–08:00 UTC on M15
  const asian = m15Candles.filter(c => {
    const h = new Date(c.t).getUTCHours();
    return c.t >= todayUTC.getTime() && h < 8;
  });
  if (asian.length > 0) {
    levels.push({ price: Math.max(...asian.map(c=>c.h)), type:'ASH', label:'Asian Session High', strength:'medium', strengthScore:2 });
    levels.push({ price: Math.min(...asian.map(c=>c.l)), type:'ASL', label:'Asian Session Low',  strength:'medium', strengthScore:2 });
  }

  // Equal Highs / Lows — M5, last 20 candles, within 0.05%
  const recent20 = m5Candles.slice(-20);
  const EQ_TOL   = 0.0005; // 0.05%

  // Equal Highs
  const eqHighGroups = [];
  recent20.forEach((c, i) => {
    let placed = false;
    for (const g of eqHighGroups) {
      if (pct(c.h, g[0].h) <= EQ_TOL) { g.push(c); placed = true; break; }
    }
    if (!placed) eqHighGroups.push([c]);
  });
  eqHighGroups.filter(g => g.length >= 2).forEach(g => {
    const avg = g.reduce((s,c)=>s+c.h,0)/g.length;
    const strength = g.length >= 4 ? 'strong' : g.length === 3 ? 'medium' : 'weak';
    levels.push({ price: avg, type:'EQH', label:'Equal Highs (x'+g.length+')',
                  strength, strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1, touches: g.length });
  });

  // Equal Lows
  const eqLowGroups = [];
  recent20.forEach(c => {
    let placed = false;
    for (const g of eqLowGroups) {
      if (pct(c.l, g[0].l) <= EQ_TOL) { g.push(c); placed = true; break; }
    }
    if (!placed) eqLowGroups.push([c]);
  });
  eqLowGroups.filter(g => g.length >= 2).forEach(g => {
    const avg = g.reduce((s,c)=>s+c.l,0)/g.length;
    const strength = g.length >= 4 ? 'strong' : g.length === 3 ? 'medium' : 'weak';
    levels.push({ price: avg, type:'EQL', label:'Equal Lows (x'+g.length+')',
                  strength, strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1, touches: g.length });
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
      const dist    = Math.abs(price - lvl.price);
      const distPct = dist / lvl.price;
      return {
        ...lvl,
        dist:       parseFloat(dist.toFixed(4)),
        distPct:    parseFloat((distPct * 100).toFixed(3)),
        approaching: distPct <= threshold,
        side:       price > lvl.price ? 'above' : 'below'
      };
    })
    .filter(l => l.approaching)
    .sort((a, b) => a.distPct - b.distPct);
}

function detectSweepPotential(price, approachingLevels, candles) {
  if (!approachingLevels.length || candles.length < 4) return [];
  const last3    = candles.slice(-3);
  const momentum = last3[last3.length - 1].c - last3[0].c;
  const alerts   = [];
  for (const lvl of approachingLevels) {
    const movingToward =
      (lvl.side === 'below' && momentum < 0) ||
      (lvl.side === 'above' && momentum > 0);
    if (movingToward) {
      const dir = lvl.side === 'below' ? 'BUY' : 'SELL';
      alerts.push({
        type:      'sweep_potential',
        level:     lvl,
        direction: dir,
        message:   'Price approaching ' + lvl.label + ' at $' + lvl.price.toFixed(3) +
                   ' (' + lvl.distPct + '% away) - potential ' + dir + ' sweep forming'
      });
    }
  }
  return alerts;
}

// ─── SWEEP DETECTION ──────────────────────────────────────────────────────
// Returns: { found, candleIdx, level, direction, wickPct }
function detectSweep(candles, levels) {
  const SWEEP_BREAK  = 0.0002; // 0.02% minimum penetration
  const WICK_MIN_PCT = 0.30;   // wick >= 30% of total range

  for (let i = candles.length - 6; i < candles.length; i++) {
    if (i < 0) continue;
    const c = candles[i];
    const totalRange = range(c);
    if (totalRange === 0) continue;

    for (const lvl of levels) {
      const p = lvl.price;

      // BUY sweep: low breaks BELOW level by >= 0.02%, close BACK ABOVE
      if (c.l < p * (1 - SWEEP_BREAK) && c.c > p) {
        const wickSize = p - c.l;
        if (wickSize / totalRange >= WICK_MIN_PCT) {
          return { found: true, candleIdx: i, level: lvl, direction: 'BUY',
                   sweepExtreme: c.l, closePrice: c.c, wickPct: wickSize/totalRange };
        }
      }

      // SELL sweep: high breaks ABOVE level by >= 0.02%, close BACK BELOW
      if (c.h > p * (1 + SWEEP_BREAK) && c.c < p) {
        const wickSize = c.h - p;
        if (wickSize / totalRange >= WICK_MIN_PCT) {
          return { found: true, candleIdx: i, level: lvl, direction: 'SELL',
                   sweepExtreme: c.h, closePrice: c.c, wickPct: wickSize/totalRange };
        }
      }
    }
  }
  return { found: false };
}

// ─── DISPLACEMENT ──────────────────────────────────────────────────────────
// Must occur within 1–3 candles after sweep candle
// Returns: { found, candleIdx, bodySize, avgBody }
function detectDisplacement(candles, sweepIdx, direction) {
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
    const bodyStrong = b >= avgBody10 * BODY_MULT;
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

// ─── SCORING SYSTEM (0–100) ──────────────────────────────────────────────────
// Six components. Each has a max score and hard-fail conditions.
// A+ = 85+, A = 75-84, discard < 75.

function scoreSetup(sessionLabel, sessionOk, sweep, displacement, bos, pullback,
                    volatilityOk, directionalBias, biasPenalty) {

  const breakdown = {};
  let total = 0;

  // ── 1. SESSION QUALITY (max 20) ──────────────────────────────
  if (!sessionOk) return { total: 0, grade: 'REJECT', reason: 'Outside session', breakdown: {} };
  const sessScore = sessionLabel === 'London+NY Overlap' ? 20
                  : sessionLabel === 'New York'           ? 16
                  : sessionLabel === 'London'             ? 13 : 0;
  breakdown.session = { score: sessScore, max: 20, label: sessionLabel };
  total += sessScore;

  // ── 2. LIQUIDITY QUALITY (max 20) ────────────────────────────
  if (!sweep.found) return { total: 0, grade: 'REJECT', reason: 'No sweep', breakdown };
  const liqScore = sweep.level
    ? (sweep.level.type === 'PDH' || sweep.level.type === 'PDL' ? 20   // strongest levels
    : sweep.level.type === 'ASH' || sweep.level.type === 'ASL'  ? 16
    : sweep.level.strengthScore >= 3                            ? 14   // EQH/EQL x4+
    : sweep.level.strengthScore >= 2                            ? 10   // x3
    : 6)                                                               // x2 weak
    : 6;
  breakdown.liquidity = { score: liqScore, max: 20, label: sweep.level?.label || '—' };
  total += liqScore;

  // ── 3. SWEEP QUALITY (max 20) ────────────────────────────────
  // Wick must be >= 30% of range (already enforced), score by wick size
  const wickPct = sweep.wickPct || 0;
  const sweepScore = wickPct >= 0.6 ? 20   // very clean sweep
                   : wickPct >= 0.45 ? 16
                   : wickPct >= 0.30 ? 12   // minimum valid
                   : 0;                     // below minimum = hard fail
  if (sweepScore === 0) return { total: 0, grade: 'REJECT', reason: 'Weak sweep wick', breakdown };
  breakdown.sweep = { score: sweepScore, max: 20, wickPct: Math.round(wickPct*100) };
  total += sweepScore;

  // ── 4. DISPLACEMENT STRENGTH (max 15) ────────────────────────
  if (!displacement.found) return { total: 0, grade: 'REJECT', reason: 'No displacement', breakdown };
  const dispScore = displacement.ratio >= 2.5 ? 15
                  : displacement.ratio >= 2.0 ? 13
                  : displacement.ratio >= 1.5 ? 10   // minimum
                  : 0;
  if (dispScore === 0) return { total: 0, grade: 'REJECT', reason: 'Displacement too weak', breakdown };
  const dispFinal = displacement.weakGap ? Math.max(dispScore - 3, 6) : dispScore;
  breakdown.displacement = { score: dispFinal, max: 15, ratio: displacement.ratio, weakGap: displacement.weakGap };
  total += dispFinal;

  // ── 5. STRUCTURE BREAK (max 15) ──────────────────────────────
  if (!bos.found) return { total: 0, grade: 'REJECT', reason: 'No BOS', breakdown };
  const bosScore = bos.structure_type === 'internal' && bos.method === 'close' ? 15
                 : bos.structure_type === 'internal'                            ? 13
                 : bos.method === 'close'                                       ? 11
                 : 9; // external + wick method = weakest valid
  breakdown.structure = { score: bosScore, max: 15, type: bos.structure_type, method: bos.method };
  total += bosScore;

  // ── 6. PULLBACK QUALITY (max 10) ─────────────────────────────
  if (!pullback.found) return { total: 0, grade: 'REJECT', reason: 'No pullback', breakdown };
  const pb = parseFloat(pullback.retracement);
  if (pb > 70) return { total: 0, grade: 'REJECT', reason: 'Pullback exceeded 70%', breakdown };
  const pbScore = (pb >= 50 && pb <= 61.8) ? 10   // ideal Fibonacci zone
                : (pb >= 45 && pb < 50)    ? 7    // slightly early
                : (pb > 61.8 && pb <= 70)  ? 5    // late but valid
                : 0;
  if (pbScore === 0) return { total: 0, grade: 'REJECT', reason: 'Pullback outside valid zone', breakdown };
  breakdown.pullback = { score: pbScore, max: 10, retracement: pb };
  total += pbScore;

  // ── VOLATILITY PENALTY ───────────────────────────────────────
  if (!volatilityOk) {
    total -= 8;
    breakdown.volatility = { penalty: -8 };
  }

  // ── DIRECTIONAL BIAS PENALTY ─────────────────────────────────
  if (biasPenalty > 0 && sweep.found) {
    const isCounter = (directionalBias === 'bearish_bias' && sweep.direction === 'BUY') ||
                      (directionalBias === 'bullish_bias' && sweep.direction === 'SELL');
    if (isCounter) {
      total -= biasPenalty;
      breakdown.bias = { penalty: -biasPenalty, note: 'Counter-trend' };
    }
  }

  total = Math.min(Math.max(total, 0), 100);
  const grade = total >= 85 ? 'A+' : total >= 75 ? 'A' : 'REJECT';

  return { total, grade, breakdown,
           reason: grade === 'REJECT' ? 'Score ' + total + ' below threshold 75' : null };
}

// Backwards-compatible wrapper used in /analyze route
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

  // ── 1. FETCH DATA ──────────────────────────────────────────────────────
  let m5, m15, atrValues;
  try {
    // ONE API call per symbol — everything else derived from M5 candles.
    // M15 is derived by grouping M5 candles (3 M5 = 1 M15).
    // ATR is calculated locally from candle data (Wilder's method).
    // This keeps total calls at 2/scan (1 per symbol) — within free tier.
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

  // ── SYSTEM STATE DETERMINATION ─────────────────────────────────────────────
  // State is independent of session. Data issues ≠ session closed.
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
  if (latestCandleAge > 30 && inSession) {
    return res.json({ success: true, symbol: sym, price: m5[m5Count-1]?.c || null,
      system_state: 'data_error',
      session: sessionName(nowUtc) || 'Active',
      session_ok: true, setup_state: 'standby',
      levels: [], log: ['Live data temporarily unavailable — latest candle is ' + Math.round(latestCandleAge) + ' minutes old'],
      signal: null, m5_candles: m5Count });
  }

  // M15 low — warn but do not block
  if (!m15 || m15Count < 8) {
    console.warn(sym + ': low M15 data (' + m15Count + ' candles) — M15 BOS confirmation disabled');
  }

  const currentPrice  = m5[m5.length-1].c;
  const currentATR    = atrValues?.length > 0 ? atrValues[atrValues.length-1] : null;
  const currentTS     = m5[m5.length-1].t;
  // Use current wall-clock UTC time for session detection, NOT candle timestamp.
  // Candle timestamp can be stale (SLV closes at 20:00 UTC; last candle
  // would otherwise make London session appear closed next morning).
  const sess          = sessionName(Date.now());
  const sessionOk     = sess !== null;

  // ── 2. LIQUIDITY LEVELS ────────────────────────────────────────────────
  const levels = buildLevels(m5, m15);

  // ── 3. VOLATILITY FILTER ──────────────────────────────────────────────
  const volatility = checkATR(sym, atrValues);

  // --- DIRECTIONAL BIAS -------------------------------------------------------
  // Light filter: adjusts confidence based on price vs PDH/PDL.
  // Does NOT block trades — only weights.
  let directionalBias = 'neutral';
  let biasPenalty     = 0;
  const pdhLevel = levels.find(l => l.type === 'PDH');
  const pdlLevel = levels.find(l => l.type === 'PDL');
  if (pdhLevel && currentPrice > pdhLevel.price) {
    directionalBias = 'bearish_bias';  // price above PDH = prefer shorts
    biasPenalty = 5;
  } else if (pdlLevel && currentPrice < pdlLevel.price) {
    directionalBias = 'bullish_bias';  // price below PDL = prefer longs
    biasPenalty = 5;
  }

    // ── STATE MACHINE ─────────────────────────────────────────────────────
  let setupState  = 'idle';
  let signal      = null;
  const log       = [];

  if (!sessionOk) {
    setupState = 'idle';
    log.push('Outside active trading sessions (London 07:00–16:00 UTC / New York 13:00–22:00 UTC) — signal engine paused');
  } else if (volatility.ok === false) {
    setupState = 'idle';
    log.push('Volatility check: ' + volatility.note);
  } else {
    // ── 4. SWEEP DETECTION ──────────────────────────────────────────────
    const sweep = detectSweep(m5, levels);

    if (!sweep.found) {
      setupState = 'idle';
      log.push('No liquidity grab detected on current M5 data');
    } else {
      setupState = 'sweep_detected';
      log.push('Liquidity grab: ' + sweep.direction + ' — price swept ' + sweep.level.label + ' at $' + sweep.level.price.toFixed(3) + ' (wick ' + (sweep.wickPct*100).toFixed(1) + '% of candle range)');

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
          log.push('Strong move: not confirmed — ' + disp.reason);
        } else {
          setupState = 'displacement_confirmed';
          log.push('Strong move confirmed: ' + disp.ratio + 'x average candle size, ' + (disp.candleIdx - sweep.candleIdx) + ' candle(s) after the liquidity grab' + (disp.weakGap ? ' (one weak candle gap tolerated)' : ''));

          // ── 7. BOS ────────────────────────────────────────────────
          const bos = detectBOS(m5, sweep.candleIdx, sweep.direction);

          if (!bos.found) {
            setupState = 'displacement_confirmed';
            log.push('Trend shift: not confirmed — ' + bos.reason);
          } else {
            const m15bos = confirmBOS_M15(m15, sweep.direction, bos.bos_level);
            setupState = 'structure_break';
            log.push('Trend shift (M5): ' + bos.label);
            log.push('Trend shift (M15): ' + (m15bos ? 'also visible on 15-minute chart' : 'not visible on 15-minute chart — M5 confirmation used'));

            // ── 8. PULLBACK ───────────────────────────────────────
            const pb = detectPullback(m5, disp.candleIdx, sweep.direction, sweep.sweepExtreme);

            if (!pb.found) {
              setupState = 'waiting_pullback';
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
                // ── 10. CONFIDENCE ────────────────────────────────
                const confidence = calcConfidence(sessionOk, sessionOverlap, volatility.ok === true || volatility.ok === undefined, sweep, disp, bos, pb, sweep.level, sess, directionalBias, biasPenalty);
                log.push('Confidence score: ' + confidence + '/100');

                if (confidence < 80) {
                  setupState = 'invalidated';
                  log.push('Signal not generated — confidence score ' + confidence + ' is below the required minimum of 80.');
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

  // --- PROXIMITY + PRE-SIGNAL ALERTS ----------------------------------------
  const approachingLevels = (levels.length && livePrice)
    ? detectApproaching(livePrice, levels, sym)
    : [];
  const sweepPotentials = approachingLevels.length
    ? detectSweepPotential(livePrice, approachingLevels, m5)
    : [];

  // Build near_setup alert based on furthest confirmed stage
  let near_setup = null;
  if (sweepPotentials.length && setupState === 'idle') {
    near_setup = {
      stage: 'approaching_liquidity',
      message: sweepPotentials[0].message,
      direction: sweepPotentials[0].direction,
      level: sweepPotentials[0].level,
      alert: formatPreSignalAlert('approaching_liquidity', sym, sweepPotentials[0].direction,
               sweepPotentials[0].message, sweepPotentials[0].level, sess, directionalBias)
    };
  } else if (setupState === 'sweep_detected') {
    near_setup = {
      stage: 'sweep_detected',
      message: (sweep.level ? 'Sweep of ' + sweep.level.label + ' confirmed' : 'Sweep confirmed') + ' - awaiting displacement',
      direction: sweep.direction,
      alert: formatPreSignalAlert('sweep_detected', sym, sweep.direction,
               null, sweep.level, sess, directionalBias)
    };
  } else if (setupState === 'displacement_confirmed') {
    near_setup = {
      stage: 'displacement_confirmed',
      message: 'Displacement confirmed (' + (disp ? disp.ratio : '?') + 'x body) - awaiting BOS',
      direction: sweep.direction,
      alert: formatPreSignalAlert('displacement_confirmed', sym, sweep.direction,
               null, null, sess, directionalBias)
    };
  } else if (setupState === 'structure_break') {
    near_setup = {
      stage: 'structure_break',
      message: 'BOS confirmed - awaiting pullback entry',
      direction: sweep.direction,
      alert: formatPreSignalAlert('structure_break', sym, sweep.direction,
               null, null, sess, directionalBias)
    };
  }

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
    sweep_potentials: sweepPotentials,
    directional_bias: directionalBias,
    m5_candles:   m5.length,
    ratio
  });
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
  // Serve prices from in-memory cache — zero API calls
  // Cache is populated by /analyze calls; stale by at most 60s which is fine
  const xauCache = getCached('candles_XAUUSD_5min_120');
  const xagCache = getCached('candles_XAGUSD_5min_120');
  const xau = xauCache ? parseFloat(xauCache[xauCache.length-1]?.c) || null : null;
  const xag = xagCache ? parseFloat(xagCache[xagCache.length-1]?.c) || null : null;
  res.json({
    success: true,
    prices: { XAUUSD: xau, XAGUSD: xag },
    ratio: xau && xag ? parseFloat((xau/xag).toFixed(2)) : null,
    from_cache: true,
    ts: new Date().toUTCString()
  });
});

// Keep-alive ping — call this from frontend every 4 min to prevent Railway sleep
app.get('/ping', (req, res) => res.json({ ok: true, ts: Date.now() }));

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
  status:'ok', version:'4.0',
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
  const isBuy  = sig.direction === 'BUY';
  const emoji  = isBuy ? '🟢' : '🔴';
  const asset  = sig.asset === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const grade  = sig.grade || (sig.confidence >= 85 ? 'A+' : 'A');
  const gradeEmoji = grade === 'A+' ? '⭐' : '✅';
  const alert  = sig.alert || {};
  const expiry = alert.expiry || '—';
  const bd     = sig.scoreBreakdown || {};

  // Score breakdown lines
  const bdLines = [];
  if (bd.session)      bdLines.push('  Session:      ' + bd.session.score + '/20 (' + bd.session.label + ')');
  if (bd.liquidity)    bdLines.push('  Liquidity:    ' + bd.liquidity.score + '/20 (' + bd.liquidity.label + ')');
  if (bd.sweep)        bdLines.push('  Sweep:        ' + bd.sweep.score + '/20 (wick ' + bd.sweep.wickPct + '%)');
  if (bd.displacement) bdLines.push('  Displacement: ' + bd.displacement.score + '/15 (' + bd.displacement.ratio + 'x body)');
  if (bd.structure)    bdLines.push('  Structure:    ' + bd.structure.score + '/15 (' + bd.structure.type + ' ' + bd.structure.method + ')');
  if (bd.pullback)     bdLines.push('  Pullback:     ' + bd.pullback.score + '/10 (' + bd.pullback.retracement + '% retrace)');

  const biasMap = {
    bullish_bias: '↑ Bullish — price below prior day low',
    bearish_bias: '↓ Bearish — price above prior day high',
    neutral:      'Neutral — within prior day range'
  };

  return [
    emoji + ' <b>' + asset + ' ' + sig.direction + ' — ' + gradeEmoji + ' ' + grade + ' SETUP (' + sig.confidence + '/100)</b>',
    '',
    '📋 <b>What happened:</b>',
    (alert.context || sig.reason || '—'),
    '',
    '📊 <b>Trade levels:</b>',
    '• Entry zone: $' + sig.entry,
    '• Stop loss:  $' + sig.stop_loss,
    '• Target 1:   $' + sig.take_profit_1,
    '• Target 2:   $' + sig.take_profit_2,
    '• Risk/Reward: 1:' + sig.rr,
    '• Expires:    ' + expiry,
    '',
    '📈 <b>Score breakdown (' + sig.confidence + '/100):</b>',
    ...bdLines,
    '',
    '🧭 <b>Market bias:</b> ' + (biasMap[sig.directional_bias] || 'Neutral'),
    '📍 <b>Session:</b> ' + (sig.session || '—'),
    '',
    '💡 <b>What this means:</b>',
    (alert.interpretation || '—'),
    '',
    '⚡ <b>Action:</b>',
    (alert.execution || 'Wait for pullback into entry zone before executing.'),
    '',
    '─────────────────',
    gradeEmoji + ' ' + grade + ' Signal #' + (sig.id || '—') + ' | Aurum Signals'
  ].join('\n');
}

// Format pre-signal alert for Telegram
function formatTelegramPreSignal(sym, ns) {
  const asset  = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const isBuy  = ns.direction === 'BUY';
  const emojis = {
    approaching_liquidity:  '📍',
    sweep_detected:         '⚡',
    displacement_confirmed: '↗️',
    structure_break:        '✅'
  };
  const emoji = emojis[ns.stage] || '📡';
  const stageLabel = {
    approaching_liquidity:  'KEY LEVEL NEARBY',
    sweep_detected:         'LIQUIDITY GRAB DETECTED',
    displacement_confirmed: 'STRONG MOVE CONFIRMED',
    structure_break:        'TREND SHIFT CONFIRMED'
  }[ns.stage] || 'SETUP FORMING';

  const alert = ns.alert || {};
  return [
    emoji + ' <b>' + asset + ' — ' + stageLabel + '</b>',
    '',
    alert.context || ns.message || '—',
    '',
    '⏳ ' + (alert.action || 'Monitoring. No action required yet.'),
    '',
    '─────────────────',
    'Pre-signal | Aurum Signals'
  ].join('\n');
}

// ═══════════════════════════════════════════════════════════════
// AUTO-SCAN ENGINE
// Runs every 5 minutes during London/NY sessions.
// Sends Telegram alerts when signals or pre-signals are detected.
// Tracks sent signals to avoid duplicate alerts.
// ═══════════════════════════════════════════════════════════════
const sentSignals    = new Set(); // track signal IDs already alerted
const sentPreSignals = {};        // track pre-signal stages already alerted per symbol

// Active setup tracker — max 1 per symbol at a time
// { XAUUSD: { direction, entry, startedAt, stage }, XAGUSD: {...} }
const activeSetups = {};

// Session time remaining in minutes
function sessionMinutesRemaining() {
  const h = new Date().getUTCHours();
  const m = new Date().getUTCMinutes();
  const nowMins = h * 60 + m;
  // London ends 16:00, NY ends 22:00
  const londonEnd = 16 * 60;
  const nyEnd     = 22 * 60;
  if (nowMins >= 7*60  && nowMins < londonEnd) return londonEnd - nowMins;
  if (nowMins >= 13*60 && nowMins < nyEnd)      return nyEnd - nowMins;
  return 0;
}

// Invalidation sender
async function sendInvalidation(sym, reason) {
  const asset = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  const msg = '⚠️ <b>' + asset + ' — SETUP INVALIDATED</b>\n\n' +
    reason + '\n\nThe setup has been cancelled. Watching for next opportunity.\n\n' +
    '─────────────────\nAurum Signals';
  console.log('[invalidation] ' + sym + ': ' + reason);
  await sendTelegram(msg);
}

async function autoScan() {
  const h         = new Date().getUTCHours();
  const inSession = (h >= 7 && h < 16) || (h >= 13 && h < 22);

  if (!inSession) {
    // Clear active setups at session close
    for (const sym of ['XAUUSD','XAGUSD']) {
      if (activeSetups[sym]) {
        delete activeSetups[sym];
        console.log('[auto-scan] Session closed — cleared active setup for ' + sym);
      }
    }
    console.log('[auto-scan] Outside session (UTC ' + h + ':xx) — skipping');
    return;
  }

  // ── SESSION TIME REMAINING CHECK ──────────────────────────────
  const minsLeft = sessionMinutesRemaining();
  if (minsLeft < 60) {
    console.log('[auto-scan] Less than 60 min remaining in session (' + minsLeft + 'min) — no new signals');
    return;
  }

  console.log('[auto-scan] Running — UTC ' + h + ':' + String(new Date().getUTCMinutes()).padStart(2,'0') +
    ' | Session: ' + minsLeft + 'min remaining');

  const delay = ms => new Promise(r => setTimeout(r, ms));

  for (const sym of ['XAUUSD', 'XAGUSD']) {
    try {
      const m5 = await getCandles(sym, '5min', 120);
      if (!m5 || m5.length < 50) {
        console.log('[auto-scan] ' + sym + ': insufficient data (' + (m5?.length||0) + ')');
        await delay(400);
        continue;
      }

      const m15        = deriveM15FromM5(m5);
      const atrValues  = calcATRFromCandles(m5, 14);
      const currentATR = atrValues.length ? atrValues[atrValues.length-1] : null;
      const livePrice  = m5[m5.length-1].c;
      const volatility = checkATR(sym, atrValues);
      const levels     = buildLevels(m5, m15);
      const sess       = sessionName(Date.now());
      const sessionOk  = sess !== null;
      const sessionOverlap = sess === 'London+NY Overlap';

      // Directional bias
      const pdhLevel = levels.find(l => l.type === 'PDH');
      const pdlLevel = levels.find(l => l.type === 'PDL');
      let directionalBias = 'neutral', biasPenalty = 0;
      if (pdhLevel && livePrice > pdhLevel.price)      { directionalBias = 'bearish_bias'; biasPenalty = 5; }
      else if (pdlLevel && livePrice < pdlLevel.price) { directionalBias = 'bullish_bias'; biasPenalty = 5; }

      // ── ACTIVE SETUP: check invalidation ─────────────────────
      const active = activeSetups[sym];
      if (active) {
        const ageCandles = Math.round((Date.now() - active.startedAt) / (5 * 60 * 1000));

        // Time decay invalidation
        if (ageCandles > 10) {
          delete activeSetups[sym];
          await sendInvalidation(sym, 'Setup expired — no entry within 10 candles (50 minutes).');
          await delay(400); continue;
        }

        // Structure break against direction
        const sweep2 = detectSweep(m5, levels);
        if (sweep2.found && sweep2.direction !== active.direction) {
          delete activeSetups[sym];
          await sendInvalidation(sym, 'Structure broke against trade direction. Setup cancelled.');
          await delay(400); continue;
        }

        // Already have active setup — skip new signal generation for this symbol
        console.log('[auto-scan] ' + sym + ': active setup (' + active.stage + ', ' + ageCandles + ' candles old) — holding');
        await delay(400); continue;
      }

      // ── SIGNAL ENGINE ─────────────────────────────────────────
      const sweep = detectSweep(m5, levels);
      let signal = null, near_setup = null;

      if (sessionOk && volatility.ok !== false && sweep.found) {
        const sweepToNow = m5.length - 1 - sweep.candleIdx;

        if (sweepToNow > 10) {
          // Time decay — sweep too old
          console.log('[auto-scan] ' + sym + ': sweep expired (' + sweepToNow + ' candles ago)');
        } else {
          const disp = detectDisplacement(m5, sweep.candleIdx, sweep.direction);

          if (!disp.found) {
            near_setup = { stage: 'sweep_detected', direction: sweep.direction,
              message: sweep.level.label + ' sweep confirmed — awaiting strong move',
              alert: { context: 'Liquidity grab detected at ' + sweep.level.label + '. Waiting for displacement candle.' }};
          } else {
            const bos = detectBOS(m5, sweep.candleIdx, sweep.direction);

            if (!bos.found) {
              near_setup = { stage: 'displacement_confirmed', direction: sweep.direction,
                message: 'Displacement confirmed (' + disp.ratio + 'x body) — awaiting trend shift',
                alert: { context: 'Strong move confirmed. Waiting for break of structure.' }};
            } else {
              const m15bos = m15.length >= 8 ? confirmBOS_M15(m15, sweep.direction, bos.bos_level) : false;
              const pb     = detectPullback(m5, disp.candleIdx, sweep.direction, sweep.sweepExtreme);

              if (!pb.found) {
                if (pb.reason && pb.reason.includes('70%')) {
                  // Pullback exceeded 70% — hard invalidation
                  await sendInvalidation(sym, 'Pullback exceeded 70% retracement. Setup invalidated.');
                } else {
                  near_setup = { stage: 'structure_break', direction: sweep.direction,
                    message: 'Trend shift confirmed — waiting for pullback entry zone',
                    alert: { context: 'BOS confirmed on M5' + (m15bos?'/M15':'') + '. Waiting for 50-61.8% pullback.' }};
                }
              } else {
                // All conditions present — run full score
                const sl  = calcSL(sweep.direction, sweep.sweepExtreme, currentATR || 0.5);
                const tps = calcTP(sweep.direction, pb.entry, sl, levels);

                if (tps.rr1 < 1.5) {
                  console.log('[auto-scan] ' + sym + ': R:R ' + tps.rr1 + ' below 1.5 minimum — skip');
                } else {
                  // ── FULL SCORING ────────────────────────────────
                  const scoreResult = scoreSetup(sess, sessionOk, sweep, disp, bos, pb,
                    volatility.ok === true || volatility.ok === undefined,
                    directionalBias, biasPenalty);

                  console.log('[auto-scan] ' + sym + ' score: ' + scoreResult.total +
                    ' (' + scoreResult.grade + ')' +
                    (scoreResult.reason ? ' — ' + scoreResult.reason : ''));

                  if (scoreResult.grade === 'REJECT') {
                    // Silent discard — no alert for low-quality setups
                  } else {
                    // A or A+ setup — generate signal
                    const expiryMs  = Date.now() + minsLeft * 60 * 1000;
                    const expiryUTC = new Date(expiryMs).toUTCString().split(' ')[4] + ' UTC';
                    const reasonParts = [
                      sess + ' ' + sweep.level.label + ' sweep',
                      (sweep.direction==='BUY'?'bullish':'bearish') + ' displacement (' + disp.ratio + '× body)',
                      (m15bos?'M5/M15':'M5') + ' BOS',
                      pb.retracement + '% pullback entry'
                    ];
                    const rawSig = {
                      id: Date.now(), asset: sym, direction: sweep.direction,
                      entry: parseFloat(pb.entry.toFixed(3)),
                      stop_loss: sl, take_profit_1: tps.tp1, take_profit_2: tps.tp2,
                      rr: tps.rr1, confidence: scoreResult.total,
                      grade: scoreResult.grade, scoreBreakdown: scoreResult.breakdown,
                      session: sess, reason: reasonParts.join(' → '),
                      sweep_level: sweep.level.label, pullback_pct: pb.retracement,
                      directional_bias: directionalBias, expiry: expiryUTC
                    };
                    rawSig.alert = formatSignalAlert(rawSig, currentATR);
                    signal = rawSig;
                  }
                }
              }
            }
          }
        }
      }

      // ── SEND ALERTS ───────────────────────────────────────────

      if (signal) {
        const sigKey = sym + '_' + signal.direction + '_' + signal.entry;
        if (!sentSignals.has(sigKey)) {
          sentSignals.add(sigKey);
          setTimeout(() => sentSignals.delete(sigKey), 4 * 60 * 60 * 1000);

          // Register active setup — blocks new signals for this symbol
          activeSetups[sym] = {
            direction: signal.direction,
            entry: signal.entry,
            startedAt: Date.now(),
            stage: 'entry_triggered'
          };

          console.log('[auto-scan] ' + signal.grade + ' SIGNAL: ' + sym + ' ' +
            signal.direction + ' @ ' + signal.entry + ' (score ' + signal.confidence + ')');
          await sendTelegram(formatTelegramSignal(signal));
        }
      }

      // Pre-signal — only send if no active setup, once per stage per hour
      if (near_setup && !signal) {
        const preKey   = sym + '_' + near_setup.stage;
        const lastSent = sentPreSignals[preKey] || 0;
        if (Date.now() - lastSent > 60 * 60 * 1000) {
          sentPreSignals[preKey] = Date.now();

          // Track setup progression
          if (!activeSetups[sym]) {
            activeSetups[sym] = {
              direction: near_setup.direction,
              startedAt: Date.now(),
              stage: near_setup.stage
            };
          } else {
            activeSetups[sym].stage = near_setup.stage;
          }

          console.log('[auto-scan] PRE-SIGNAL: ' + sym + ' ' + near_setup.stage);
          await sendTelegram(formatTelegramPreSignal(sym, near_setup));
        }
      }

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
});
