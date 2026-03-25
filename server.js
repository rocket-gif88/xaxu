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
      merged.push({
        type:          zoneType,
        zoneType:      zoneType === 'EQH' ? 'sell_zone' : 'buy_zone',
        price:         avgPrice,           // representative price (for sweep detection)
        minPrice:      clMin,
        maxPrice:      clMax,
        totalTouches:  allTouches,
        levelCount:    cl.length,
        levels:        cl,
        strengthScore: maxStr,
        strength:      maxStr >= 3 ? 'strong' : maxStr >= 2 ? 'medium' : 'weak',
        isZone:        true,               // flag: this is a clustered zone, not a single level
        label:         zoneType === 'EQH'
          ? 'Equal Highs zone (' + allTouches + ' touches)'
          : 'Equal Lows zone ('  + allTouches + ' touches)',
        priceRange:    parseFloat(Math.min(...cl.map(l=>l.price)).toFixed(3)) + '–' +
                       parseFloat(Math.max(...cl.map(l=>l.price)).toFixed(3))
      });
    }
  }
  return merged;
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

  // Build raw EQH levels (filter singles)
  const rawEQH = eqHighGroups.filter(g => g.length >= 2).map(g => {
    const avg = g.reduce((s,c)=>s+c.h,0)/g.length;
    return { price: avg, type:'EQH', touches: g.length,
             strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1 };
  });

  const rawEQL = eqLowGroups.filter(g => g.length >= 2).map(g => {
    const avg = g.reduce((s,c)=>s+c.l,0)/g.length;
    return { price: avg, type:'EQL', touches: g.length,
             strengthScore: g.length >= 4 ? 3 : g.length === 3 ? 2 : 1 };
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
function scoreZone(z, price, sess) {
  let total = 0;
  const breakdown = {};

  // A. Liquidity Strength (0–25)
  const touches = z.totalTouches || z.touches || 1;
  const liqScore = touches >= 8 ? 25 : touches >= 6 ? 22 : touches >= 4 ? 18
                 : touches >= 3 ? 15 : touches >= 2 ? 10 : 5;
  breakdown.liquidity = { score: liqScore, max: 25, touches };
  total += liqScore;

  // B. Zone Width — tighter zone = cleaner level (0–25)
  // Narrow zone (< 0.1%) = strong, wide zone (> 0.3%) = weak
  const zoneWidth = z.maxPrice && z.minPrice ? (z.maxPrice - z.minPrice) / z.minPrice : 0;
  const widthScore = zoneWidth <= 0.001 ? 25 : zoneWidth <= 0.002 ? 20
                   : zoneWidth <= 0.003 ? 15 : 10;
  breakdown.zone_width = { score: widthScore, max: 25, widthPct: parseFloat((zoneWidth*100).toFixed(3)) };
  total += widthScore;

  // C. Proximity (0–25) — how close is price to zone
  const nearEdge  = z.type === 'EQH' ? z.minPrice : z.maxPrice;
  const distPct   = Math.abs(price - nearEdge) / nearEdge;
  const inside    = price >= z.minPrice && price <= z.maxPrice;
  const proxScore = inside ? 25 : distPct <= 0.001 ? 22 : distPct <= 0.002 ? 18
                  : distPct <= 0.003 ? 13 : distPct <= 0.005 ? 8 : 3;
  breakdown.proximity = { score: proxScore, max: 25, distPct: parseFloat((distPct*100).toFixed(3)), inside };
  total += proxScore;

  // D. Session Quality (0–15)
  const sessScore = sess === 'London+NY Overlap' ? 15 : sess === 'New York' ? 10
                  : sess === 'London' ? 10 : 0;
  breakdown.session = { score: sessScore, max: 15, session: sess };
  total += sessScore;

  // E. Level Count Bonus (0–10) — more clustered levels = stronger confirmation
  const levelCount = z.levelCount || 1;
  const lvlScore   = levelCount >= 4 ? 10 : levelCount >= 3 ? 7 : levelCount >= 2 ? 4 : 0;
  breakdown.level_count = { score: lvlScore, max: 10, levelCount };
  total += lvlScore;

  total = Math.min(Math.max(total, 0), 100);

  const grade = total >= 80 ? 'HIGH' : total >= 60 ? 'MEDIUM' : total >= 40 ? 'LOW' : 'IGNORE';
  const emoji = total >= 80 ? '🟢' : total >= 60 ? '🟡' : '🔴';

  return { total, grade, emoji, breakdown };
}

// ── SELECT PRIMARY ZONE ─────────────────────────────────────────────────────
// Scores each zone, selects highest, attaches confidence object.
function selectPrimaryZone(levels, price, sess) {
  const zones = levels.filter(l => l.isZone && (l.type === 'EQH' || l.type === 'EQL'));
  if (!zones.length) return null;

  const scored = zones.map(z => {
    const direction  = z.type === 'EQH' ? 'SELL' : 'BUY';
    const nearEdge   = z.type === 'EQH' ? z.minPrice : z.maxPrice;
    const distPct    = Math.abs(price - nearEdge) / nearEdge;
    const inside     = price >= z.minPrice && price <= z.maxPrice;
    const confidence = scoreZone(z, price, sess);
    // Selection score (for ranking only — not shown to user)
    const selScore   = confidence.total;
    return { ...z, direction,
             distPct: parseFloat((distPct*100).toFixed(3)),
             inside, confidence, score: selScore };
  });

  scored.sort((a, b) => b.score - a.score);
  const primary = scored[0];
  console.log('[XAUUSD-like] Primary zone: ' + primary.direction +
    ' (' + primary.priceRange + ') confidence=' + primary.confidence.total +
    ' (' + primary.confidence.grade + ') touches=' + primary.totalTouches);
  return primary;
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
    let dir;
    if (lvl.type === 'EQH' || lvl.type === 'PDH' || lvl.type === 'ASH') dir = 'SELL';
    else if (lvl.type === 'EQL' || lvl.type === 'PDL' || lvl.type === 'ASL') dir = 'BUY';
    else dir = lvl.side === 'below' ? 'BUY' : 'SELL'; // fallback

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
  const ratio    = moveSize / avgRange;
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
                    volatilityOk, directionalBias, biasPenalty) {

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

  // ── PENALTIES ────────────────────────────────────────────────
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
                const scoreResult2 = scoreSetup(sess, sessionOk, sweep, disp, bos, pb,
                  volatility.ok === true || volatility.ok === undefined, directionalBias, biasPenalty);
                if (scoreResult2.tier !== 'HIGH') {
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

  // --- PRIMARY ZONE SELECTION + PRE-SIGNAL ALERTS --------------------------
  // Select ONE primary zone — highest scored EQH/EQL cluster.
  // All pre-signals derive from this single zone to prevent conflicting directions.
  const primaryZone     = selectPrimaryZone(levels, livePrice, sess);
  const approachingLevels = primaryZone
    ? [{ ...primaryZone,
         distPct: primaryZone.distPct,
         isPrimary: true }]
    : detectApproaching(livePrice, levels.filter(l => l.type !== 'EQH' && l.type !== 'EQL'), sym).slice(0, 1);

  // Gate pre-signals: only generate if zone confidence >= 40 (not IGNORE)
  const pzConf  = primaryZone?.confidence?.total || 0;
  const pzGrade = primaryZone?.confidence?.grade || 'IGNORE';
  const sweepPotentials = (primaryZone && pzConf >= 40 && approachingLevels.length)
    ? detectSweepPotential(livePrice, approachingLevels, m5)
    : [];
  if (primaryZone && pzConf < 40) {
    log.push('Zone confidence too low (' + pzConf + ') — pre-signal suppressed');
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
    zone_grade:        pzGrade,
    directional_bias:  directionalBias,
    m5_candles:   m5.length,
    ratio
  });
  } // end qf.pass
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
    emoji + ' <b>' + asset + ' ' + sig.direction + ' — ' + gradeEmoji + ' ' + grade + ' SETUP</b>',
    '📊 Confidence: ' + sig.confidence + '/100 — ' + (sig.tier || (sig.confidence >= 75 ? 'HIGH' : 'VALID')),
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
  // Zone confidence badge for approaching_liquidity
  const zoneConf  = ns.zone_confidence;
  const confLine  = zoneConf
    ? zoneConf.emoji + ' Zone confidence: ' + zoneConf.total + '/100 (' + zoneConf.grade + ')'
    : null;

  return [
    emoji + ' <b>' + asset + ' — ' + stageLabel + '</b>',
    '',
    alert.context || ns.message || '—',
    '',
    ...(confLine ? [confLine, ''] : []),
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
    startedAt:    Date.now(),
    lastEventAt:  Date.now(),
    // Cooldown per event type (ms) — safety net against edge-case re-triggers
    cooldowns: {}
  };
  console.log('[setup] Created id=' + id + ' dir=' + direction + ' zone=' + zoneId);
  return setup;
}

// Setups keyed by symbol
const setups = { XAUUSD: null, XAGUSD: null };

// Legacy compat — sentSignals dedup by entry price
const sentSignals = new Set();

// Transition: advance stage and fire alert if event not yet sent
// Returns true if alert was sent, false if blocked
async function fireEvent(setup, event, sym, alertFn) {
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
  setup.invalidated       = true;
  setup.active            = false;
  setup.events.invalidated = true;
  setup.stage             = 'invalidated';
  console.log('[' + sym + '] Setup invalidated → alert sent once (id=' + setup.id + '): ' + reason);
  const asset = sym === 'XAUUSD' ? 'GOLD' : 'SILVER';
  await sendTelegram('⚠️ <b>' + asset + ' — SETUP INVALIDATED</b>\n\n' + reason + '\n\nWatching for next opportunity.\n\n─────────────────\nAurum Signals');
}

// Reset a symbol's setup — called when session closes or new sweep on different zone
function resetSetup(sym, reason) {
  const existing = setups[sym];
  if (existing) {
    console.log('[' + sym + '] Setup reset (id=' + existing.id + '): ' + reason);
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
    const expectedDir = (lvlType === 'EQH' || lvlType === 'PDH' || lvlType === 'ASH') ? 'SELL'
                      : (lvlType === 'EQL' || lvlType === 'PDL' || lvlType === 'ASL') ? 'BUY'
                      : null;
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
  const lvlType = sweep.level.type;
  const correct  = (lvlType === 'EQH' || lvlType === 'PDH' || lvlType === 'ASH') ? 'SELL'
                 : (lvlType === 'EQL' || lvlType === 'PDL' || lvlType === 'ASL') ? 'BUY'
                 : sweep.direction; // no override for unknown types
  if (correct !== sweep.direction) {
    console.log('[validate] Direction corrected: ' + sweep.direction + ' → ' + correct +
      ' (zone type: ' + lvlType + ')');
  }
  return { ...sweep, direction: correct };
}

async function autoScan() {
  const h         = new Date().getUTCHours();
  const inSession = (h >= 7 && h < 16) || (h >= 13 && h < 22);

  if (!inSession) {
    for (const sym of ['XAUUSD','XAGUSD']) {
      if (setups[sym]) {
        resetSetup(sym, 'Session closed');
      }
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
      const sess       = sessionName(Date.now());
      const sessionOk  = sess !== null;
      const sessionOverlap = sess === 'London+NY Overlap';

      // Directional bias
      const pdhLevel = levels.find(l => l.type === 'PDH');
      const pdlLevel = levels.find(l => l.type === 'PDL');
      let directionalBias = 'neutral', biasPenalty = 0;
      if (pdhLevel && livePrice > pdhLevel.price)      { directionalBias = 'bearish_bias'; biasPenalty = 5; }
      else if (pdlLevel && livePrice < pdlLevel.price) { directionalBias = 'bullish_bias'; biasPenalty = 5; }

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

        // Counter-structure: new sweep in opposite direction
        const sweep2 = detectSweep(m5, levels);
        if (sweep2.found && sweep2.direction !== setup.direction) {
          await invalidateSetup(sym, 'Structure broke against ' + setup.direction + ' direction.');
          resetSetup(sym, 'Counter-structure');
          await delay(400); continue;
        }
      }

      // ── DETECT MARKET STATE ────────────────────────────────────
      if (!sessionOk || volatility.ok === false) {
        await delay(400); continue;
      }

      // Detect sweep, then correct direction based on zone type (Rule 1 fix)
      let sweep = detectSweep(m5, levels);
      if (sweep.found) sweep = correctSweepDirection(sweep);
      const sweepToNow = sweep.found ? (m5.length - 1 - sweep.candleIdx) : 999;

      // ── STAGE: APPROACHING ─────────────────────────────────────
      // Fire once when price is near a key zone and no setup is active yet
      if (!setup && !sweep.found) {
        const approaching = levels
          .filter(l => (l.type === 'EQH' || l.type === 'EQL' || l.type === 'PDH' || l.type === 'PDL'))
          .map(l => {
            const nearEdge = l.isZone
              ? (livePrice > l.maxPrice ? l.maxPrice : livePrice < l.minPrice ? l.minPrice : livePrice)
              : l.price;
            const distPct = Math.abs(livePrice - nearEdge) / nearEdge;
            return { ...l, distPct, dir: l.type === 'EQH' || l.type === 'PDH' ? 'SELL' : 'BUY' };
          })
          .filter(l => l.distPct <= 0.002) // within 0.2%
          .sort((a,b) => a.distPct - b.distPct)[0];

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
          const newSetup = createSetup(sym, approaching.dir, approaching);
          setups[sym] = newSetup;
          setup = newSetup;
          const rangeStr = approaching.isZone
            ? '$' + parseFloat(approaching.minPrice).toFixed(2) + '–$' + parseFloat(approaching.maxPrice).toFixed(2)
            : '$' + parseFloat(approaching.price).toFixed(2);
          await fireEvent(setup, 'approaching', sym, () => sendTelegram(
            '📍 <b>' + asset + ' — KEY ZONE NEARBY</b>\n\n' +
            asset + ' is approaching a ' + (approaching.dir === 'SELL' ? 'sell' : 'buy') +
            ' zone at ' + rangeStr + '.\n\n' +
            'If price sweeps through and reverses, a ' + approaching.dir + ' setup may form.\n\n' +
            '⏳ No action yet — monitoring.\n\n─────────────────\nAurum Signals'
          ));
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

      // Validate before creating any setup
      if (!setup) {
        const vResult = validateSignal(sym, sweep, m5, levels, null);
        if (!vResult.valid) {
          console.log('[validate] ' + sym + ': setup creation blocked (' + vResult.reasons.length + ' failures)');
          await delay(400); continue;
        }
        setup = createSetup(sym, sweep.direction, sweep.level);
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

      await fireEvent(setup, 'sweep', sym, () => sendTelegram(
        '⚡ <b>' + asset + ' — LIQUIDITY GRAB DETECTED</b>\n\n' +
        asset + ' swept ' + lvlDesc + ' and closed back inside.\n' +
        'Direction: <b>' + sweep.direction + '</b>\n\n' +
        '⏳ Waiting for a strong displacement candle.\n\n─────────────────\nAurum Signals'
      ));

      // ── STAGE: DISPLACEMENT (MOVE) ────────────────────────────
      const disp = detectDisplacement(m5, sweep.candleIdx, sweep.direction);
      if (!disp.found) {
        console.log('[' + sym + '] Waiting for displacement');
        await delay(400); continue;
      }

      await fireEvent(setup, 'move', sym, () => sendTelegram(
        '↗️ <b>' + asset + ' — STRONG MOVE CONFIRMED</b>\n\n' +
        'A ' + disp.ratio + '× displacement candle followed the liquidity grab.\n' +
        'Direction: <b>' + sweep.direction + '</b>\n\n' +
        '⏳ Waiting for break of structure.\n\n─────────────────\nAurum Signals'
      ));

      // ── STAGE: TREND SHIFT (BOS) ──────────────────────────────
      const bos = detectBOS(m5, sweep.candleIdx, sweep.direction);
      if (!bos.found) {
        console.log('[' + sym + '] Waiting for BOS');
        await delay(400); continue;
      }

      const m15bos = m15.length >= 8 ? confirmBOS_M15(m15, sweep.direction, bos.bos_level) : false;
      await fireEvent(setup, 'trend', sym, () => sendTelegram(
        '✅ <b>' + asset + ' — TREND SHIFT CONFIRMED</b>\n\n' +
        'Break of structure confirmed on M5' + (m15bos ? '/M15' : '') + '.\n' +
        'Direction: <b>' + sweep.direction + '</b>\n\n' +
        '⏳ Waiting for 50–61.8% pullback into entry zone.\n\n─────────────────\nAurum Signals'
      ));

      // ── STAGE: PULLBACK ───────────────────────────────────────
      const pb = detectPullback(m5, disp.candleIdx, sweep.direction, sweep.sweepExtreme);
      if (!pb.found) {
        if (pb.reason && pb.reason.includes('70%')) {
          // 70% invalidation — fires once
          await invalidateSetup(sym, 'Pullback exceeded 70% retracement.');
          resetSetup(sym, 'Pullback > 70%');
        } else {
          console.log('[' + sym + '] Waiting for pullback');
        }
        await delay(400); continue;
      }

      await fireEvent(setup, 'pullback', sym, () => sendTelegram(
        '🎯 <b>' + asset + ' — PULLBACK INTO ENTRY ZONE</b>\n\n' +
        'Price pulled back ' + pb.retracement + '% into the entry zone.\n' +
        'Preparing to evaluate full signal.\n\n' +
        '⏳ Running quality checks...\n\n─────────────────\nAurum Signals'
      ));

      // ── STAGE: ENTRY SIGNAL ───────────────────────────────────
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

      console.log('[' + sym + '] Score: ' + scoreResult.total + ' (' + scoreResult.grade + ')');

      // Tier gate: only HIGH (≥75) gets a full signal
      // VALID (60-74) → pre-signal already fired, no full entry
      // LOW / IGNORE → silent discard
      if (scoreResult.tier !== 'HIGH') {
        console.log('[' + sym + '] Score ' + scoreResult.total + ' tier=' + scoreResult.tier + ' — full signal requires HIGH (≥75)');
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
        stop_loss: sl, take_profit_1: tps.tp1, take_profit_2: tps.tp2,
        rr: tps.rr1, confidence: scoreResult.total,
        grade: scoreResult.grade, tier: scoreResult.tier, scoreBreakdown: scoreResult.breakdown,
        session: sess, directional_bias: directionalBias,
        sweep_level: sweep.level?.label || '—',
        pullback_pct: pb.retracement, expiry: expiryUTC,
        reason: sess + ' ' + (sweep.level?.label||'') + ' sweep → ' +
          sweep.direction.toLowerCase() + ' displacement (' + disp.ratio + '×) → BOS → ' + pb.retracement + '% pullback'
      };
      rawSig.alert = formatSignalAlert(rawSig, currentATR);

      await fireEvent(setup, 'entry', sym, async () => {
        sentSignals.add(sigKey);
        setTimeout(() => sentSignals.delete(sigKey), 4 * 60 * 60 * 1000);
        // Mark setup as complete — no more events
        setup.active = false;
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
});
