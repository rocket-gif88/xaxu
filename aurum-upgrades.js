// ═══════════════════════════════════════════════════════════════════════════
// AURUM v5.2 — UPGRADE PATCH
// Drop alongside server.js · require at top of server.js
//
// Adds:
//   1. Persistent logging    — Sheets hydration on boot (survives Railway restarts)
//   2. Zone memory           — fresh vs exhausted zone tracking
//   3. VWAP reclaim          — post-sweep confirmation filter
//   4. ATR position sizing   — per-signal lot size in Telegram alerts
//   5. Session quality gate  — scores ASIA vs London vs NY vs Overlap
//
// Integration: see INTEGRATION GUIDE at bottom of this file.
// ═══════════════════════════════════════════════════════════════════════════

'use strict';

// ──────────────────────────────────────────────────────────────────────────
// 1. PERSISTENT LOGGING — Sheets hydration on boot
// ──────────────────────────────────────────────────────────────────────────
// Called once in app.listen callback.
// Reads last 24h of ENTRY rows from Google Sheets → restores _setupLogs.
// After a Railway restart the in-memory log store is repopulated so /stats
// and the feedback loop survive deployments.
// ──────────────────────────────────────────────────────────────────────────

async function hydrateFromSheets(setupLogs) {
  const sheetId   = process.env.GOOGLE_SHEET_ID;
  const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;

  if (!sheetId || !credsJson) {
    console.log('[hydrate] Sheets not configured — skipping boot hydration');
    return;
  }

  try {
    const { google } = require('googleapis');
    const creds = JSON.parse(credsJson);
    const auth  = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    const resp = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range:         'Aurum!A:R',
    });

    const rows   = resp.data.values || [];
    const cutoff = Date.now() - 24 * 60 * 60 * 1000; // last 24h only
    let restored = 0;

    for (const row of rows.slice(1)) { // skip header row
      const [ts, id, sym, dir, sess,
             zLow, zHigh, zScore, touches,
             event, entry, sl, tp1, tp2,
             candles, result, , stages] = row;

      if (!ts || !id || !event) continue;

      const rowTime = new Date(ts).getTime();
      if (isNaN(rowTime) || rowTime < cutoff) continue;

      // Restore ENTRY rows — these are the actionable records
      if (event === 'ENTRY' && !setupLogs[id]) {
        setupLogs[id] = {
          id,
          symbol:        sym    || '',
          direction:     dir    || '',
          session:       sess   || '',
          zone: {
            low:    parseFloat(zLow)   || null,
            high:   parseFloat(zHigh)  || null,
            score:  parseFloat(zScore) || null,
            touches: parseInt(touches) || null,
          },
          entryTriggered:  true,
          entryPrice:      parseFloat(entry)   || null,
          stopLoss:        parseFloat(sl)      || null,
          takeProfits:     [tp1, tp2].filter(Boolean).map(Number),
          candlesToEntry:  parseInt(candles)   || null,
          result:          result              || null,
          timestamp_start: ts,
          stages:          stages ? stages.split(',').reduce((o,k) => { o[k]=true; return o; }, {}) : {},
          _version:        1,
          _restored:       true,   // flag: came from Sheets, not live
        };
        restored++;
      }

      // Also restore RESULT rows for already-restored setups
      if (event === 'RESULT' && setupLogs[id] && result) {
        setupLogs[id].result = result;
      }
    }

    console.log('[hydrate] ✓ Restored ' + restored + ' entry log(s) from Sheets (last 24h)');

  } catch (e) {
    // Never crash on hydration failure — it's cosmetic
    console.error('[hydrate] Failed (non-fatal):', e.message);
  }
}


// ──────────────────────────────────────────────────────────────────────────
// 2. ZONE MEMORY — fresh vs exhausted tracking
// ──────────────────────────────────────────────────────────────────────────
// Tracks how many times each zone has been "activated" (a setup was created
// at that zone) within the current session and across restarts.
//
// KEY INSIGHT: A zone loses institutional order flow with each retest.
//   Fresh  (0–1 tests) → full signal allowed
//   Tested (2 tests)   → signal allowed but confidence docked
//   Exhausted (3+)     → signal suppressed (no unfilled orders remain)
//
// Persisted per-session in memory. At session close, zones are aged out.
// ──────────────────────────────────────────────────────────────────────────

const zoneMemory = {
  XAUUSD: {},  // priceRange → { firstSeenAt, touchCount, lastTouchedAt }
  XAGUSD: {},
};

function updateZoneMemory(sym, zone) {
  if (!zone || !zone.priceRange) return;
  const key = zone.priceRange;
  const mem = zoneMemory[sym];
  if (!mem[key]) {
    mem[key] = {
      firstSeenAt:   Date.now(),
      touchCount:    0,
      lastTouchedAt: 0,
      direction:     zone.direction || null,
    };
  }
  mem[key].touchCount++;
  mem[key].lastTouchedAt = Date.now();
  console.log('[zone-mem] ' + sym + ' zone ' + key +
    ' touch #' + mem[key].touchCount);
}

// Returns freshness info for a zone. Used in Telegram alerts and score gating.
function getZoneFreshness(sym, zone) {
  if (!zone || !zone.priceRange) {
    return { fresh: true, touchCount: 0, label: '🟢 FRESH', suppress: false };
  }
  const key   = zone.priceRange;
  const entry = zoneMemory[sym]?.[key];

  if (!entry) {
    return { fresh: true, touchCount: 0, label: '🟢 FRESH', suppress: false };
  }

  const count = entry.touchCount;

  if (count <= 1) return { fresh: true,  touchCount: count, label: '🟢 FRESH (1st test)',           suppress: false };
  if (count === 2) return { fresh: false, touchCount: count, label: '🟡 RETESTED (2nd test)',         suppress: false };
  if (count === 3) return { fresh: false, touchCount: count, label: '🟠 WEAKENED (3rd test)',          suppress: false };
  return             { fresh: false, touchCount: count, label: '🔴 EXHAUSTED (' + count + ' tests)', suppress: true  };
}

// Call at session close to clear per-session zone memory
function clearZoneMemory(sym) {
  zoneMemory[sym] = {};
  console.log('[zone-mem] ' + sym + ': zone memory cleared (session close)');
}


// ──────────────────────────────────────────────────────────────────────────
// 3. VWAP RECLAIM — post-sweep confirmation
// ──────────────────────────────────────────────────────────────────────────
// After a liquidity sweep, we check whether price has reclaimed the session
// VWAP. A VWAP reclaim after a BUY sweep = genuine institutional demand.
// Failure to reclaim = likely trap / continuation lower.
//
// VWAP here is range-weighted (no free-tier volume on XAU/USD) — standard
// proxy used by retail prop firms and effective on 5M gold charts.
// ──────────────────────────────────────────────────────────────────────────

// Calculate session VWAP from a candle array (range-weighted typical price)
function calcVWAP(candles) {
  if (!candles || candles.length === 0) return null;
  let cumTPV = 0;
  let cumVol = 0;

  for (const c of candles) {
    const tp  = (c.h + c.l + c.c) / 3;  // typical price
    const vol = Math.max(c.h - c.l, 0.0001); // range as volume proxy
    cumTPV += tp * vol;
    cumVol += vol;
  }

  return cumVol > 0 ? cumTPV / cumVol : null;
}

// Check whether price has reclaimed VWAP after a sweep
// sweepIdx: candle index of the sweep
// direction: 'BUY' or 'SELL'
// Returns { reclaimed, vwap, candlesAfterSweep, note }
function detectVWAPReclaim(candles, sweepIdx, direction) {
  if (!candles || sweepIdx < 0 || sweepIdx >= candles.length) {
    return { reclaimed: false, vwap: null, note: 'insufficient data' };
  }

  // Use candles up to and including sweep for VWAP calculation
  const vwap = calcVWAP(candles.slice(0, sweepIdx + 1));
  if (!vwap) return { reclaimed: false, vwap: null, note: 'VWAP calc failed' };

  // Check candles AFTER the sweep
  const postSweep = candles.slice(sweepIdx + 1);
  if (postSweep.length === 0) {
    return { reclaimed: false, vwap, note: 'no candles after sweep yet' };
  }

  // BUY sweep: we need at least one post-sweep candle to close ABOVE VWAP
  // SELL sweep: at least one post-sweep candle to close BELOW VWAP
  let reclaimed = false;
  for (const c of postSweep) {
    if (direction === 'BUY'  && c.c > vwap) { reclaimed = true; break; }
    if (direction === 'SELL' && c.c < vwap) { reclaimed = true; break; }
  }

  const note = reclaimed
    ? 'Price reclaimed VWAP ($' + vwap.toFixed(2) + ') after sweep ✓'
    : 'Price has NOT reclaimed VWAP ($' + vwap.toFixed(2) + ') — weak follow-through';

  return {
    reclaimed,
    vwap:              parseFloat(vwap.toFixed(3)),
    candlesAfterSweep: postSweep.length,
    note,
  };
}


// ──────────────────────────────────────────────────────────────────────────
// 4. ATR POSITION SIZING — per-signal lot recommendation
// ──────────────────────────────────────────────────────────────────────────
// Gold standard lot sizing: 1 standard lot XAU/USD = 100 oz = $100/pip ($1)
// Position size formula:  lots = (accountRisk$) / (stopDistance$ × $100)
// Example: $10,000 × 1% risk / ($5.00 stop × $100) = 0.20 lots
//
// Three account tiers shown in every signal: $10k · $50k · $100k
// ──────────────────────────────────────────────────────────────────────────

const RISK_PCT    = 1;      // 1% risk per trade
const PIP_VALUE   = 100;    // $100 per full lot per $1 move on XAU/USD

function calcLots(accountSize, entryPrice, stopLoss) {
  const stopDist = Math.abs(entryPrice - stopLoss);
  if (!stopDist || !accountSize) return null;
  const riskAmount = accountSize * (RISK_PCT / 100);
  const lots = riskAmount / (stopDist * PIP_VALUE);
  return parseFloat(Math.max(lots, 0.01).toFixed(2));
}

// Returns the formatted ATR block string for insertion into Telegram signal
function formatATRBlock(currentATR, entryPrice, stopLoss) {
  if (!currentATR || !entryPrice || !stopLoss) return '';

  const stopDist    = Math.abs(entryPrice - stopLoss);
  const atrMultiple = (stopDist / currentATR).toFixed(1);

  const lot10k  = calcLots(10000,  entryPrice, stopLoss);
  const lot50k  = calcLots(50000,  entryPrice, stopLoss);
  const lot100k = calcLots(100000, entryPrice, stopLoss);

  return [
    '',
    '<b>📊 POSITION SIZING (1% risk)</b>',
    'ATR: $' + currentATR.toFixed(2) + '  |  Stop dist: $' +
      stopDist.toFixed(2) + ' (' + atrMultiple + '× ATR)',
    '$10k  → ' + lot10k  + ' lots',
    '$50k  → ' + lot50k  + ' lots',
    '$100k → ' + lot100k + ' lots',
  ].join('\n');
}


// ──────────────────────────────────────────────────────────────────────────
// 5. SESSION QUALITY GATE
// ──────────────────────────────────────────────────────────────────────────
// Returns structured session quality. Used for logging and signal labelling.
// The trading engine already blocks outside London/NY — this adds context
// and a quality score that flows into the Telegram alert.
// ──────────────────────────────────────────────────────────────────────────

function getSessionQuality(utcHour) {
  const isLondon  = utcHour >= 7  && utcHour < 16;
  const isNY      = utcHour >= 13 && utcHour < 22;
  const isOverlap = isLondon && isNY;

  if (isOverlap) return { ok: true, quality: 'HIGH',   score: 10, label: 'London+NY Overlap ⭐' };
  if (isNY)      return { ok: true, quality: 'MEDIUM', score: 7,  label: 'New York' };
  if (isLondon)  return { ok: true, quality: 'MEDIUM', score: 5,  label: 'London' };

  // Asia / dead zone — already blocked by autoScan but return for logging
  return { ok: false, quality: 'LOW', score: 0, label: 'Asia/Off-Hours — signals suppressed' };
}


// ──────────────────────────────────────────────────────────────────────────
// EXPORTS
// ──────────────────────────────────────────────────────────────────────────

module.exports = {
  // 1. Persistence
  hydrateFromSheets,

  // 2. Zone memory
  zoneMemory,
  updateZoneMemory,
  getZoneFreshness,
  clearZoneMemory,

  // 3. VWAP
  calcVWAP,
  detectVWAPReclaim,

  // 4. ATR sizing
  calcLots,
  formatATRBlock,

  // 5. Session quality
  getSessionQuality,
};


// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATION GUIDE — 6 changes to server.js
// ═══════════════════════════════════════════════════════════════════════════
//
// ── STEP 1: Require this module at the top of server.js ─────────────────
// Add after line 4 (after `const app = express();`):
//
//   const upgrades = require('./aurum-upgrades');
//   const { hydrateFromSheets, updateZoneMemory, getZoneFreshness,
//           clearZoneMemory, detectVWAPReclaim, formatATRBlock,
//           getSessionQuality, zoneMemory } = upgrades;
//
//
// ── STEP 2: Boot hydration — restore logs after Railway restart ──────────
// Add inside app.listen callback, after console.log('[scheduler]...'):
//
//   hydrateFromSheets(_setupLogs).catch(e => console.error('[boot]', e.message));
//
//
// ── STEP 3: Zone memory — update on every primaryZone selection ──────────
// In autoScan(), find the block after `const primaryZone = selectPrimaryZone(...)`.
// Add immediately after:
//
//   updateZoneMemory(sym, primaryZone);
//   const zoneFreshness = getZoneFreshness(sym, primaryZone);
//   if (zoneFreshness.suppress) {
//     console.log('[zone-mem] ' + sym + ': zone EXHAUSTED — signal suppressed (' + zoneFreshness.label + ')');
//     await delay(400); continue;
//   }
//   if (!zoneFreshness.fresh) {
//     console.log('[zone-mem] ' + sym + ': zone ' + zoneFreshness.label + ' (reduced confidence)');
//   }
//
//
// ── STEP 4: Clear zone memory at session close ───────────────────────────
// In autoScan(), inside the `if (!inSession)` block, alongside the existing
// resetSetup and bias resets:
//
//   clearZoneMemory(sym);
//
//
// ── STEP 5: VWAP reclaim — add after displacement confirmed ─────────────
// In autoScan(), find the block after `if (moveFired) { ... continue; }`.
// Add before the BOS detection block:
//
//   const vwapCheck = detectVWAPReclaim(m5, sweep.candleIdx, sweep.direction);
//   console.log('[vwap] ' + sym + ': ' + vwapCheck.note);
//   if (!vwapCheck.reclaimed && m5.length - sweep.candleIdx > 3) {
//     console.log('[vwap] ' + sym + ': no VWAP reclaim after 3 candles — soft block (logged)');
//     // Soft block: log but do not hard-stop (observe for 2 weeks before hardening)
//   }
//
//
// ── STEP 6: ATR block in Telegram signal ─────────────────────────────────
// In formatTelegramSignal(), find the return array (around line 2518).
// Replace the '' before '<b>CONFIDENCE...' with:
//
//   formatATRBlock(sig.atr, entryPrice, sl),
//
// Also pass atr into rawSig in autoScan (it's already in scope as currentATR):
//   In the rawSig object (around line 3926), add:
//   atr: currentATR,
//
// ── STEP 7: Zone freshness in Telegram signal (optional but recommended) ─
// In formatTelegramSignal(), add to the reasons array:
//
//   if (sig.zoneFreshness) reasons.push('• Zone: ' + sig.zoneFreshness);
//
// Also add to rawSig:
//   zoneFreshness: zoneFreshness?.label || null,
//
// ═══════════════════════════════════════════════════════════════════════════
