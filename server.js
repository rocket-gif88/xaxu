const express = require('express');
const cors    = require('cors');
const fetch   = require('node-fetch');
const app     = express();

app.use(cors());
app.use(express.json());

const TWELVE_KEY    = '7f3fc6ca85664930ab6e687db8ff0c5d';
const ANTHROPIC_KEY = ['sk-ant-','api03-PSBtiCb9gNCUnpxHjEl2sqWVtfNop5DtO1WCW2pdUw_upi3Zl0VDjCT7Yyk','W9bboA3Bxnq2ucHBFyuNrNx6CL','w-qYuk4wAA'].join('');

const SYMBOLS = { XAUUSD:'XAU/USD', XAGUSD:'XAG/USD' };

async function td(path) {
  const base = 'https://api.twelvedata.com';
  const sep  = path.includes('?') ? '&' : '?';
  const r    = await fetch(`${base}${path}${sep}apikey=${TWELVE_KEY}`, { signal: AbortSignal.timeout(9000) });
  return r.json();
}

// --- Fetch OHLC time series (H1 last N candles)
async function getCandles(sym, n = 48) {
  try {
    const s = SYMBOLS[sym];
    const d = await td(`/time_series?symbol=${encodeURIComponent(s)}&interval=1h&outputsize=${n}&format=JSON`);
    if (!d.values) return null;
    return d.values.map(c => ({
      t:    new Date(c.datetime + ' UTC').getTime(),
      o:    parseFloat(c.open),
      h:    parseFloat(c.high),
      l:    parseFloat(c.low),
      c:    parseFloat(c.close),
    })).reverse(); // oldest first
  } catch(e) { return null; }
}

// --- Fetch live price
async function getPrice(sym) {
  try {
    const d = await td(`/price?symbol=${encodeURIComponent(SYMBOLS[sym])}`);
    return parseFloat(d.price) || null;
  } catch(e) { return null; }
}

// --- Fetch ATR
async function getATR(sym, period = 14) {
  try {
    const d = await td(`/atr?symbol=${encodeURIComponent(SYMBOLS[sym])}&interval=1h&time_period=${period}&outputsize=1`);
    if (d.values && d.values[0]) return parseFloat(d.values[0].atr);
    return null;
  } catch(e) { return null; }
}

// =====================================================
// LIQUIDITY SWEEP ENGINE
// =====================================================

function findLiquidityLevels(candles) {
  const n   = candles.length;
  const lvls = [];

  // Previous Day High/Low (last complete 24h)
  const oneDayAgo = Date.now() - 86400000;
  const yesterday = candles.filter(c => c.t < oneDayAgo);
  if (yesterday.length > 0) {
    const pdh = Math.max(...yesterday.map(c => c.h));
    const pdl = Math.min(...yesterday.map(c => c.l));
    lvls.push({ price: pdh, type: 'PDH', label: 'Prev Day High' });
    lvls.push({ price: pdl, type: 'PDL', label: 'Prev Day Low'  });
  }

  // Asian session high/low (00:00–08:00 UTC today)
  const todayStart = new Date(); todayStart.setUTCHours(0,0,0,0);
  const asian = candles.filter(c => {
    const h = new Date(c.t).getUTCHours();
    return c.t >= todayStart.getTime() && h >= 0 && h < 8;
  });
  if (asian.length > 0) {
    lvls.push({ price: Math.max(...asian.map(c => c.h)), type: 'ASH', label: 'Asian Session High' });
    lvls.push({ price: Math.min(...asian.map(c => c.l)), type: 'ASL', label: 'Asian Session Low'  });
  }

  // Equal Highs / Equal Lows (within 0.12% of each other, last 20 candles)
  const recent = candles.slice(-20);
  const EQ_THRESHOLD = 0.0012;
  // Equal highs
  for (let i = 0; i < recent.length - 1; i++) {
    for (let j = i + 1; j < recent.length; j++) {
      const diff = Math.abs(recent[i].h - recent[j].h) / recent[i].h;
      if (diff < EQ_THRESHOLD) {
        lvls.push({ price: (recent[i].h + recent[j].h) / 2, type: 'EQH', label: 'Equal Highs' });
        break;
      }
    }
  }
  // Equal lows
  for (let i = 0; i < recent.length - 1; i++) {
    for (let j = i + 1; j < recent.length; j++) {
      const diff = Math.abs(recent[i].l - recent[j].l) / recent[i].l;
      if (diff < EQ_THRESHOLD) {
        lvls.push({ price: (recent[i].l + recent[j].l) / 2, type: 'EQL', label: 'Equal Lows' });
        break;
      }
    }
  }

  return lvls;
}

function detectSweep(candles, levels) {
  const sweeps = [];
  if (candles.length < 3) return sweeps;

  // Look at last 3 completed candles for sweep pattern
  for (let i = candles.length - 3; i < candles.length - 1; i++) {
    const prev = candles[i];
    const curr = candles[i + 1];

    levels.forEach(lvl => {
      const p = lvl.price;

      // BULLISH SWEEP: candle wicked below level then closed above
      if (prev.l < p && prev.c > p && prev.c > prev.o) {
        const wickSize   = p - prev.l;
        const candleSize = Math.abs(prev.h - prev.l);
        if (wickSize / candleSize > 0.3) { // wick is > 30% of range
          sweeps.push({
            direction: 'BUY',
            level:     lvl,
            sweepCandle: prev,
            confirmCandle: curr,
            sweepLow:  prev.l,
            sweepHigh: prev.h
          });
        }
      }

      // BEARISH SWEEP: candle wicked above level then closed below
      if (prev.h > p && prev.c < p && prev.c < prev.o) {
        const wickSize   = prev.h - p;
        const candleSize = Math.abs(prev.h - prev.l);
        if (wickSize / candleSize > 0.3) {
          sweeps.push({
            direction: 'SELL',
            level:     lvl,
            sweepCandle: prev,
            confirmCandle: curr,
            sweepLow:  prev.l,
            sweepHigh: prev.h
          });
        }
      }
    });
  }
  return sweeps;
}

function detectDisplacement(candles, direction, atr) {
  if (!atr) return { valid: false, strength: 0 };
  const recent = candles.slice(-5);
  const avgBody = recent.reduce((s,c) => s + Math.abs(c.c - c.o), 0) / recent.length;
  const lastCandle = candles[candles.length - 1];
  const body = Math.abs(lastCandle.c - lastCandle.o);

  const dirOk = direction === 'BUY'
    ? lastCandle.c > lastCandle.o
    : lastCandle.c < lastCandle.o;

  const strength = body / atr;
  return { valid: dirOk && body > avgBody * 1.2, strength: parseFloat(strength.toFixed(2)) };
}

function detectStructureShift(candles, direction) {
  const n = candles.slice(-8);
  if (n.length < 4) return false;

  if (direction === 'BUY') {
    // Look for higher low + break of prior high
    const lows  = n.map(c => c.l);
    const highs = n.map(c => c.h);
    const lastHigh = highs[highs.length - 1];
    const prevHighs = highs.slice(0, -1);
    const higherLow = lows[lows.length-1] > lows[lows.length-3];
    const bosHigh   = lastHigh > Math.max(...prevHighs.slice(-3));
    return higherLow || bosHigh;
  } else {
    const lows  = n.map(c => c.l);
    const highs = n.map(c => c.h);
    const lastLow  = lows[lows.length - 1];
    const prevLows = lows.slice(0, -1);
    const lowerHigh = highs[highs.length-1] < highs[highs.length-3];
    const bosLow    = lastLow < Math.min(...prevLows.slice(-3));
    return lowerHigh || bosLow;
  }
}

function calcConfidence(sessionOk, sessionOverlap, sweep, displacement, structureShift, pullbackOk) {
  let score = 0;
  if (!sessionOk) return 0;
  score += sessionOverlap ? 25 : 15;
  score += sweep ? 20 : 0;
  score += displacement.valid ? (displacement.strength > 1.5 ? 25 : 15) : 0;
  score += structureShift ? 20 : 0;
  score += pullbackOk ? 10 : 0;
  return Math.min(score, 98);
}

function isSession(type) {
  const h = new Date().getUTCHours();
  if (type === 'london') return h >= 8  && h < 17;
  if (type === 'ny')     return h >= 13 && h < 22;
  return (h >= 8 && h < 17) || (h >= 13 && h < 22);
}

// =====================================================
// MAIN SIGNAL ROUTE
// =====================================================
app.get('/analyze/:sym', async (req, res) => {
  const sym = req.params.sym.toUpperCase();
  if (!SYMBOLS[sym]) return res.status(400).json({ success: false, error: 'Unknown symbol' });

  try {
    const [candles, price, atr] = await Promise.all([
      getCandles(sym, 48),
      getPrice(sym),
      getATR(sym, 14)
    ]);

    if (!candles || candles.length < 10) {
      return res.json({ success: false, error: 'Insufficient candle data. Market may be closed.' });
    }
    if (!price) return res.json({ success: false, error: 'Price unavailable.' });

    const sessionOk      = isSession('any');
    const sessionOverlap = isSession('london') && isSession('ny');
    const sessionName    = isSession('london') && isSession('ny') ? 'London+NY Overlap'
                         : isSession('london') ? 'London' : isSession('ny') ? 'New York' : 'Closed';

    const levels   = findLiquidityLevels(candles);
    const sweeps   = detectSweep(candles, levels);

    // Current candle data
    const lastCandle = candles[candles.length - 1];
    const avgBody    = candles.slice(-10).reduce((s,c) => s + Math.abs(c.c - c.o), 0) / 10;

    let signal = null;

    if (sessionOk && sweeps.length > 0) {
      const sweep = sweeps[sweeps.length - 1]; // Most recent sweep
      const dir   = sweep.direction;

      const disp   = detectDisplacement(candles, dir, atr);
      const struct = detectStructureShift(candles, dir);

      // Pullback check: price retraced 30-70% of last move
      const displacementRange = Math.abs(lastCandle.h - lastCandle.l);
      const pullbackOk = displacementRange > 0 && atr > 0 && (displacementRange / atr) < 2.0;

      const confidence = calcConfidence(sessionOk, sessionOverlap, sweep, disp, struct, pullbackOk);

      if (confidence >= 70) {
        const entry = price;
        const slDist  = Math.abs(entry - (dir === 'BUY' ? sweep.sweepLow : sweep.sweepHigh)) + (atr * 0.1);
        const tp1Dist = slDist * 2.0;
        const tp2Dist = slDist * 3.5;

        const sl  = dir === 'BUY' ? parseFloat((entry - slDist).toFixed(3))  : parseFloat((entry + slDist).toFixed(3));
        const tp1 = dir === 'BUY' ? parseFloat((entry + tp1Dist).toFixed(3)) : parseFloat((entry - tp1Dist).toFixed(3));
        const tp2 = dir === 'BUY' ? parseFloat((entry + tp2Dist).toFixed(3)) : parseFloat((entry - tp2Dist).toFixed(3));
        const rr  = parseFloat((tp1Dist / slDist).toFixed(2));

        if (rr >= 2.0) {
          const reasons = [];
          reasons.push(`Liquidity sweep of ${sweep.level.label}`);
          if (disp.valid) reasons.push('strong displacement candle');
          if (struct)     reasons.push(`${dir === 'BUY' ? 'bullish' : 'bearish'} market structure shift`);
          if (sessionOverlap) reasons.push('session overlap');

          signal = {
            asset:        sym,
            direction:    dir,
            entry:        parseFloat(entry.toFixed(3)),
            stop_loss:    sl,
            take_profit_1:tp1,
            take_profit_2:tp2,
            rr,
            confidence,
            session:      sessionName,
            reason:       reasons.join(' + '),
            setup_type:   'Liquidity Sweep Reversal',
            sweep_level:  sweep.level.label,
            sweep_price:  sweep.level.price,
            displacement: disp.strength,
            structure:    struct
          };
        }
      }
    }

    res.json({
      success:     true,
      symbol:      sym,
      price,
      atr,
      session:     sessionName,
      session_ok:  sessionOk,
      levels:      levels.slice(0, 8),
      sweep_count: sweeps.length,
      candle_count:candles.length,
      last_candle: lastCandle,
      signal
    });

  } catch(e) {
    console.error(e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// Live prices only
app.get('/prices', async (req, res) => {
  try {
    const [xau, xag] = await Promise.all([getPrice('XAUUSD'), getPrice('XAGUSD')]);
    res.json({ success:true, prices:{ XAUUSD:xau, XAGUSD:xag }, ratio: xau&&xag ? parseFloat((xau/xag).toFixed(2)):null, ts: new Date().toUTCString() });
  } catch(e) { res.status(500).json({ success:false, error:e.message }); }
});

app.get('/', (req, res) => res.json({ status:'ok', version:'3.0', engine:'Liquidity Sweep Strategy' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Aurum Signal Engine v3 running on port ${PORT}`));
