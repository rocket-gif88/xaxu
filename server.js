const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const TWELVE_KEY = '7f3fc6ca85664930ab6e687db8ff0c5d';
const ANTHROPIC_KEY = ['sk-ant-','api03-PSBtiCb9gNCUnpxHjEl2sqWVtfNop5DtO1WCW2pdUw_upi3Zl0VDjCT7Yyk','W9bboA3Bxnq2ucHBFyuNrNx6CL','w-qYuk4wAA'].join('');

const SYMBOLS = {
  XAUUSD: { td: 'XAU/USD' },
  XAGUSD: { td: 'XAG/USD' },
  NASDAQ: { td: 'QQQ' }
};

async function safeFetch(url) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(id);
    return await res.json();
  } catch(e) { clearTimeout(id); throw e; }
}

async function getPrice(symbol) {
  try {
    const td = SYMBOLS[symbol].td;
    const data = await safeFetch(`https://api.twelvedata.com/price?symbol=${encodeURIComponent(td)}&apikey=${TWELVE_KEY}`);
    const p = parseFloat(data.price);
    return isNaN(p) ? null : p;
  } catch(e) { return null; }
}

async function getEMA(symbol, period) {
  try {
    const td = SYMBOLS[symbol].td;
    const data = await safeFetch(`https://api.twelvedata.com/ema?symbol=${encodeURIComponent(td)}&interval=1h&time_period=${period}&outputsize=3&apikey=${TWELVE_KEY}`);
    if (data.values && data.values.length >= 2) {
      return { current: parseFloat(data.values[0].ema), previous: parseFloat(data.values[1].ema) };
    }
    return null;
  } catch(e) { return null; }
}

async function getATR(symbol) {
  try {
    const td = SYMBOLS[symbol].td;
    const data = await safeFetch(`https://api.twelvedata.com/atr?symbol=${encodeURIComponent(td)}&interval=1h&time_period=14&outputsize=1&apikey=${TWELVE_KEY}`);
    if (data.values && data.values.length > 0) {
      const a = parseFloat(data.values[0].atr);
      return isNaN(a) ? null : a;
    }
    return null;
  } catch(e) { return null; }
}

function detectCross(ema9, ema20) {
  if (!ema9 || !ema20) return 'NEUTRAL';
  if (ema9.previous <= ema20.previous && ema9.current > ema20.current) return 'BULLISH_CROSS';
  if (ema9.previous >= ema20.previous && ema9.current < ema20.current) return 'BEARISH_CROSS';
  if (ema9.current > ema20.current) return 'ABOVE_EMA';
  if (ema9.current < ema20.current) return 'BELOW_EMA';
  return 'NEUTRAL';
}

function calcLevels(price, direction, atr, budget) {
  if (!price || !atr || atr <= 0) return null;
  const sl = atr * 1.5;
  const tp = atr * 2.5;
  const entry = parseFloat(price.toFixed(2));
  const stop_loss = direction === 'LONG' ? parseFloat((price - sl).toFixed(2)) : parseFloat((price + sl).toFixed(2));
  const take_profit = direction === 'LONG' ? parseFloat((price + tp).toFixed(2)) : parseFloat((price - tp).toFixed(2));
  const riskAmt = budget * 0.10;
  const riskPerUnit = Math.abs(entry - stop_loss);
  const units = riskPerUnit > 0 ? parseFloat((riskAmt / riskPerUnit).toFixed(4)) : 0.01;
  const rr = parseFloat((tp / sl).toFixed(2));
  return { entry, stop_loss, take_profit, units, rr };
}

async function generateSignal(symbol, marketData) {
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 300,
        system: 'You are a trading signal engine. Analyze real market data. Respond ONLY with valid JSON no markdown: {"direction":"LONG" or "SHORT" or "WAIT","confidence":"HIGH" or "MEDIUM" or "LOW","reason":"<max 10 words>","trend":"BULLISH" or "BEARISH" or "NEUTRAL"}. LONG=EMA9>EMA20+bullish. SHORT=EMA9<EMA20+bearish. WAIT=unclear.',
        messages: [{ role: 'user', content: `Signal for ${symbol}. Data: ${JSON.stringify(marketData)}. UTC: ${new Date().toUTCString()}` }]
      })
    });
    const data = await res.json();
    const text = data.content?.find(b => b.type === 'text')?.text || '{}';
    const parsed = JSON.parse(text.replace(/```json|```/g,'').trim());
    if (!['LONG','SHORT','WAIT'].includes(parsed.direction)) parsed.direction = 'WAIT';
    if (!['HIGH','MEDIUM','LOW'].includes(parsed.confidence)) parsed.confidence = 'LOW';
    if (!['BULLISH','BEARISH','NEUTRAL'].includes(parsed.trend)) parsed.trend = 'NEUTRAL';
    return parsed;
  } catch(e) {
    return { direction: 'WAIT', confidence: 'LOW', reason: 'Analysis unavailable', trend: 'NEUTRAL' };
  }
}

app.get('/', (req, res) => res.json({ status: 'ok', version: '2.0', assets: ['XAUUSD','XAGUSD','NASDAQ'] }));

app.get('/signal/:symbol', async (req, res) => {
  const symbol = req.params.symbol.toUpperCase();
  if (!SYMBOLS[symbol]) return res.status(400).json({ success: false, error: 'Unknown symbol: ' + symbol });
  const budget = parseFloat(req.query.budget) || 100;
  try {
    const [price, ema9, ema20, atr] = await Promise.all([getPrice(symbol), getEMA(symbol,9), getEMA(symbol,20), getATR(symbol)]);
    if (!price) return res.status(503).json({ success: false, error: 'Price unavailable. Market may be closed.' });
    const emaCross = detectCross(ema9, ema20);
    const pctFromEma = (price && ema20) ? parseFloat(((price - ema20.current) / ema20.current * 100).toFixed(3)) : null;
    let ratio = null;
    if (symbol === 'XAUUSD' || symbol === 'XAGUSD') {
      const [xau, xag] = await Promise.all([getPrice('XAUUSD'), getPrice('XAGUSD')]);
      if (xau && xag) ratio = parseFloat((xau/xag).toFixed(2));
    }
    const marketData = { price, ema9: ema9?.current||null, ema20: ema20?.current||null, emaCross, pctFromEma, atr, ratio };
    const signal = await generateSignal(symbol, marketData);
    const levels = signal.direction !== 'WAIT' ? calcLevels(price, signal.direction, atr, budget) : null;
    res.json({ success: true, symbol, timestamp: new Date().toUTCString(), price, ema9: ema9?.current||null, ema20: ema20?.current||null, emaCross, pctFromEma, atr, ratio, signal, levels, budget });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/prices', async (req, res) => {
  try {
    const [xau, xag, ndx] = await Promise.all([getPrice('XAUUSD'), getPrice('XAGUSD'), getPrice('NASDAQ')]);
    res.json({ success: true, timestamp: new Date().toUTCString(), prices: { XAUUSD: xau, XAGUSD: xag, NASDAQ: ndx }, ratio: xau&&xag ? parseFloat((xau/xag).toFixed(2)) : null });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Trading Signal Server v2 running on port ' + PORT));
