const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

const TWELVE_KEY = '7f3fc6ca85664930ab6e687db8ff0c5d';
const ANTHROPIC_KEY = ['sk-ant-', 'api03-PSBtiCb9gNCUnpxHjEl2sqWVtfNop5DtO1WCW2pdUw_upi3Zl0VDjCT7Yyk',
                       'W9bboA3Bxnq2ucHBFyuNrNx6CL', 'w-qYuk4wAA'].join('');

// ── Symbol mapping for Twelve Data
const SYMBOLS = {
  XAUUSD: { td: 'XAU/USD', type: 'forex' },
  XAGUSD: { td: 'XAG/USD', type: 'forex' },
  BTC:    { td: 'BTC/USD', type: 'crypto' },
  ETH:    { td: 'ETH/USD', type: 'crypto' },
  SOL:    { td: 'SOL/USD', type: 'crypto' }
};

// ── Fetch live price from Twelve Data
async function getPrice(symbol) {
  try {
    const td = SYMBOLS[symbol].td;
    const url = `https://api.twelvedata.com/price?symbol=${td}&apikey=${TWELVE_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    return parseFloat(data.price) || null;
  } catch(e) {
    console.error(`Price fetch error for ${symbol}:`, e.message);
    return null;
  }
}

// ── Fetch EMA from Twelve Data
async function getEMA(symbol, period) {
  try {
    const td = SYMBOLS[symbol].td;
    const url = `https://api.twelvedata.com/ema?symbol=${td}&interval=1h&time_period=${period}&outputsize=2&apikey=${TWELVE_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    if (data.values && data.values.length >= 2) {
      return {
        current: parseFloat(data.values[0].ema),
        previous: parseFloat(data.values[1].ema)
      };
    }
    return null;
  } catch(e) {
    console.error(`EMA fetch error for ${symbol}:`, e.message);
    return null;
  }
}

// ── Fetch news from Twelve Data
async function getNews(symbol) {
  try {
    const td = SYMBOLS[symbol]?.td || symbol;
    const url = `https://api.twelvedata.com/news?symbol=${td}&outputsize=3&apikey=${TWELVE_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    if (data.data && data.data.length > 0) {
      return data.data.slice(0, 3).map(n => n.title).join(' | ');
    }
    return 'No recent news available.';
  } catch(e) {
    return 'News unavailable.';
  }
}

// ── Generate AI signal via Anthropic
async function generateSignal(asset, marketData) {
  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 500,
        system: `You are an expert trading signal generator. Analyze market data and respond ONLY with valid JSON, no markdown:
{
  "action": "BUY" or "WAIT" or "SELL",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "entry": <number or null>,
  "stop_loss": <number or null>,
  "take_profit": <number or null>,
  "sentiment": "BULLISH" or "BEARISH" or "NEUTRAL",
  "reason": "<max 12 words>",
  "news_summary": "<one sentence summary of news sentiment>"
}`,
        messages: [{
          role: 'user',
          content: `Generate trading signal for ${asset}.
Market data: ${JSON.stringify(marketData)}
UTC time: ${new Date().toUTCString()}
${asset === 'XAUUSD' || asset === 'XAGUSD' 
  ? `XAU/XAG Ratio: ${marketData.ratio}. Strategy: ratio>80=favor XAU, ratio<70=favor XAG, 70-80=WAIT`
  : `Strategy: Use EMA crossover, momentum, volume, and news sentiment to decide. Be selective - only signal BUY when confident.`
}`
        }]
      })
    });
    const data = await res.json();
    const text = data.content?.find(b => b.type === 'text')?.text || '{}';
    return JSON.parse(text.replace(/```json|```/g, '').trim());
  } catch(e) {
    console.error('Signal generation error:', e.message);
    return { action: 'WAIT', reason: 'Analysis unavailable', confidence: 'LOW', sentiment: 'NEUTRAL' };
  }
}

// ══════════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════════

// ── Health check
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Trading signals server running' });
});

// ── XAU/XAG Signal
app.get('/signal/metals', async (req, res) => {
  try {
    const [xauPrice, xagPrice, xauEma9, xauEma20, xagEma9, xagEma20] = await Promise.all([
      getPrice('XAUUSD'),
      getPrice('XAGUSD'),
      getEMA('XAUUSD', 9),
      getEMA('XAUUSD', 20),
      getEMA('XAGUSD', 9),
      getEMA('XAGUSD', 20)
    ]);

    const ratio = xauPrice && xagPrice ? parseFloat((xauPrice / xagPrice).toFixed(2)) : null;

    // EMA crossover detection
    const xauCross = xauEma9 && xauEma20 ? (
      xauEma9.previous <= xauEma20.previous && xauEma9.current > xauEma20.current ? 'BULLISH_CROSS' :
      xauEma9.previous >= xauEma20.previous && xauEma9.current < xauEma20.current ? 'BEARISH_CROSS' : 'NEUTRAL'
    ) : 'NEUTRAL';

    const xagCross = xagEma9 && xagEma20 ? (
      xagEma9.previous <= xagEma20.previous && xagEma9.current > xagEma20.current ? 'BULLISH_CROSS' :
      xagEma9.previous >= xagEma20.previous && xagEma9.current < xagEma20.current ? 'BEARISH_CROSS' : 'NEUTRAL'
    ) : 'NEUTRAL';

    const marketData = { xauPrice, xagPrice, ratio, xauCross, xagCross };
    const signal = await generateSignal('XAUUSD', marketData);

    res.json({
      success: true,
      timestamp: new Date().toUTCString(),
      xauPrice, xagPrice, ratio,
      ema: { xauCross, xagCross },
      signal
    });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ── Crypto Signal (BTC, ETH, SOL)
app.get('/signal/crypto/:asset', async (req, res) => {
  const asset = req.params.asset.toUpperCase();
  if (!['BTC', 'ETH', 'SOL'].includes(asset)) {
    return res.status(400).json({ success: false, error: 'Invalid asset' });
  }

  try {
    const [price, ema9, ema20, news] = await Promise.all([
      getPrice(asset),
      getEMA(asset, 9),
      getEMA(asset, 20),
      getNews(asset)
    ]);

    const emaCross = ema9 && ema20 ? (
      ema9.previous <= ema20.previous && ema9.current > ema20.current ? 'BULLISH_CROSS' :
      ema9.previous >= ema20.previous && ema9.current < ema20.current ? 'BEARISH_CROSS' : 'NEUTRAL'
    ) : 'NEUTRAL';

    const pctFromEma = price && ema20 ? (((price - ema20.current) / ema20.current) * 100).toFixed(2) : null;

    const marketData = { price, ema9: ema9?.current, ema20: ema20?.current, emaCross, pctFromEma, news };
    const signal = await generateSignal(asset, marketData);

    res.json({
      success: true,
      timestamp: new Date().toUTCString(),
      asset, price, emaCross, pctFromEma,
      news, signal
    });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ── Portfolio prices (all assets at once)
app.get('/prices', async (req, res) => {
  try {
    const [xau, xag, btc, eth, sol] = await Promise.all([
      getPrice('XAUUSD'), getPrice('XAGUSD'),
      getPrice('BTC'), getPrice('ETH'), getPrice('SOL')
    ]);
    res.json({
      success: true,
      timestamp: new Date().toUTCString(),
      prices: { XAUUSD: xau, XAGUSD: xag, BTC: btc, ETH: eth, SOL: sol },
      ratio: xau && xag ? parseFloat((xau / xag).toFixed(2)) : null
    });
  } catch(e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Trading server running on port ${PORT}`));
