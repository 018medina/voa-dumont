// Voa, Dumont! — ranking compartilhado (Vercel Serverless Function + Upstash Redis)
//
// GET  /api/ranking                          -> { top: [{ name, score }, ...] }  (top 5)
// POST /api/ranking  { "name": "Ana", "score": 12 } -> { top: [...], improved: true|false }
//
// Variáveis de ambiente (criadas automaticamente ao conectar o Upstash Redis no painel da Vercel):
//   KV_REST_API_URL / KV_REST_API_TOKEN   ou   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
// Sem dependências: fala com a API REST do Upstash via fetch.

const crypto = require('crypto');

const TOP_N = 5;
const MAX_NAME = 16;
const MAX_SCORE = 999;
const MIN_GAP_S = 3; // intervalo mínimo entre envios do mesmo IP

const ZKEY = 'voa-dumont:scores'; // sorted set: membro = nome em minúsculas, score = recorde
const HKEY = 'voa-dumont:names';  // hash: nome em minúsculas -> nome como foi digitado

const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

async function redis(commands) {
  const r = await fetch(`${URL_}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
  });
  if (!r.ok) throw new Error(`Redis HTTP ${r.status}`);
  const out = await r.json();
  return out.map((x) => {
    if (x.error) throw new Error(x.error);
    return x.result;
  });
}

async function topList() {
  const [flat] = await redis([['ZREVRANGE', ZKEY, 0, TOP_N - 1, 'WITHSCORES']]);
  if (!flat || !flat.length) return [];
  const keys = [], scores = [];
  for (let i = 0; i < flat.length; i += 2) { keys.push(flat[i]); scores.push(Number(flat[i + 1])); }
  const [names] = await redis([['HMGET', HKEY, ...keys]]);
  return keys.map((k, i) => ({ name: (names && names[i]) || k, score: scores[i] }));
}

function cleanName(raw) {
  return String(raw || '')
    .replace(/[^\p{L}\p{N} ._-]/gu, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, MAX_NAME);
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  if (!URL_ || !TOKEN) {
    return res.status(500).json({ error: 'Banco do ranking não configurado na Vercel.' });
  }

  try {
    if (req.method === 'GET') {
      return res.status(200).json({ top: await topList() });
    }

    if (req.method !== 'POST') {
      res.setHeader('Allow', 'GET, POST');
      return res.status(405).json({ error: 'Método não permitido.' });
    }

    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
    const name = cleanName(body && body.name);
    const score = body && body.score;

    if (name.length < 2) return res.status(400).json({ error: 'O nome precisa ter pelo menos 2 caracteres.' });
    if (!Number.isInteger(score) || score < 1 || score > MAX_SCORE) {
      return res.status(400).json({ error: 'Pontuação inválida.' });
    }

    // Limite de frequência por IP (guardado só como hash)
    const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || 'x';
    const ipKey = 'voa-dumont:rl:' + crypto.createHash('sha256').update(ip).digest('hex').slice(0, 32);
    const key = name.toLowerCase();

    const [rl] = await redis([['SET', ipKey, '1', 'EX', MIN_GAP_S, 'NX']]);
    if (rl !== 'OK') {
      return res.status(429).json({ error: 'Calma, comandante! Aguarde uns segundos e tente de novo.' });
    }

    const [prev] = await redis([['ZSCORE', ZKEY, key]]);
    const improved = prev === null || score > Number(prev);
    if (improved) {
      await redis([
        ['ZADD', ZKEY, score, key],
        ['HSET', HKEY, key, name],
      ]);
    }

    return res.status(200).json({ top: await topList(), improved });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'Não foi possível acessar o ranking agora.' });
  }
};
