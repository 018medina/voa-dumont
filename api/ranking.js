// Voa, Dumont! — ranking compartilhado (Vercel Serverless Function + Upstash Redis)
//
// GET  /api/ranking
//      -> { top: [...geral], week: [...semana], weekId, weekEnds, champion }  (top 5 de cada)
//         A semana zera toda segunda às 00h (Brasília). O campeão da semana anterior
//         fica guardado no Hall da Fama (hash voa-dumont:hall).
//
// POST /api/ranking  { "action": "start" }
//      -> { run: "<bilhete>" }   Chamado na decolagem. O servidor anota a hora de início.
//
// POST /api/ranking  { "run": "<bilhete>", "score": 87, "name": "Ana", "key": "<chave do piloto>" }
//      Fecha a partida (confere se a pontuação é possível no tempo que passou) e,
//      se vier nome, registra no ranking.
//      -> { top, week, ..., improved: true|false, improvedWeek: true|false }
//
// POST /api/ranking  { "run": "<bilhete>", "score": 87, "key": "..." }      (sem nome)
//      -> { finished: true }   A partida fica guardada por 30 min para a pessoa colocar o nome depois.
//
// Proteções:
//   1. Bilhete por partida: cada pontuação precisa de um bilhete emitido pelo servidor na decolagem,
//      e o tempo entre a decolagem e o fim precisa ser compatível com a pontuação. Cada bilhete vale uma vez.
//   2. Nome com dono: o primeiro aparelho que registra um nome guarda uma chave secreta;
//      depois disso só quem tem essa chave pontua com aquele nome.
//   3. Filtro de nomes ofensivos.
//
// Variáveis de ambiente (criadas ao conectar o Upstash Redis no painel da Vercel):
//   KV_REST_API_URL / KV_REST_API_TOKEN   ou   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN

const crypto = require('crypto');

const TOP_N = 5;
const MAX_NAME = 16;
const MAX_SCORE = 9999;
const MIN_GAP_S = 3;          // intervalo mínimo entre envios com o mesmo nome
const RUN_TTL_S = 30 * 60;    // bilhete vale 30 minutos
const STARTS_PER_MIN = 40;    // decolagens por minuto por IP

// Tempo mínimo para fazer N pontos. No jogo, o 1º obstáculo está a ~395 px e os seguintes a ~240 px,
// com velocidade máxima de 230 px/s. Usamos 80% desse mínimo como margem para atrasos de rede.
const FIRST_PX = 395, GAP_PX = 240, MAX_SPEED = 230, SLACK = 0.8;
const minSeconds = (n) => (SLACK * (FIRST_PX + GAP_PX * (n - 1))) / MAX_SPEED;

const P = 'voa-dumont:';
const ZKEY = P + 'scores'; // sorted set: nome em minúsculas -> recorde
const HKEY = P + 'names';  // hash: nome em minúsculas -> nome como foi digitado
const OKEY = P + 'owners'; // hash: nome em minúsculas -> hash da chave do dono

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

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');

// ---------- Semanas (zera toda segunda 00h, horário de Brasília, UTC-3) ----------
const BRT = 3 * 3600 * 1000, DAY = 86400 * 1000;
function weekOf(ms) {
  const d = new Date(ms - BRT);                          // relógio de Brasília
  const back = (d.getUTCDay() + 6) % 7;                  // dias desde segunda
  const mon = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - back);
  const start = mon + BRT;                               // segunda 00h BRT em UTC
  return { id: new Date(mon).toISOString().slice(0, 10), start, end: start + 7 * DAY };
}
const WKEY = (id) => P + 'week:' + id;  // sorted set do placar da semana
const HALL = P + 'hall';                // hash: semana -> {"name","score"} do campeão

async function topOf(zkey) {
  const [flat] = await redis([['ZREVRANGE', zkey, 0, TOP_N - 1, 'WITHSCORES']]);
  if (!flat || !flat.length) return [];
  const keys = [], scores = [];
  for (let i = 0; i < flat.length; i += 2) { keys.push(flat[i]); scores.push(Number(flat[i + 1])); }
  const [names] = await redis([['HMGET', HKEY, ...keys]]);
  return keys.map((k, i) => ({ name: (names && names[i]) || k, score: scores[i] }));
}

// Campeão da semana passada: grava no Hall da Fama na primeira consulta da semana nova
async function champion(now) {
  const prev = weekOf(weekOf(now).start - 1);
  let [raw] = await redis([['HGET', HALL, prev.id]]);
  if (!raw) {
    const [first] = await topOf(WKEY(prev.id));
    if (!first) return null;
    raw = JSON.stringify(first);
    await redis([['HSETNX', HALL, prev.id, raw]]);
  }
  return { ...JSON.parse(raw), week: prev.id };
}

async function boards() {
  const now = Date.now(), w = weekOf(now);
  const [top, week, champ] = await Promise.all([topOf(ZKEY), topOf(WKEY(w.id)), champion(now)]);
  return { top, week, weekId: w.id, weekEnds: w.end, champion: champ };
}

function cleanName(raw) {
  return String(raw || '')
    .replace(/[^\p{L}\p{N} ._-]/gu, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, MAX_NAME);
}

// ---------- Filtro de nomes ofensivos ----------
// Normaliza acentos, troca números/símbolos comuns por letras (p0rr4 -> porra)
// e junta letras repetidas (poooorra -> pora), aplicando o mesmo nas listas.
const LEET = { 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', 7: 't', 8: 'b', '@': 'a', $: 's', '!': 'i' };
function norm(s) {
  return String(s)
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[0134578@$!]/g, (c) => LEET[c] || c);
}
const squeeze = (s) => s.replace(/(.)\1+/g, '$1');

// Bloqueadas em qualquer parte do nome (palavras longas e sem falsos positivos comuns)
const ANYWHERE = [
  'caralho', 'buceta', 'boceta', 'piroca', 'arrombad', 'punheta', 'xoxota', 'xereca',
  'filhodaputa', 'fdputa', 'fodase', 'foder', 'fodid', 'estupr', 'nazis', 'hitler', 'vagabund',
  'prostitut', 'retardad', 'viadinho', 'cuzao', 'cuzinho', 'putaria', 'puteiro', 'sapatao',
  'traveco', 'crioulo', 'fuck', 'shit', 'bitch', 'cunt', 'whore', 'pussy',
  'asshole', 'motherf', 'pedofil', 'pedophil',
].map(squeeze);
// Bloqueadas como palavra inteira (curtas demais para procurar dentro de outras)
const WHOLE = new Set([
  'cu', 'pica', 'puta', 'puto', 'putas', 'merda', 'foda', 'fdp', 'vsf', 'tnc', 'pqp', 'kct', 'krl',
  'viado', 'veado', 'bicha', 'corno', 'cacete', 'xota', 'broxa', 'babaca', 'otario', 'piranha',
  'macaco', 'macaca', 'mongol', 'rola', 'bosta', 'pau', 'dick', 'cock', 'fag', 'slut', 'rape',
  'nazi', 'porra', 'poha', 'rapist', 'nigger', 'nigga', 'niggas', 'faggot',
].map(squeeze));

function isOffensive(name) {
  const n = norm(name);
  const joined = squeeze(n.replace(/[^a-z]/g, ''));
  if (ANYWHERE.some((w) => joined.includes(w))) return true;
  const tokens = n.split(/[^a-z]+/).filter(Boolean).map(squeeze);
  return tokens.some((t) => WHOLE.has(t));
}

// ---------- Rotas ----------
function ipOf(req) {
  return String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || 'x';
}

async function startRun(req, res) {
  const ipKey = P + 'starts:' + sha(ipOf(req)).slice(0, 24);
  const [count] = await redis([['INCR', ipKey], ['EXPIRE', ipKey, 60]]);
  if (count > STARTS_PER_MIN) {
    return res.status(429).json({ error: 'Muitas decolagens seguidas. Aguarde um minuto.' });
  }
  const run = crypto.randomBytes(16).toString('hex');
  await redis([['SET', P + 'run:' + run, JSON.stringify({ s: Date.now() }), 'EX', RUN_TTL_S]]);
  return res.status(200).json({ run });
}

async function submit(body, res) {
  const run = String(body.run || '');
  if (!/^[a-f0-9]{32}$/.test(run)) {
    return res.status(400).json({ error: 'Partida sem bilhete válido.', code: 'run' });
  }
  const runKey = P + 'run:' + run;
  const [raw] = await redis([['GET', runKey]]);
  if (!raw) {
    return res.status(400).json({ error: 'Esta partida expirou ou já foi registrada.', code: 'run' });
  }
  const t = JSON.parse(raw);

  // 1) Fecha a partida, se ainda não foi fechada: confere se a pontuação cabe no tempo
  if (t.sc === undefined) {
    const score = body.score;
    if (!Number.isInteger(score) || score < 1 || score > MAX_SCORE) {
      return res.status(400).json({ error: 'Pontuação inválida.', code: 'score' });
    }
    const elapsed = (Date.now() - t.s) / 1000;
    if (elapsed < minSeconds(score)) {
      await redis([['DEL', runKey]]);
      return res.status(400).json({ error: 'Pontuação incompatível com o tempo de voo.', code: 'score' });
    }
    t.sc = score;
    t.e = Date.now();
    await redis([['SET', runKey, JSON.stringify(t), 'EX', RUN_TTL_S]]);
  }

  // 2) Sem nome: a partida fica guardada para ser registrada depois
  const name = cleanName(body.name);
  if (!name) return res.status(200).json({ finished: true });

  if (name.length < 2) {
    return res.status(400).json({ error: 'O nome precisa ter pelo menos 2 caracteres.', code: 'name' });
  }
  if (isOffensive(name)) {
    return res.status(400).json({ error: 'Esse nome não é permitido. Escolha outro.', code: 'name' });
  }

  // 3) Nome com dono
  const keyRaw = String(body.key || '');
  if (!/^[a-f0-9]{32,64}$/.test(keyRaw)) {
    return res.status(400).json({ error: 'Chave do piloto ausente. Recarregue a página.', code: 'key' });
  }
  const lower = name.toLowerCase();
  const keyHash = sha(keyRaw);
  const [claimed] = await redis([['HSETNX', OKEY, lower, keyHash]]);
  if (!claimed) {
    const [owner] = await redis([['HGET', OKEY, lower]]);
    if (owner !== keyHash) {
      return res.status(403).json({ error: 'Esse nome já pertence a outro piloto. Escolha outro.', code: 'owner' });
    }
  }

  // 4) Limite de frequência por nome
  const rlKey = P + 'rl:' + sha(lower).slice(0, 32);
  const [rl] = await redis([['SET', rlKey, '1', 'EX', MIN_GAP_S, 'NX']]);
  if (rl !== 'OK') {
    return res.status(429).json({ error: 'Calma, comandante! Aguarde uns segundos e tente de novo.' });
  }

  // 5) Consome o bilhete e registra (só sobe, nunca desce)
  await redis([['DEL', runKey]]);
  // A partida conta para a semana em que ela terminou
  const wk = WKEY(weekOf(t.e || Date.now()).id);
  const [prev, prevWeek] = await redis([['ZSCORE', ZKEY, lower], ['ZSCORE', wk, lower]]);
  const improved = prev === null || t.sc > Number(prev);
  const improvedWeek = prevWeek === null || t.sc > Number(prevWeek);
  const cmds = [['HSET', HKEY, lower, name]];
  if (improved) cmds.push(['ZADD', ZKEY, t.sc, lower]);
  if (improvedWeek) cmds.push(['ZADD', wk, t.sc, lower], ['EXPIRE', wk, 120 * 86400]);
  await redis(cmds);
  return res.status(200).json({ ...(await boards()), improved, improvedWeek, score: t.sc });
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  if (!URL_ || !TOKEN) {
    return res.status(500).json({ error: 'Banco do ranking não configurado na Vercel.' });
  }

  try {
    if (req.method === 'GET') {
      return res.status(200).json(await boards());
    }
    if (req.method !== 'POST') {
      res.setHeader('Allow', 'GET, POST');
      return res.status(405).json({ error: 'Método não permitido.' });
    }

    let body = req.body;
    if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
    body = body || {};

    if (body.action === 'start') return await startRun(req, res);
    return await submit(body, res);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'Não foi possível acessar o ranking agora.' });
  }
};

module.exports.isOffensive = isOffensive;
module.exports.minSeconds = minSeconds;
module.exports.weekOf = weekOf;
