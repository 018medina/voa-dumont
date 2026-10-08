<?php
/**
 * Voa, Dumont! — ranking compartilhado
 *
 * GET  ranking.php                         -> {"top":[{"name":"...","score":12}, ...]}  (top 5)
 * POST ranking.php  {"name":"Ana","score":12} -> {"top":[...], "improved":true|false}
 *
 * Guarda o melhor recorde de cada nome num arquivo JSON (ranking-data/ranking.json).
 * Requer PHP 7.2+ e permissão de escrita na pasta ranking-data.
 */

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

const TOP_N      = 5;
const MAX_NAME   = 16;
const MAX_SCORE  = 999;   // teto de sanidade
const MIN_GAP_S  = 3;     // intervalo mínimo entre envios do mesmo IP (segundos)

$dir  = __DIR__ . '/ranking-data';
$file = $dir . '/ranking.json';

if (!is_dir($dir)) {
    @mkdir($dir, 0755, true);
    // Bloqueia acesso direto ao JSON em servidores Apache
    @file_put_contents($dir . '/.htaccess', "Require all denied\nDeny from all\n");
}

function fail($code, $msg) {
    http_response_code($code);
    echo json_encode(['error' => $msg], JSON_UNESCAPED_UNICODE);
    exit;
}

function top_list(array $data) {
    $rows = array_values($data['players'] ?? []);
    usort($rows, function ($a, $b) {
        return $b['score'] <=> $a['score'] ?: $a['at'] <=> $b['at']; // empate: quem fez primeiro
    });
    return array_map(function ($r) {
        return ['name' => $r['name'], 'score' => (int)$r['score']];
    }, array_slice($rows, 0, TOP_N));
}

$fh = @fopen($file, 'c+');
if (!$fh) fail(500, 'Não foi possível abrir o arquivo do ranking. Verifique a permissão de escrita da pasta.');

$method = $_SERVER['REQUEST_METHOD'] ?? 'GET';

if ($method === 'GET') {
    flock($fh, LOCK_SH);
    $raw  = stream_get_contents($fh);
    flock($fh, LOCK_UN);
    fclose($fh);
    $data = json_decode($raw ?: '{}', true) ?: [];
    echo json_encode(['top' => top_list($data)], JSON_UNESCAPED_UNICODE);
    exit;
}

if ($method !== 'POST') fail(405, 'Método não permitido.');

$in    = json_decode(file_get_contents('php://input'), true);
$name  = trim((string)($in['name'] ?? ''));
$score = $in['score'] ?? null;

// Nome: letras, números, espaço e alguns símbolos; colapsa espaços
$name = preg_replace('/[^\p{L}\p{N} ._\-]/u', '', $name);
$name = preg_replace('/\s+/u', ' ', $name);
$name = mb_substr($name, 0, MAX_NAME);

if (mb_strlen($name) < 2)                          fail(400, 'O nome precisa ter pelo menos 2 caracteres.');
if (!is_int($score) || $score < 1 || $score > MAX_SCORE) fail(400, 'Pontuação inválida.');

flock($fh, LOCK_EX);
$raw  = stream_get_contents($fh);
$data = json_decode($raw ?: '{}', true) ?: [];
$data['players'] = $data['players'] ?? [];
$data['ips']     = $data['ips'] ?? [];

// Limite simples de frequência por IP
$ip  = $_SERVER['REMOTE_ADDR'] ?? 'x';
$ipk = hash('sha256', $ip);
$now = time();
if (isset($data['ips'][$ipk]) && $now - $data['ips'][$ipk] < MIN_GAP_S) {
    flock($fh, LOCK_UN); fclose($fh);
    fail(429, 'Calma, comandante! Aguarde uns segundos e tente de novo.');
}
$data['ips'][$ipk] = $now;
// limpa IPs antigos
$data['ips'] = array_filter($data['ips'], function ($t) use ($now) { return $now - $t < 3600; });

// Melhor pontuação por nome (sem diferenciar maiúsculas)
$key      = mb_strtolower($name);
$improved = false;
if (!isset($data['players'][$key]) || $score > $data['players'][$key]['score']) {
    $data['players'][$key] = ['name' => $name, 'score' => $score, 'at' => $now];
    $improved = true;
}

ftruncate($fh, 0);
rewind($fh);
fwrite($fh, json_encode($data, JSON_UNESCAPED_UNICODE));
fflush($fh);
flock($fh, LOCK_UN);
fclose($fh);

echo json_encode(['top' => top_list($data), 'improved' => $improved], JSON_UNESCAPED_UNICODE);
