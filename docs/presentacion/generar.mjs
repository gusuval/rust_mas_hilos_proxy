// Genera docs/presentacion/proxy-l7.pptx
//   npm i pptxgenjs@3 && node docs/presentacion/generar.mjs
//   (o PPTX_MODULES=<dir con node_modules> node docs/presentacion/generar.mjs)
// Coste: tokens exactos sumados del transcript de la sesión de Claude Code
// (~/.claude/projects/<proyecto>/<sesión>.jsonl, campo usage de cada respuesta,
// deduplicado por id de mensaje) y valorados a precios de la API de Opus 5.5.
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(process.env.PPTX_MODULES ? path.join(process.env.PPTX_MODULES, 'x.js') : import.meta.url);
const PptxGenJS = require('pptxgenjs');
const here = path.dirname(fileURLToPath(import.meta.url));

// ---- datos -----------------------------------------------------------
const COSTE = {
  // Sesión 564e0127, 1-oct 20:18-22:09 (UTC-3), 165 respuestas de claude-opus-5-5:
  // repo en GitHub, versión C con pthread y port a Rust (hasta antes de esta documentación)
  tokens: { entrada: 330, salida: 256742, cache_escritura_1h: 532835, cache_lectura: 46808600 },
  // USD por millón de tokens (Claude Opus 5.5, API first-party)
  precio: { entrada: 4, salida: 20, cache_escritura_1h: 8, cache_lectura: 0.2 },
  duracion: '1 h 51 min',
};
const usd = (k) => (COSTE.tokens[k] * COSTE.precio[k]) / 1e6;
const COSTE_TOTAL = Object.keys(COSTE.tokens).reduce((a, k) => a + usd(k), 0);
const money = (v) => '$' + v.toFixed(2).replace('.', ',');
const mill = (n) => (n >= 1e6 ? (n / 1e6).toFixed(1).replace('.', ',') + ' M' : Math.round(n / 1e3) + ' k');

const ESC = ['HTTPS c100', 'HTTPS c200', 'HTTPS c400', 'HTTP c200'];
// Rust, stack completo (proxy y backend en Rust), ejecución 2 (bench-20261001-213831-rust.md)
const BENCH = [
  { esc: 'HTTPS c100', rps: 202156, p99: '1,08 ms' },
  { esc: 'HTTPS c200', rps: 189936, p99: '2,10 ms' },
  { esc: 'HTTPS c400', rps: 163825, p99: '5,46 ms' },
  { esc: 'HTTP c200', rps: 341340, p99: '1,76 ms' },
];
// Medias por versión (bench/results/comparativa-*.md)
const FORK = [223372, 210342, 173506, 307471]; // 2 ejecuciones (c400: solo la #2)
const PTHREAD2 = [226981, 213020, 171153, 337706]; // las 2 alternas con fork
const C_PTHREAD = [228532, 220261, 177788, 348992]; // misma sesión que las de Rust, backend C
const RUST_CBACK = [203985, 197585, 162959, 365279]; // aws-lc, backend C
const RUST_FULL = [201662, 191660, 163457, 338261]; // media de 2, backend Rust
const EVOL = { fork: FORK, pthread: [227498, 215434, 173364, 341468], rust: RUST_FULL };

// ---- estilo ----------------------------------------------------------
const C = { ink: '1D2330', muted: '5B6475', rule: 'D9DEE7', accent: '0B6E69', accent2: '3FB8AF', grey: '9AA3B2', soft: 'E6F2F1', warn: '9A5B00', warnSoft: 'FFF4E0', bg: 'FFFFFF', dark: '0F2A2E' };
const FONT = 'Calibri';

const pptx = new PptxGenJS();
pptx.layout = 'LAYOUT_WIDE'; // 13.33 x 7.5 in
pptx.title = 'Proxy inverso L7 en Rust — hilos, epoll/kqueue';
pptx.author = 'Gustavo Uval';

pptx.defineSlideMaster({
  title: 'BASE',
  background: { color: C.bg },
  objects: [
    { rect: { x: 0, y: 0, w: 0.12, h: 7.5, fill: { color: C.accent } } },
    { text: { text: 'Proxy L7 · Rust · hilos · epoll/kqueue', options: { x: 0.6, y: 7.0, w: 6, h: 0.3, fontFace: FONT, fontSize: 10, color: C.muted } } },
  ],
  slideNumber: { x: 12.3, y: 7.0, w: 0.6, h: 0.3, fontFace: FONT, fontSize: 10, color: C.muted, align: 'right' },
});

function titled(title, kicker) {
  const s = pptx.addSlide({ masterName: 'BASE' });
  if (kicker) s.addText(kicker.toUpperCase(), { x: 0.6, y: 0.35, w: 12, h: 0.35, fontFace: FONT, fontSize: 12, bold: true, color: C.accent, charSpacing: 2 });
  s.addText(title, { x: 0.6, y: 0.65, w: 12.1, h: 0.8, fontFace: FONT, fontSize: 30, bold: true, color: C.ink });
  return s;
}

function bullets(s, items, box, size = 17) {
  const runs = [];
  items.forEach((it, i) => {
    const [head, body] = Array.isArray(it) ? it : [it, null];
    runs.push({ text: head, options: { bold: !!body, bullet: { code: '25A0' }, color: C.ink, breakLine: !body } });
    if (body) runs.push({ text: ' — ' + body, options: { color: C.muted, breakLine: true } });
    if (i < items.length - 1) runs.push({ text: '', options: { fontSize: 6, breakLine: true } });
  });
  s.addText(runs, { fontFace: FONT, fontSize: size, valign: 'top', paraSpaceAfter: 2, ...box });
}

function card(s, x, y, w, h, title, lines, opts = {}) {
  s.addShape(pptx.ShapeType.roundRect, { x, y, w, h, rectRadius: 0.08, fill: { color: opts.fill || C.soft }, line: { color: opts.line || C.accent, width: 1 } });
  s.addText(title, { x: x + 0.2, y: y + 0.12, w: w - 0.4, h: 0.45, fontFace: FONT, fontSize: 16, bold: true, color: opts.line || C.accent });
  s.addText(lines.map((t) => ({ text: t, options: { bullet: { code: '2022' }, breakLine: true } })),
    { x: x + 0.2, y: y + 0.58, w: w - 0.4, h: h - 0.7, fontFace: FONT, fontSize: opts.size || 13.5, color: C.ink, valign: 'top', paraSpaceAfter: 3 });
}

function kpi(s, x, y, w, value, label, color = C.accent) {
  s.addShape(pptx.ShapeType.rect, { x, y, w, h: 1.35, fill: { color: 'FFFFFF' }, line: { color: C.rule, width: 1 } });
  s.addShape(pptx.ShapeType.rect, { x, y, w, h: 0.08, fill: { color }, line: { color, width: 0 } });
  s.addText(value, { x: x + 0.15, y: y + 0.18, w: w - 0.3, h: 0.65, fontFace: FONT, fontSize: 28, bold: true, color: C.ink });
  s.addText(label, { x: x + 0.15, y: y + 0.8, w: w - 0.3, h: 0.52, fontFace: FONT, fontSize: 12, color: C.muted, valign: 'top' });
}

const fmt = (n) => n.toLocaleString('es-ES');
const pct = (a, b) => { const v = Math.round((100 * (a - b)) / b); return (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v) + ' %'; };
const hdr = (t, right) => ({ text: t, options: { bold: true, fill: { color: C.soft }, align: right ? 'right' : 'left' } });
const num = (n, bold) => ({ text: fmt(n), options: { align: 'right', bold: !!bold } });
const TABLE = { fontFace: FONT, fontSize: 13, color: C.ink, border: { type: 'solid', color: C.rule, pt: 0.75 }, rowH: 0.4 };

function groupedChart(s, series, colors, box, max = 400000) {
  s.addChart(pptx.ChartType.bar, series.map(([name, values]) => ({ name, labels: ESC, values })), {
    ...box, barDir: 'bar', barGrouping: 'clustered', barGapWidthPct: 60, chartColors: colors,
    catAxisLabelFontFace: FONT, catAxisLabelFontSize: 13, catAxisOrientation: 'maxMin',
    valAxisLabelFontFace: FONT, valAxisLabelFontSize: 10, valAxisLabelFormatCode: '#,##0', valAxisMaxVal: max, valAxisMinVal: 0, valAxisMajorUnit: 100000,
    valGridLine: { color: 'E9ECF1', size: 0.75 },
    showValue: true, dataLabelFontFace: FONT, dataLabelFontSize: 10, dataLabelFormatCode: '#,##0', dataLabelPosition: 'outEnd',
    showLegend: true, legendPos: 'b', legendFontFace: FONT, legendFontSize: 12,
  });
}

// ---- 1. portada ------------------------------------------------------
{
  const s = pptx.addSlide();
  s.background = { color: C.dark };
  s.addShape(pptx.ShapeType.rect, { x: 0.8, y: 2.2, w: 0.12, h: 2.3, fill: { color: C.accent2 }, line: { color: C.accent2, width: 0 } });
  s.addText('PROYECTO DE SISTEMAS · VERSIÓN 2.0', { x: 1.15, y: 2.15, w: 11, h: 0.4, fontFace: FONT, fontSize: 14, bold: true, color: C.accent2, charSpacing: 3 });
  s.addText('Proxy inverso L7 de alto rendimiento', { x: 1.15, y: 2.6, w: 11.5, h: 0.9, fontFace: FONT, fontSize: 40, bold: true, color: 'FFFFFF' });
  s.addText('Rust · un event loop por hilo (mio: epoll / kqueue) · TLS con rustls · recarga en caliente', { x: 1.15, y: 3.5, w: 11.5, h: 0.6, fontFace: FONT, fontSize: 20, color: 'C9D6D8' });
  s.addText('Del C con procesos al Rust con hilos · Decisiones · Benchmark y comparativas · Coste', { x: 1.15, y: 4.05, w: 11.5, h: 0.5, fontFace: FONT, fontSize: 16, color: '8FA7AB' });
  s.addText('Gustavo Uval · 1 de octubre de 2026 · github.com/gusuval/rust_mas_hilos_proxy', { x: 1.15, y: 6.5, w: 11.5, h: 0.4, fontFace: FONT, fontSize: 13, color: '8FA7AB' });
}

// ---- 2. evolución ----------------------------------------------------
{
  const s = titled('Tres versiones del mismo proxy', 'El camino');
  card(s, 0.6, 1.7, 3.95, 4.6, '1.0 · C, procesos', [
    'C11 + Meson, OpenSSL',
    'Master + N procesos (fork)',
    'Memoria compartida mmap + seqlock',
    'Un worker caído se relanza',
    '96 pruebas de integración',
  ], { fill: 'F4F6F9', line: C.muted, size: 16 });
  card(s, 4.7, 1.7, 3.95, 4.6, '1.1 · C, hilos', [
    'Un proceso: master + N pthreads',
    'Estado del worker _Thread_local',
    'Canal socketpair, parada con CHAN_STOP',
    'Seqlock → mutex (carrera detectada por TSan)',
    'Pierde el relanzamiento: un fallo tumba el proceso',
  ], { fill: 'F4F6F9', line: C.muted, size: 16 });
  card(s, 8.8, 1.7, 3.95, 4.6, '2.0 · Rust, hilos', [
    'mio + rustls + cargo, ~7.100 líneas',
    'Arc para lo compartido, Rc/Cell en el worker',
    'Un panic solo tumba su hilo: se relanza',
    'Certificados leídos una vez, en el master',
    'La misma batería de C pasa entera (+4 pruebas)',
  ], { size: 16 });
  s.addText('Las tres tienen el mismo comportamiento y superan la meta de 50.000 req/s por HTTPS más de 3 veces.',
    { x: 0.6, y: 6.4, w: 12.1, h: 0.45, fontFace: FONT, fontSize: 14, color: C.muted, italic: true });
}

// ---- 3. arquitectura -------------------------------------------------
{
  const s = titled('Un proceso: hilo master + N hilos worker', 'Arquitectura');
  const box = (x, y, w, h, t, sub, fill = C.soft, line = C.accent) => {
    s.addShape(pptx.ShapeType.roundRect, { x, y, w, h, rectRadius: 0.08, fill: { color: fill }, line: { color: line, width: 1.25 } });
    s.addText([{ text: t, options: { bold: true, breakLine: true, fontSize: 15 } }, { text: sub, options: { fontSize: 12, color: C.muted } }],
      { x, y, w, h, fontFace: FONT, color: C.ink, align: 'center', valign: 'middle' });
  };
  const arrow = (x1, y1, x2, y2, color = C.muted) => s.addShape(pptx.ShapeType.line, { x: Math.min(x1, x2), y: Math.min(y1, y2), w: Math.abs(x2 - x1) || 0.001, h: Math.abs(y2 - y1) || 0.001, flipV: y2 < y1, line: { color, width: 1.5, endArrowType: 'triangle' } });
  box(0.7, 3.05, 1.8, 1.1, 'Clientes', 'HTTP / HTTPS', 'FFFFFF', C.muted);
  box(3.3, 1.55, 5.2, 1.15, 'Hilo master', 'valida y compila la config (Arc) · inotify · señales\nrelanza workers (también tras un panic) · stats');
  box(3.3, 3.0, 2.45, 1.7, 'Worker 0', 'event loop mio\nRc<Runtime> propio\n+ hilo de health');
  box(6.05, 3.0, 2.45, 1.7, 'Worker N-1', 'event loop mio\nRc<Runtime> propio\n+ hilo de health');
  box(3.3, 5.1, 2.45, 0.85, 'Estadísticas', 'slot por worker (atómicos)', 'F4F6F9', C.muted);
  box(6.05, 5.1, 2.45, 0.85, 'Hilo de log', 'cola MPSC acotada', 'F4F6F9', C.muted);
  box(9.6, 2.3, 2.4, 0.75, 'backend A', '', 'FFFFFF', C.muted);
  box(9.6, 3.45, 2.4, 0.75, 'backend B', '', 'FFFFFF', C.muted);
  box(9.6, 4.6, 2.4, 0.75, 'backend C', '', 'FFFFFF', C.muted);
  arrow(2.5, 3.6, 3.3, 3.6);
  arrow(8.5, 3.5, 9.6, 2.7); arrow(8.5, 3.85, 9.6, 3.85); arrow(8.5, 4.2, 9.6, 4.95);
  arrow(5.9, 2.7, 4.5, 3.0, C.accent); arrow(5.9, 2.7, 7.3, 3.0, C.accent);
  s.addText('SO_REUSEPORT', { x: 2.3, y: 3.15, w: 1.2, h: 0.3, fontFace: FONT, fontSize: 10, color: C.muted, align: 'center' });
  s.addText('canal + Waker', { x: 3.3, y: 2.72, w: 1.3, h: 0.28, fontFace: FONT, fontSize: 10, color: C.accent });
  s.addText('pool keep-alive', { x: 8.45, y: 2.55, w: 1.3, h: 0.3, fontFace: FONT, fontSize: 10, color: C.muted });
  s.addText('Lo compartido (config compilada, salud de los servidores, stats) va en Arc; el estado de cada petición, en Rc/Cell: el compilador impide que salga de su hilo.',
    { x: 0.6, y: 6.15, w: 12.1, h: 0.6, fontFace: FONT, fontSize: 14, color: C.muted, italic: true });
}

// ---- 4-6. decisiones -------------------------------------------------
{
  const s = titled('Event loop: edge-triggered sin perder eventos', 'Decisiones técnicas · 1/3');
  card(s, 0.6, 1.7, 3.95, 4.95, 'mio, no tokio', [
    'Capa fina sobre epoll/kqueue: mismos eventos, mismo modo ET',
    'Un Poll por hilo worker, sin planificador encima',
    'Port fiel de la máquina de estados de C',
  ], { size: 17 });
  card(s, 4.7, 1.7, 3.95, 4.95, 'sess_drive()', [
    'leer → procesar → E/S backend → procesar → escribir',
    'Se repite mientras haya progreso: nunca queda trabajo sin evento',
    'Presupuesto de 32 vueltas y replanificación: nadie acapara el loop',
  ], { size: 17 });
  card(s, 8.8, 1.7, 3.95, 4.95, 'Slab con generaciones', [
    'Token = clase | generación | índice',
    'Un evento o timer tardío no alcanza al objeto que reutiliza el hueco',
    'Min-heap perezoso: refrescar un timeout cuesta O(1)',
  ], { size: 17 });
}
{
  const s = titled('HTTP/1.1 y TLS correctos por construcción', 'Decisiones técnicas · 2/3');
  bullets(s, [
    ['Parser por cabecera completa', 'busca \\r\\n\\r\\n recordando lo escaneado; rangos sobre el buffer, sin copias'],
    ['Cuerpos en streaming', '4 buffers de 16 KB por conexión; contrapresión natural, también a través de rustls (límite de 64 KB)'],
    ['Anti request smuggling', 'rechaza CL+TE, CL duplicados y obs-fold; nunca quita el encuadre aunque lo pida Connection'],
    ['Pipelining en serie y reintento acotado', 'orden garantizado sin colas; solo se reintenta lo idempotente, sin cuerpo, en conexión reutilizada'],
    ['TLS con rustls', 'API sin E/S que encaja con el loop; resolvedor SNI propio; wildcard de una etiqueta (RFC 6125); 421 si SNI ≠ Host'],
    ['Sin unsafe en el camino de datos', 'solo en master.rs (señales e inotify): 11 bloques'],
  ], { x: 0.6, y: 1.65, w: 12.1, h: 5.2 });
}
{
  const s = titled('Balanceo, salud, recarga y fallos', 'Decisiones técnicas · 3/3');
  card(s, 0.6, 1.7, 2.95, 4.95, 'least_load', [
    'X-Backend-Load, eliminada hacia el cliente',
    'EMA α=0,3, caduca a los 5 s',
    'Power of two choices',
  ], { size: 15 });
  card(s, 3.7, 1.7, 2.95, 4.95, 'Salud', [
    'Pasiva: fallos de connect/reset',
    'Activa: sondas TCP/HTTP, un hilo por generación',
    'Arc<ServerShared> atómico: lo único compartido',
  ], { size: 15 });
  card(s, 6.8, 1.7, 2.95, 4.95, 'Recarga (RCU)', [
    'inotify sobre el directorio',
    'El master compila una vez: Arc<Compiled>',
    'Cada worker hace swap de su Rc; lo en vuelo termina con la vieja',
  ], { size: 15 });
  card(s, 9.9, 1.7, 2.85, 4.95, 'Panic de un worker', [
    'Solo termina ese hilo',
    'Un guardia avisa al master al soltarse (unwinding)',
    'Relanzado con backoff; el tráfico sigue',
  ], { size: 15 });
}

// ---- 7. calidad ------------------------------------------------------
{
  const s = titled('Verificado de extremo a extremo', 'Calidad');
  kpi(s, 0.6, 1.75, 2.85, '43', 'tests unitarios (cargo test)');
  kpi(s, 3.65, 1.75, 2.85, '103', 'comprobaciones de integración (99 heredadas de C + 4 de panic)');
  kpi(s, 6.7, 1.75, 2.85, '0', 'avisos de cargo clippy');
  kpi(s, 9.75, 1.75, 2.95, '11', 'bloques unsafe, todos en master.rs');
  s.addText('Lo que encontraron las pruebas en esta sesión', { x: 0.6, y: 3.4, w: 12, h: 0.45, fontFace: FONT, fontSize: 18, bold: true, color: C.ink });
  bullets(s, [
    ['Carrera en las estadísticas (C, pthread)', 'el seqlock era una carrera de datos entre hilos; ThreadSanitizer la detectó y se cambió por un mutex'],
    ['Builds que no compilaban este código', 'los build/ copiados apuntaban a otro proyecto: los tests pasaban contra el código viejo'],
    ['Backend de pruebas en Rust lento', 'ponía a cero 64 KB por evento: limitaba HTTP a 298k req/s; con buffer reutilizado, 341k'],
  ], { x: 0.6, y: 3.85, w: 12.1, h: 2.9 }, 15);
}

// ---- 8. benchmark ----------------------------------------------------
{
  const s = titled('Meta de 50.000 req/s HTTPS: superada ×3,2 – ×4', 'Benchmark · Rust');
  s.addChart(pptx.ChartType.bar, [{ name: 'req/s', labels: BENCH.map((b) => b.esc), values: BENCH.map((b) => b.rps) }], {
    x: 0.5, y: 1.6, w: 7.6, h: 5.2, barDir: 'bar', chartColors: [C.accent],
    catAxisLabelFontFace: FONT, catAxisLabelFontSize: 13, catAxisOrientation: 'maxMin',
    valAxisLabelFontFace: FONT, valAxisLabelFontSize: 11, valAxisLabelFormatCode: '#,##0', valAxisMaxVal: 400000, valAxisMinVal: 0,
    valGridLine: { color: 'E9ECF1', size: 0.75 },
    showValue: true, dataLabelFontFace: FONT, dataLabelFontSize: 12, dataLabelFormatCode: '#,##0', dataLabelPosition: 'outEnd',
  });
  const rows = [[hdr('Escenario'), hdr('req/s', 1), hdr('p99', 1)],
    ...BENCH.map((b) => [b.esc, num(b.rps, true), { text: b.p99, options: { align: 'right' } }])];
  s.addTable(rows, { x: 8.4, y: 1.75, w: 4.4, colW: [1.8, 1.4, 1.2], ...TABLE, rowH: 0.42 });
  s.addText([
    { text: '0 errores', options: { bold: true, color: C.accent } },
    { text: ' en 26,95 M peticiones (todas 2xx), con solo 845 conexiones a los backends gracias al pool.', options: { breakLine: true } },
    { text: ' ', options: { fontSize: 6, breakLine: true } },
    { text: 'Proxy y backends en Rust · rustls + aws-lc-rs · Ryzen 7 7735HS (16 hilos) en WSL2 · 8 workers · release + LTO · 100 B · 30 s por escenario · CPUs fijadas.', options: { color: C.muted, fontSize: 12 } },
  ], { x: 8.4, y: 4.05, w: 4.4, h: 2.7, fontFace: FONT, fontSize: 14, color: C.ink, valign: 'top' });
}

// ---- 9. comparativa fork / pthread ----------------------------------
{
  const s = titled('C: procesos (fork) frente a hilos (pthread)', 'Comparativa de ejecuciones · 1/3');
  groupedChart(s, [['fork (procesos)', FORK], ['pthread (hilos)', PTHREAD2]], [C.grey, C.accent], { x: 0.5, y: 1.55, w: 7.4, h: 5.3 });
  s.addTable([
    [hdr('Escenario'), hdr('fork', 1), hdr('pthread', 1), hdr('Δ', 1)],
    ...ESC.map((e, i) => [e, num(FORK[i]), num(PTHREAD2[i]), { text: pct(PTHREAD2[i], FORK[i]), options: { align: 'right', bold: true } }]),
  ], { x: 8.2, y: 1.7, w: 4.6, colW: [1.6, 1.1, 1.1, 0.8], ...TABLE });
  bullets(s, [
    ['HTTPS', 'equivalentes: las diferencias son del tamaño del ruido entre ejecuciones'],
    ['HTTP', 'hilos +6-13 % en las dos ejecuciones'],
    ['Anomalía', 'fork #1 c400: 400 timeouts y 2,5 min; no se reprodujo (excluida de la media)'],
  ], { x: 8.2, y: 3.95, w: 4.6, h: 2.9 }, 13);
}

// ---- 10. comparativa Rust / C ---------------------------------------
{
  const s = titled('Rust frente a C', 'Comparativa de ejecuciones · 2/3');
  groupedChart(s, [['C + OpenSSL (backend C)', C_PTHREAD], ['Rust + rustls (backend C)', RUST_CBACK], ['Rust (backend Rust)', RUST_FULL]],
    [C.grey, C.accent2, C.accent], { x: 0.5, y: 1.55, w: 7.6, h: 5.3 });
  s.addTable([
    [hdr('Escenario'), hdr('C', 1), hdr('Rust*', 1), hdr('Δ', 1)],
    ...ESC.map((e, i) => [e, num(C_PTHREAD[i]), num(RUST_CBACK[i]), { text: pct(RUST_CBACK[i], C_PTHREAD[i]), options: { align: 'right', bold: true } }]),
  ], { x: 8.35, y: 1.7, w: 4.45, colW: [1.5, 1.1, 1.1, 0.75], ...TABLE });
  bullets(s, [
    ['HTTP', 'con el mismo backend, Rust iguala o supera a C: el proxy no es más lento'],
    ['HTTPS', '8-12 % menos: coste por registro de rustls con respuestas de 100 B; cambiar ring por aws-lc-rs no lo mueve'],
  ], { x: 8.35, y: 3.95, w: 4.45, h: 2.4 }, 13);
  s.addText('* Rust + aws-lc-rs con el backend en C, para aislar el proxy.', { x: 8.35, y: 6.4, w: 4.45, h: 0.35, fontFace: FONT, fontSize: 11, color: C.muted, italic: true });
}

// ---- 11. evolución ---------------------------------------------------
{
  const s = titled('Evolución: rendimiento frente a garantías', 'Comparativa de ejecuciones · 3/3');
  s.addTable([
    [hdr('Escenario (media)'), hdr('C, fork', 1), hdr('C, pthread', 1), hdr('Rust', 1), hdr('Rust / C pthread', 1)],
    ...ESC.map((e, i) => [e, num(EVOL.fork[i]), num(EVOL.pthread[i]), num(EVOL.rust[i], true), { text: pct(EVOL.rust[i], EVOL.pthread[i]), options: { align: 'right', bold: true, color: C.accent } }]),
    [{ text: 'Ejecuciones', options: { color: C.muted } }, { text: '2 (c400: 1)', options: { align: 'right', color: C.muted } }, { text: '3', options: { align: 'right', color: C.muted } }, { text: '2', options: { align: 'right', color: C.muted } }, ''],
  ], { x: 0.6, y: 1.7, w: 12.1, colW: [3.1, 2.1, 2.1, 2.1, 2.7], ...TABLE, fontSize: 15, rowH: 0.48 });
  card(s, 0.6, 5.0, 5.9, 1.8, 'Lo que cuesta Rust', [
    '~10 % menos en HTTPS con respuestas pequeñas (rustls)',
    'Opciones: API unbuffered de rustls u OpenSSL desde Rust',
  ], { fill: C.warnSoft, line: C.warn, size: 15 });
  card(s, 6.85, 5.0, 5.9, 1.8, 'Lo que aporta Rust', [
    'Seguridad de memoria y sin carreras de datos en todo el camino de datos',
    'Workers que se recuperan de un panic; certificados leídos una vez',
  ], { size: 15 });
}

// ---- 12. coste -------------------------------------------------------
{
  const s = titled('Coste de generarlo', 'Claude Code · Claude Opus 5.5');
  const totalIn = COSTE.tokens.entrada + COSTE.tokens.cache_escritura_1h + COSTE.tokens.cache_lectura;
  kpi(s, 0.6, 1.75, 2.85, '≈ ' + money(COSTE_TOTAL), 'USD, equivalente a precios de API');
  kpi(s, 3.65, 1.75, 2.85, mill(totalIn), 'tokens de entrada (' + Math.round((100 * COSTE.tokens.cache_lectura) / totalIn) + ' % desde caché)');
  kpi(s, 6.7, 1.75, 2.85, mill(COSTE.tokens.salida), 'tokens de salida (código, docs y razonamiento)');
  kpi(s, 9.75, 1.75, 2.95, COSTE.duracion, 'de sesión: pthread en C y port a Rust');

  const r = (label, k) => [label, { text: mill(COSTE.tokens[k]), options: { align: 'right' } }, { text: money(COSTE.precio[k]) + '/M', options: { align: 'right', color: C.muted } }, { text: money(usd(k)), options: { align: 'right', bold: true } }];
  s.addTable([
    [hdr('Concepto'), hdr('Tokens', 1), hdr('Precio', 1), hdr('USD', 1)],
    r('Lectura de caché (contexto reutilizado)', 'cache_lectura'),
    r('Salida generada', 'salida'),
    r('Escritura de caché (TTL 1 h)', 'cache_escritura_1h'),
    [{ text: 'Total', options: { bold: true } }, '', '', { text: money(COSTE_TOTAL), options: { align: 'right', bold: true, color: C.accent } }],
  ], { x: 0.6, y: 3.45, w: 7.4, colW: [3.5, 1.2, 1.2, 1.5], ...TABLE, rowH: 0.42 });

  s.addText([
    { text: 'Qué se obtuvo por ese coste', options: { bold: true, breakLine: true } },
    { text: 'Versión C con hilos (verificada con ASan, UBSan y TSan), port completo a Rust (~7.100 líneas, 146 pruebas), 11 ejecuciones de benchmark, comparativas y 2 repos en GitHub.', options: { breakLine: true } },
    { text: ' ', options: { fontSize: 6, breakLine: true } },
    { text: 'Intervención humana: 11 mensajes cortos ("sí, haz el merge", "pasa el proyecto a Rust con hilos"...).', options: { color: C.muted, fontSize: 12, breakLine: true } },
    { text: ' ', options: { fontSize: 6, breakLine: true } },
    { text: 'No incluye esta documentación. Con plan Claude Max no se factura por token: la cifra es la referencia a precios de API.', options: { color: C.warn, fontSize: 12, italic: true } },
  ], { x: 8.3, y: 3.45, w: 4.45, h: 3.3, fontFace: FONT, fontSize: 14, color: C.ink, valign: 'top' });
}

// ---- 13. estado ------------------------------------------------------
{
  const s = titled('Estado y próximos pasos', 'Cierre');
  card(s, 0.6, 1.7, 5.9, 4.9, 'Entregado', [
    'Proxy en Rust completo y verificado en Linux',
    'Misma batería que la versión C, en verde (103/103)',
    'Benchmark reproducible y comparativas con C (bench/results/)',
    'Documentación técnica y manual de usuario (PDF)',
    'Trazabilidad requerimiento → test (docs/verificacion.md)',
  ], { size: 17 });
  card(s, 6.85, 1.7, 5.9, 4.9, 'Pendiente / limitaciones', [
    'Compilar y probar en macOS/BSD',
    'Recortar el ~10 % de HTTPS (rustls unbuffered u OpenSSL)',
    'Vídeo demo',
    'Cliente HTTP/1.0 recibe chunked sin convertir',
    'Cambiar el nº de workers requiere reiniciar',
  ], { fill: C.warnSoft, line: C.warn, size: 17 });
}

const out = path.join(here, 'proxy-l7.pptx');
await pptx.writeFile({ fileName: out });
console.log('pptx:', out);
