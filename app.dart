import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:wakelock_plus/wakelock_plus.dart';

// ===== ESTRATEGIAS =====
class Candle {
  final int t; final double o, h, l, c;
  Candle(this.t, this.o, this.h, this.l, this.c);
}
class Vote { int buys = 0, sells = 0; double atr = 0; }

List<double> _ema(List<double> v, int n) {
  final k = 2 / (n + 1), r = <double>[v[0]];
  for (var i = 1; i < v.length; i++) r.add(v[i] * k + r[i - 1] * (1 - k));
  return r;
}
double _sma(List<double> v, int n) => v.sublist(v.length - n).reduce((a, b) => a + b) / n;
double _rsi(List<double> c, int n) {
  var g = 0.0, l = 0.0;
  for (var i = 1; i <= n; i++) { final d = c[i] - c[i - 1]; d > 0 ? g += d : l -= d; }
  g /= n; l /= n;
  for (var i = n + 1; i < c.length; i++) {
    final d = c[i] - c[i - 1];
    g = (g * (n - 1) + max(d, 0)) / n; l = (l * (n - 1) + max(-d, 0)) / n;
  }
  return l == 0 ? 100 : 100 - 100 / (1 + g / l);
}
double _atr(List<Candle> k, int n) {
  var s = 0.0;
  for (var i = k.length - n; i < k.length; i++) {
    s += [k[i].h - k[i].l, (k[i].h - k[i - 1].c).abs(), (k[i].l - k[i - 1].c).abs()].reduce(max);
  }
  return s / n;
}
Vote vote(List<Candle> k) {
  final c = k.map((x) => x.c).toList(), last = c.last, n = k.length;
  final s20 = _sma(c, 20), s50 = _sma(c, 50), e200 = _ema(c, 200).last, atr = _atr(k, 14);
  final e12 = _ema(c, 12), e26 = _ema(c, 26);
  final macd = [for (var i = 0; i < n; i++) e12[i] - e26[i]], sig = _ema(macd, 9);
  final m = macd.last, sg = sig.last;
  final sd = sqrt(c.sublist(n - 20).map((x) => pow(x - s20, 2)).reduce((a, b) => a + b) / 20);
  final v = List<int>.filled(7, 0);
  v[0] = s20 > s50 ? 1 : -1;
  final prev = k.sublist(n - 21, n - 1);
  if (last > e200 && last > prev.map((x) => x.h).reduce(max)) v[1] = 1;
  if (last < e200 && last < prev.map((x) => x.l).reduce(min)) v[1] = -1;
  final r = _rsi(c, 14);
  if (r < 30) v[2] = 1; else if (r > 70) v[2] = -1;
  if (m > sg && m > 0) v[3] = 1; else if (m < sg && m < 0) v[3] = -1;
  if (last < s20 - 2 * sd) v[4] = 1; else if (last > s20 + 2 * sd) v[4] = -1;
  if (last < s50 - atr) v[5] = 1; else if (last > s50 + atr) v[5] = -1;
  final w = k.sublist(n - 50);
  var hi = 0, lo = 0;
  for (var i = 0; i < w.length; i++) {
    if (w[i].h > w[hi].h) hi = i;
    if (w[i].l < w[lo].l) lo = i;
  }
  final hH = w[hi].h, lL = w[lo].l, rg = hH - lL;
  if (rg > 0) {
    if (lo < hi && last <= hH - .382 * rg && last >= hH - .618 * rg) v[6] = 1;
    if (hi < lo && last >= lL + .382 * rg && last <= lL + .618 * rg) v[6] = -1;
  }
  final o = Vote()..atr = atr;
  for (final x in v) { if (x > 0) o.buys++; if (x < 0) o.sells++; }
  return o;
}

// ===== cTRADER OPEN API =====
class CT {
  final bool live;
  CT(this.live);
  WebSocket? _ws; Timer? _hb; int? acct; int _n = 0;
  final _p = <String, Completer<Map>>{};

  Future<void> connect() async {
    _ws = await WebSocket.connect('wss://${live ? 'live' : 'demo'}.ctraderapi.com:5036');
    _ws!.listen((m) {
      final j = jsonDecode(m as String) as Map, id = j['clientMsgId'];
      if (id != null && _p.containsKey(id)) _p.remove(id)!.complete(j);
    }, onDone: () { for (final c in _p.values) { c.completeError('desconectado'); } _p.clear(); });
    _hb = Timer.periodic(const Duration(seconds: 20),
        (_) => _ws?.add(jsonEncode({'payloadType': 51, 'payload': {}})));
  }
  Future<Map> req(int t, Map p) async {
    final id = 'm${_n++}', c = Completer<Map>();
    _p[id] = c;
    _ws!.add(jsonEncode({'clientMsgId': id, 'payloadType': t, 'payload': p}));
    final r = await c.future.timeout(const Duration(seconds: 20));
    final pt = r['payloadType'];
    final pl = (r['payload'] as Map?) ?? {};
    if (pt == 2142 || pt == 50 || pt == 2132) throw '${pl['description'] ?? pl['errorCode'] ?? pl}';
    return pl;
  }
  Future<void> login(String id, String secret, String token) async {
    await req(2100, {'clientId': id, 'clientSecret': secret});
    final l = await req(2149, {'accessToken': token});
    final list = (l['ctidTraderAccount'] as List).cast<Map>().where((a) => (a['isLive'] == true) == live).toList();
    if (list.isEmpty) throw 'Nenhuma conta ${live ? 'real' : 'demo'} neste token';
    acct = (list.first['ctidTraderAccountId'] as num).toInt();
    await req(2102, {'ctidTraderAccountId': acct, 'accessToken': token});
  }
  Future<Map<String, int>> symbolIds() async {
    final r = await req(2114, {'ctidTraderAccountId': acct});
    return {for (final s in (r['symbol'] as List).cast<Map>()) s['symbolName'] as String: (s['symbolId'] as num).toInt()};
  }
  Future<Map> detail(int id) async {
    final r = await req(2116, {'ctidTraderAccountId': acct, 'symbolId': [id]});
    return (r['symbol'] as List).first as Map;
  }
  Future<List<Candle>> bars(int id) async {
    final now = DateTime.now().millisecondsSinceEpoch;
    final r = await req(2137, {
      'ctidTraderAccountId': acct, 'symbolId': id, 'period': 9,
      'fromTimestamp': now - 25 * 86400000, 'toTimestamp': now,
    });
    return (r['trendbar'] as List).cast<Map>().map((b) {
      final lo = (b['low'] as num).toDouble();
      double p(String k) => (lo + ((b[k] as num?) ?? 0)) / 100000;
      return Candle((b['utcTimestampInMinutes'] as num).toInt(), p('deltaOpen'), p('deltaHigh'), lo / 100000, p('deltaClose'));
    }).toList();
  }
  Future<List<Map>> positions() async =>
      ((await req(2124, {'ctidTraderAccountId': acct}))['position'] as List? ?? []).cast<Map>();
  Future<void> market(int sid, bool buy, int vol, double slDist, double tpDist) => req(2106, {
        'ctidTraderAccountId': acct, 'symbolId': sid, 'orderType': 1, 'tradeSide': buy ? 1 : 2,
        'volume': vol, 'relativeStopLoss': (slDist * 100000).round(), 'relativeTakeProfit': (tpDist * 100000).round(),
      });
  void close() { _hb?.cancel(); _ws?.close(); }
}

// ===== APP =====
void main() => runApp(MaterialApp(
    theme: ThemeData(colorSchemeSeed: Colors.teal, useMaterial3: true), home: const Home()));

class Home extends StatefulWidget { const Home({super.key}); @override State<Home> createState() => _S(); }

class _S extends State<Home> {
  final cid = TextEditingController(), sec = TextEditingController(), tok = TextEditingController();
  final lots = TextEditingController(text: '0.01'), votes = TextEditingController(text: '3');
  final syms = TextEditingController(text: 'EURUSD,GBPUSD,XAUUSD,BTCUSD,ETHUSD');
  bool live = false, running = false, busy = false;
  CT? ct; Timer? tm;
  final logs = <String>[], seen = <String, int>{}, ids = <String, int>{}, det = <int, Map>{};

  void log(String s) { if (mounted) setState(() => logs.insert(0, '${TimeOfDay.now().format(context)}  $s')); }

  Future<void> start() async {
    if (live) {
      final ok = await showDialog<bool>(context: context, builder: (c) => AlertDialog(
        title: const Text('Conta REAL'),
        content: const Text('O app vai operar com dinheiro real. Confirmar?'),
        actions: [TextButton(onPressed: () => Navigator.pop(c, false), child: const Text('Cancelar')),
          FilledButton(onPressed: () => Navigator.pop(c, true), child: const Text('Confirmar'))]));
      if (ok != true) return;
    }
    setState(() => running = true);
    try {
      ct = CT(live);
      await ct!.connect();
      await ct!.login(cid.text.trim(), sec.text.trim(), tok.text.trim());
      log('Conectado (${live ? 'REAL' : 'DEMO'}) conta ${ct!.acct}');
      final all = await ct!.symbolIds();
      ids.clear(); seen.clear();
      for (final s in syms.text.split(',').map((e) => e.trim())) {
        all.containsKey(s) ? ids[s] = all[s]! : log('Simbolo nao existe: $s');
      }
      WakelockPlus.enable();
      tm = Timer.periodic(const Duration(seconds: 60), (_) => scan());
      scan();
    } catch (e) { log('Erro: $e'); stop(); }
  }

  void stop() {
    tm?.cancel(); ct?.close(); WakelockPlus.disable();
    if (mounted) setState(() => running = false);
  }

  Future<void> scan() async {
    if (busy || ct == null) return;
    busy = true;
    try {
      final open = (await ct!.positions()).map((p) => ((p['tradeData'] as Map)['symbolId'] as num).toInt()).toList();
      for (final e in ids.entries) {
        var b = await ct!.bars(e.value);
        if (b.length < 211) continue;
        b = b.sublist(0, b.length - 1);
        if (seen[e.key] == b.last.t) continue;
        seen[e.key] = b.last.t;
        final v = vote(b);
        log('${e.key}: compra ${v.buys} | venda ${v.sells}');
        final need = int.tryParse(votes.text) ?? 3;
        final dir = v.buys >= need && v.buys > v.sells ? 1 : (v.sells >= need && v.sells > v.buys ? -1 : 0);
        if (dir == 0 || open.contains(e.value) || open.length >= 5) continue;
        final d = det[e.value] ??= await ct!.detail(e.value);
        final lot = (d['lotSize'] as num).toInt(), step = (d['stepVolume'] as num).toInt(), mn = (d['minVolume'] as num).toInt();
        var vol = ((double.parse(lots.text) * lot) / step).floor() * step;
        if (vol < mn) vol = mn;
        await ct!.market(e.value, dir > 0, vol, v.atr * 1.5, v.atr * 3);
        open.add(e.value);
        log('ORDEM ${dir > 0 ? 'COMPRA' : 'VENDA'} ${e.key}');
      }
    } catch (e) { log('Erro: $e'); }
    busy = false;
  }

  Widget f(String l, TextEditingController c, {bool obscure = false}) => Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: TextField(controller: c, obscureText: obscure, enabled: !running,
          decoration: InputDecoration(labelText: l, border: const OutlineInputBorder(), isDense: true)));

  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('TradeNexusPro')),
      body: ListView(padding: const EdgeInsets.all(16), children: [
        f('Client ID (openapi.ctrader.com)', cid), f('Client Secret', sec, obscure: true),
        f('Access Token', tok, obscure: true), f('Simbolos (nomes exatos da IC Markets)', syms),
        Row(children: [Expanded(child: f('Lotes', lots)), const SizedBox(width: 8), Expanded(child: f('Min. estrategias', votes))]),
        SwitchListTile(title: Text(live ? 'Conta REAL' : 'Conta DEMO'), value: live,
            onChanged: running ? null : (v) => setState(() => live = v)),
        FilledButton(onPressed: running ? stop : start, child: Text(running ? 'Parar' : 'Iniciar robo')),
        const Divider(height: 24),
        ...logs.take(60).map((l) => Text(l, style: const TextStyle(fontSize: 13))),
      ]));
}
