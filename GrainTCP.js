const CFG = {
  id: 'dd052c98-bd4a-4c04-a0a7-bfd2fa9f5001',
  chunk: 64 * 1024,
  dnPack: 32 * 1024,
  dnTail: 512,
  dnMs: 0,
  upPack: 16 * 1024,
  upQMax: 256 * 1024,
  maxED: 8 * 1024,
  concur: 4
};

export default {
  fetch: req =>
    req.headers.get('Upgrade')?.toLowerCase() === 'websocket'
      ? ws(req)
      : new Response('Hello world!')
};

// ---------- 工具函数 ----------
const sprout = (f, h, p, s = f.connect({ hostname: h, port: p })) =>
  s.opened.then(() => s);

const raceSprout = (f, h, p) => {
  if (!f?.connect) return Promise.reject(new Error('connect unavailable'));
  if (CFG.concur <= 1) return sprout(f, h, p);
  const ts = Array(CFG.concur).fill().map(() => sprout(f, h, p));
  return Promise.any(ts).then(w => {
    ts.forEach(t => t.then(s => s !== w && s.close(), () => {}));
    return w;
  });
};

// 上行队列（缓冲客户端发来的数据）
const mkQ = (cap, qCap = cap, itemsMax = Math.max(1, qCap >> 8)) => {
  let q = [], h = 0, qB = 0, buf = null;
  const trim = () => { h > 32 && h * 2 >= q.length && (q = q.slice(h), h = 0); };
  const take = () => { if (h >= q.length) return null; const d = q[h]; q[h++] = undefined; qB -= d.byteLength; trim(); return d; };
  return {
    get bytes() { return qB; },
    get size() { return q.length - h; },
    get empty() { return h >= q.length; },
    clear() { q = []; h = 0; qB = 0; },
    sow(d) {
      const n = d?.byteLength || 0;
      if (!n) return 1;
      if (qB + n > qCap || q.length - h >= itemsMax) return 0;
      q.push(d);
      qB += n;
      return 1;
    },
    bundle(d) {
      d ||= take();
      if (!d || h >= q.length || d.byteLength >= cap) return [d, 0];
      let n = d.byteLength, e = h;
      while (e < q.length) {
        const x = q[e], nn = n + x.byteLength;
        if (nn > cap) break;
        n = nn;
        e++;
      }
      if (e === h) return [d, 0];
      const out = buf ||= new Uint8Array(cap);
      out.set(d);
      for (let o = d.byteLength; h < e;) {
        const x = q[h];
        q[h++] = undefined;
        qB -= x.byteLength;
        out.set(x, o);
        o += x.byteLength;
      }
      trim();
      return [out.subarray(0, n), 1];
    }
  };
};

// 下行发送（将目标数据发回客户端）
const mkDn = w => {
  const cap = CFG.dnPack, tail = CFG.dnTail, low = Math.max(4096, tail << 3);
  let pb = new Uint8Array(cap), p = 0, tp = 0, mq = 0, gen = 0, qk = 0, qr = 0;
  const reap = () => {
    tp && clearTimeout(tp);
    tp = 0;
    mq = 0;
    if (!p) return;
    w.send(pb.subarray(0, p).slice());
    pb = new Uint8Array(cap);
    p = 0;
    qr = 0;
  };
  const ripen = () => {
    if (tp || mq) return;
    mq = 1;
    qk = gen;
    queueMicrotask(() => {
      mq = 0;
      if (!p || tp) return;
      if (cap - p < tail) return reap();
      tp = setTimeout(() => {
        tp = 0;
        if (!p) return;
        if (cap - p < tail) return reap();
        if (qr < 2 && (gen !== qk || p < low)) { qr++; qk = gen; return ripen(); }
        reap();
      }, Math.max(CFG.dnMs, 1));
    });
  };
  return {
    send(u) {
      let o = 0, n = u?.byteLength || 0;
      if (!n) return;
      while (o < n) {
        if (!p && n - o >= cap) {
          const m = Math.min(cap, n - o);
          w.send(o || m !== n ? u.subarray(o, o + m) : u);
          o += m;
          continue;
        }
        const m = Math.min(cap - p, n - o);
        pb.set(u.subarray(o, o + m), p);
        p += m;
        o += m;
        gen++;
        if (p === cap || cap - p < tail) reap();
        else ripen();
      }
    },
    reap
  };
};

// 从目标读取数据并转发给客户端
const mill = async (rd, w) => {
  const r = rd.getReader({ mode: 'byob' }), tx = mkDn(w);
  let buf = new ArrayBuffer(CFG.chunk);
  try {
    for (;;) {
      const { done, value: v } = await r.read(new Uint8Array(buf, 0, CFG.chunk));
      if (done) break;
      if (!v?.byteLength) continue;
      if (v.byteLength >= (CFG.chunk >> 1)) {
        tx.reap();
        w.send(v);
        buf = new ArrayBuffer(CFG.chunk);
      } else {
        tx.send(v.slice());
        buf = v.buffer;
      }
    }
    tx.reap();
  } catch {} finally {
    try { tx.reap(); } catch {}
    try { r.releaseLock(); } catch {}
  }
};

// ---------- WebSocket 主逻辑 ----------
const ws = async req => {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept({ allowHalfOpen: true });
  server.binaryType = 'arraybuffer';
  const fetcher = req.fetcher;

  // 可选：ed 握手（sec-websocket-protocol）
  const edStr = req.headers.get('sec-websocket-protocol');
  const ed = edStr && edStr.length <= CFG.maxED * 4 / 3 + 4
    ? Uint8Array.fromBase64(edStr, { alphabet: 'base64url' })
    : null;

  let curW = null, sock = null, closed = false, busy = false;
  const uq = mkQ(CFG.upPack, CFG.upQMax, CFG.upQMax >> 8);

  const wither = () => {
    if (closed) return;
    closed = true;
    uq.clear();
    try { curW?.releaseLock(); } catch {}
    try { sock?.close(); } catch {}
    try { server.close(); } catch {}
  };

  const toU8 = d =>
    d instanceof Uint8Array ? d
    : ArrayBuffer.isView(d) ? new Uint8Array(d.buffer, d.byteOffset, d.byteLength)
    : new Uint8Array(d);

  const sow = d => {
    const u = toU8(d), n = u.byteLength;
    if (!n) return 1;
    if (uq.sow(u)) return 1;
    wither();
    return 0;
  };

  // ---------- 路径解析：必须 /proxyip=目标 ----------
  const url = new URL(req.url);
  const pathname = url.pathname;
  if (!pathname.startsWith('/proxyip=')) {
    return new Response('Bad Request: missing /proxyip= in path', { status: 400 });
  }
  const target = pathname.substring('/proxyip='.length);
  if (!target) {
    return new Response('Bad Request: empty target', { status: 400 });
  }
  let host = target, port = 443;
  if (target.includes(':')) {
    const parts = target.split(':');
    host = parts[0];
    port = parseInt(parts[1], 10);
    if (isNaN(port) || port < 1 || port > 65535) port = 443;
  }
  const pathTarget = { host, port };
  // ------------------------------------------------

  const thresh = async () => {
    if (busy || closed) return;
    busy = true;
    try {
      for (;;) {
        if (closed) break;
        if (!sock) {
          const [first] = uq.bundle();
          let payload = first || null;
          // 发送握手成功标志（可选）
          server.send(new Uint8Array([0]));
          sock = await raceSprout(fetcher, pathTarget.host, pathTarget.port);
          if (!sock) throw wither();
          curW = sock.writable.getWriter();
          if (payload && payload.byteLength) {
            await curW.write(payload);
          }
          mill(sock.readable, server).finally(() => wither());
          continue;
        }
        const [d] = uq.bundle();
        if (!d) break;
        await curW.write(d);
      }
    } catch { wither(); } finally {
      busy = false;
      if (!uq.empty && !closed) queueMicrotask(thresh);
    }
  };

  if (ed && sow(ed)) thresh();

  server.addEventListener('message', e => {
    if (!closed) {
      sow(e.data) && thresh();
    }
  });
  server.addEventListener('close', () => wither());
  server.addEventListener('error', () => wither());

  return new Response(null, {
    status: 101,
    webSocket: client,
    headers: { 'Sec-WebSocket-Extensions': '' }
  });
};
