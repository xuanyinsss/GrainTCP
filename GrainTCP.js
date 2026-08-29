import { connect } from 'cloudflare:sockets';

/*
 * GrainTCP
 * + EdgeTunnel ProxyIP fallback
 * + PATH
 *
 * UUID:
 * 2523c510-9ff0-415b-9582-93949bfae7e3
 */

const CFG = {
  id: '2523c510-9ff0-415b-9582-93949bfae7e3',

  // WebSocket PATH
  path: '/proxyip',

  // ProxyIP
  // 可以填写：
  // 1.2.3.4
  //
  // 多个：
  // 1.2.3.4,5.6.7.8
  proxyIP: '',

  chunk: 64 * 1024,
  dnPack: 32 * 1024,
  dnTail: 512,
  dnQr: 4,
  upPack: 20 * 1024,
  maxED: 8 * 1024,
  concur: 4
};

const normalizePath = p => {
  p = String(p || CFG.path).trim();

  if (!p.startsWith('/')) {
    p = '/' + p;
  }

  if (p.length > 1) {
    p = p.replace(/\/+$/, '');
  }

  return p || '/';
};

const parseProxyIP = value => {
  return String(value || '')
    .split(',')
    .map(x => x.trim())
    .filter(Boolean);
};


/* =========================
 * Worker入口
 * ========================= */

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    const path = normalizePath(
      env?.PATH || CFG.path
    );

    if (url.pathname !== path) {
      if (url.pathname === '/') {
        return new Response(
          'GrainTCP ProxyIP',
          { status: 200 }
        );
      }

      return new Response(
        'Not Found',
        { status: 404 }
      );
    }

    if (
      req.headers.get('Upgrade')?.toLowerCase() !==
      'websocket'
    ) {
      return new Response(
        'WebSocket Required',
        { status: 426 }
      );
    }

    return ws(
      req,
      env
    );
  }
};


/* =========================
 * UUID解析
 * ========================= */

const hex = c =>
  (c > 64 ? c + 9 : c) & 0xF;

const idB =
  new Uint8Array(16);

const dec =
  new TextDecoder();

for (
  let i = 0,
      p = 0,
      c,
      h;
  i < 16;
  i++
) {
  c =
    CFG.id.charCodeAt(p++);

  if (c === 45) {
    c =
      CFG.id.charCodeAt(p++);
  }

  h = hex(c);

  c =
    CFG.id.charCodeAt(p++);

  if (c === 45) {
    c =
      CFG.id.charCodeAt(p++);
  }

  idB[i] =
    h << 4 |
    hex(c);
}

const [
  I0,I1,I2,I3,
  I4,I5,I6,I7,
  I8,I9,I10,I11,
  I12,I13,I14,I15
] = idB;

const matchID = c =>
  c[1] === I0 &&
  c[2] === I1 &&
  c[3] === I2 &&
  c[4] === I3 &&
  c[5] === I4 &&
  c[6] === I5 &&
  c[7] === I6 &&
  c[8] === I7 &&
  c[9] === I8 &&
  c[10] === I9 &&
  c[11] === I10 &&
  c[12] === I11 &&
  c[13] === I12 &&
  c[14] === I13 &&
  c[15] === I14 &&
  c[16] === I15;


/* =========================
 * 地址
 * ========================= */

const addr = (t, b) =>
  t === 1
    ? `${b[0]}.${b[1]}.${b[2]}.${b[3]}`
    : t === 3
      ? dec.decode(b)
      : `[${Array.from(
          { length: 8 },
          (_, i) =>
            (
              (b[i * 2] << 8) |
              b[i * 2 + 1]
            ).toString(16)
        ).join(':')}]`;


/* =========================
 * TCP连接
 * ========================= */

const sprout = (
  f,
  h,
  p,
  s = f.connect({
    hostname: h,
    port: p
  })
) =>
  s.opened.then(
    () => s
  );

const raceSprout = (
  f,
  h,
  p
) => {

  if (!f?.connect) {
    return Promise.reject(
      new Error(
        'connect unavailable'
      )
    );
  }

  if (CFG.concur <= 1) {
    return sprout(
      f,
      h,
      p
    );
  }

  const ts =
    Array(CFG.concur)
      .fill()
      .map(() =>
        sprout(
          f,
          h,
          p
        )
      );

  return Promise.any(ts)
    .then(w => {

      ts.forEach(t =>
        t.then(
          s => {
            if (s !== w) {
              try {
                s.close();
              } catch {}
            }
          },
          () => {}
        )
      );

      return w;
    });
};


/* =========================
 * 地址解析
 * ========================= */

const parseAddr = (
  b,
  o,
  t
) => {

  const l =
    t === 3
      ? b[o++]
      : t === 1
        ? 4
        : t === 4
          ? 16
          : null;

  if (l === null) {
    return null;
  }

  const n =
    o + l;

  if (n > b.length) {
    return null;
  }

  return {
    targetAddrBytes:
      b.subarray(o, n),

    dataOffset: n
  };
};


/* =========================
 * GrainTCP Header
 * ========================= */

const relay = c => {

  if (
    c.length < 24 ||
    !matchID(c)
  ) {
    return null;
  }

  let o =
    19 + c[17];

  const p =
    (c[o] << 8) |
    c[o + 1];

  let t =
    c[o + 2];

  if (t !== 1) {
    t += 1;
  }

  const a =
    parseAddr(
      c,
      o + 3,
      t
    );

  if (!a) {
    return null;
  }

  return {
    addrType: t,
    ...a,
    port: p
  };
};


/* =========================
 * 队列
 * ========================= */

const mkK = (
  cap,
  cpy = 0
) => {

  let q = [];
  let h = 0;
  let b = 0;
  let buf = null;

  const e = () =>
    h >= q.length;

  const trim = () => {

    if (
      h > 32 &&
      h * 2 >= q.length
    ) {
      q = q.slice(h);
      h = 0;
    }
  };

  const clear = () => {
    q = [];
    h = 0;
    b = 0;
  };

  const take = () => {

    if (e()) {
      return null;
    }

    const d =
      q[h];

    q[h++] =
      undefined;

    b -=
      d.byteLength;

    trim();

    return d;
  };

  const sow = d => {

    const n =
      d?.byteLength || 0;

    if (!n) {
      return 1;
    }

    q.push(d);
    b += n;

    return 1;
  };

  const pack = d => {

    d ||= take();

    if (!d || e()) {
      return [d, 0];
    }

    let n =
      d.byteLength;

    let j = h;

    while (
      j < q.length
    ) {

      const x =
        q[j];

      const nn =
        n + x.byteLength;

      if (nn > cap) {
        break;
      }

      n = nn;
      j++;
    }

    if (j === h) {
      return [d, 0];
    }

    const out =
      buf ||= new Uint8Array(
        cap
      );

    out.set(d);

    for (
      let o = d.byteLength;
      h < j;
    ) {

      const x =
        q[h];

      q[h++] =
        undefined;

      b -=
        x.byteLength;

      out.set(
        x,
        o
      );

      o +=
        x.byteLength;
    }

    trim();

    const u =
      out.subarray(
        0,
        n
      );

    return [
      cpy
        ? u.slice()
        : u,
      1
    ];
  };

  return {
    e,

    get b() {
      return b;
    },

    clear,
    take,
    sow,
    pack
  };
};

const mkQ = cap => {

  const k =
    mkK(cap);

  return {
    get empty() {
      return k.e();
    },

    clear:
      k.clear,

    sow:
      k.sow,

    bundle:
      d => k.pack(d)
  };
};


/* =========================
 * 下行优化
 * ========================= */

const mkDn = w => {

  const cap =
    CFG.dnPack;

  const tail =
    CFG.dnTail;

  const low =
    Math.max(
      4096,
      tail * 12
    );

  const k =
    mkK(
      cap,
      1
    );

  let tp = 0;
  let gen = 0;
  let qk = 0;
  let qr = 0;

  const reap = () => {

    if (tp) {
      clearTimeout(tp);
    }

    tp = 0;
    qr = 0;

    for (;;) {

      const [u] =
        k.pack();

      if (!u) {
        break;
      }

      w.send(u);
    }
  };

  const ripen = () => {

    if (
      k.e() ||
      tp
    ) {
      return;
    }

    if (
      k.b >= cap ||
      cap - k.b < tail
    ) {
      return reap();
    }

    tp =
      setTimeout(() => {

        tp = 0;

        if (k.e()) {
          return;
        }

        if (
          k.b >= cap ||
          cap - k.b < tail
        ) {
          return reap();
        }

        if (
          qr < CFG.dnQr &&
          (
            gen !== qk ||
            k.b < low
          )
        ) {
          qr++;
          qk = gen;

          return ripen();
        }

        reap();

      }, 1);
  };

  return {

    send(u) {

      let o = 0;

      const n =
        u?.byteLength || 0;

      if (!n) {
        return;
      }

      while (
        o < n
      ) {

        const m =
          Math.min(
            cap - k.b,
            n - o
          );

        if (!m) {
          reap();
          continue;
        }

        k.sow(
          o ||
          m !== n
            ? u.subarray(
                o,
                o + m
              )
            : u
        );

        gen++;

        o += m;

        if (
          k.b >= cap ||
          cap - k.b < tail
        ) {
          reap();
        } else {
          ripen();
        }
      }
    },

    reap
  };
};


/* =========================
 * WebSocket → TCP
 * ========================= */

const ws = async (
  req,
  env
) => {

  const [
    client,
    server
  ] = Object.values(
    new WebSocketPair()
  );

  server.accept({
    allowHalfOpen: true
  });

  server.binaryType =
    'arraybuffer';

  /*
   * Cloudflare Worker使用connect()
   */
  const fetcher = {
    connect
  };

  /*
   * ProxyIP：
   *
   * PROXYIP=1.2.3.4
   *
   * 或：
   *
   * PROXYIP=1.2.3.4,5.6.7.8
   */
  const proxyIPs =
    parseProxyIP(
      env?.PROXYIP ||
      CFG.proxyIP
    );

  let proxyIndex = 0;

  const edStr =
    req.headers.get(
      'sec-websocket-protocol'
    );

  let ed = null;

  if (
    edStr &&
    edStr.length <=
      CFG.maxED * 4 / 3 + 4
  ) {

    try {
      ed =
        Uint8Array.fromBase64(
          edStr,
          {
            alphabet:
              'base64url'
          }
        );
    } catch {}
  }

  let curW = null;
  let sock = null;
  let closed = false;
  let busy = false;

  const uq =
    mkQ(
      CFG.upPack
    );

  const wither = () => {

    if (closed) {
      return;
    }

    closed = true;

    uq.clear();

    try {
      curW?.releaseLock();
    } catch {}

    try {
      sock?.close();
    } catch {}

    try {
      server.close();
    } catch {}
  };

  const toU8 = d => {

    if (
      d instanceof Uint8Array
    ) {
      return d;
    }

    if (
      ArrayBuffer.isView(d)
    ) {
      return new Uint8Array(
        d.buffer,
        d.byteOffset,
        d.byteLength
      );
    }

    return new Uint8Array(d);
  };

  const sow = d => {

    const u =
      toU8(d);

    if (!u.byteLength) {
      return 1;
    }

    if (
      uq.sow(u)
    ) {
      return 1;
    }

    wither();

    return 0;
  };


  /*
   * ==================================
   * 核心：
   *
   * 先连接真正目标
   *
   * 没有下行数据
   * ↓
   * 使用 ProxyIP
   *
   * 但是：
   *
   * TLS数据保持不变
   * SNI仍然是原目标域名
   * ==================================
   */

  const connectWithProxy =
    async (
      targetHost,
      targetPort,
      firstData
    ) => {

      /*
       * 第一阶段：
       * 直接连接目标
       */

      let direct = null;

      try {

        direct =
          await raceSprout(
            fetcher,
            targetHost,
            targetPort
          );

        if (
          firstData?.byteLength
        ) {

          const writer =
            direct.writable.getWriter();

          await writer.write(
            firstData
          );

          writer.releaseLock();
        }

        return {
          socket: direct,
          proxy: false
        };

      } catch {

        try {
          direct?.close();
        } catch {}
      }


      /*
       * 第二阶段：
       * ProxyIP
       */

      if (
        !proxyIPs.length
      ) {
        throw new Error(
          'direct connect failed and PROXYIP is empty'
        );
      }

      const proxy =
        proxyIPs[
          proxyIndex++ %
          proxyIPs.length
        ];

      /*
       * 注意：
       *
       * 这里连接的是 ProxyIP
       *
       * 但是发送的 firstData
       * 还是客户端原始TLS数据。
       *
       * 因此TLS里面的SNI不会被改掉。
       */

      const psock =
        await raceSprout(
          fetcher,
          proxy,
          targetPort
        );

      if (
        firstData?.byteLength
      ) {

        const writer =
          psock.writable.getWriter();

        await writer.write(
          firstData
        );

        writer.releaseLock();
      }

      return {
        socket: psock,
        proxy: true
      };
    };


  /*
   * ==================================
   * 读取TCP下行
   * ==================================
   */

  const pipeRemote =
    async (
      remote,
      fallback
    ) => {

      const tx =
        mkDn(server);

      let incoming =
        false;

      const reader =
        remote.readable.getReader({
          mode: 'byob'
        });

      let buf =
        new ArrayBuffer(
          CFG.chunk
        );

      try {

        for (;;) {

          const {
            done,
            value
          } =
            await reader.read(
              new Uint8Array(
                buf,
                0,
                CFG.chunk
              )
            );

          if (done) {
            break;
          }

          if (
            !value?.byteLength
          ) {
            continue;
          }

          /*
           * 只要收到目标返回数据，
           * 就说明当前连接成功。
           */
          incoming = true;

          tx.send(
            value.slice()
          );

          buf =
            new ArrayBuffer(
              CFG.chunk
            );
        }

        tx.reap();

      } catch {

      } finally {

        try {
          tx.reap();
        } catch {}

        try {
          reader.releaseLock();
        } catch {}
      }


      /*
       * ==================================
       * 如果直连没有任何下行数据：
       *
       * ProxyIP重新连接
       * ==================================
       */

      if (
        !incoming &&
        fallback &&
        !closed &&
        proxyIPs.length
      ) {

        try {

          const proxy =
            proxyIPs[
              proxyIndex++ %
              proxyIPs.length
            ];

          const retry =
            await raceSprout(
              fetcher,
              proxy,
              fallback.port
            );

          if (
            !retry ||
            closed
          ) {
            try {
              retry?.close();
            } catch {}

            return;
          }

          try {
            sock?.close();
          } catch {}

          sock =
            retry;

          /*
           * 把之前已经解析出来的
           * 第一段TLS/HTTP数据重新发送。
           */
          if (
            fallback.payload?.byteLength
          ) {

            const writer =
              retry.writable.getWriter();

            await writer.write(
              fallback.payload
            );

            writer.releaseLock();
          }

          /*
           * ProxyIP连接建立后，
           * 继续把返回数据发送给客户端。
           */
          await pipeRemote(
            retry,
            null
          );

        } catch {

          wither();
        }
      }
    };


  /*
   * ==================================
   * GrainTCP核心循环
   * ==================================
   */

  const thresh =
    async () => {

      if (
        busy ||
        closed
      ) {
        return;
      }

      busy = true;

      try {

        for (;;) {

          if (closed) {
            break;
          }

          /*
           * 建立新的TCP连接
           */
          if (!sock) {

            const [d] =
              uq.bundle();

            if (!d) {
              break;
            }

            /*
             * GrainTCP解析
             */
            const r =
              relay(d);

            if (!r) {
              throw new Error(
                'invalid GrainTCP header'
              );
            }

            /*
             * GrainTCP响应头
             */
            server.send(
              new Uint8Array([
                d[0],
                0
              ])
            );

            /*
             * 目标地址
             */
            const host =
              addr(
                r.addrType,
                r.targetAddrBytes
              );

            /*
             * 目标端口
             */
            const port =
              r.port;

            /*
             * 去掉GrainTCP头
             * 剩下的是原始TCP数据
             *
             * 对HTTPS来说，
             * 这里通常就是TLS ClientHello。
             */
            const payload =
              d.subarray(
                r.dataOffset
              );


            /*
             * 先直连目标
             */
            let connection =
              null;

            try {

              connection =
                await raceSprout(
                  fetcher,
                  host,
                  port
                );

              sock =
                connection;

            } catch {

              /*
               * 直连失败，
               * 立即使用ProxyIP。
               */
              if (
                !proxyIPs.length
              ) {
                throw new Error(
                  'target connect failed'
                );
              }

              const proxy =
                proxyIPs[
                  proxyIndex++ %
                  proxyIPs.length
                ];

              sock =
                await raceSprout(
                  fetcher,
                  proxy,
                  port
                );
            }


            if (!sock) {
              throw new Error(
                'socket unavailable'
              );
            }


            /*
             * 第一包：
             *
             * 直接发送原始payload
             *
             * 不修改目标域名。
             */
            curW =
              sock.writable.getWriter();

            if (
              payload?.byteLength
            ) {

              await curW.write(
                payload
              );
            }

            curW.releaseLock();
            curW = null;


            /*
             * 开始TCP → WebSocket
             */
            pipeRemote(
              sock,
              {
                host,
                port,
                payload
              }
            ).finally(() => {

              if (!closed) {

                try {
                  sock?.close();
                } catch {}

                sock = null;
              }

            });

            continue;
          }


          /*
           * 已经建立连接：
           * 后续客户端数据直接写入TCP。
           */

          const [d] =
            uq.bundle();

          if (!d) {
            break;
          }

          curW =
            sock.writable.getWriter();

          await curW.write(d);

          curW.releaseLock();
          curW = null;
        }

      } catch {

        wither();

      } finally {

        busy = false;

        if (
          !uq.empty &&
          !closed
        ) {
          thresh();
        }
      }
    };


  /*
   * 0-RTT Early Data
   */
  if (
    ed &&
    sow(ed)
  ) {
    thresh();
  }


  /*
   * WebSocket数据
   */
  server.addEventListener(
    'message',
    e => {

      if (closed) {
        return;
      }

      if (
        sow(e.data)
      ) {
        thresh();
      }
    }
  );


  server.addEventListener(
    'close',
    () => wither()
  );

  server.addEventListener(
    'error',
    () => wither()
  );


  return new Response(
    null,
    {
      status: 101,
      webSocket: client,
      headers: {
        'Sec-WebSocket-Extensions':
          ''
      }
    }
  );
};
