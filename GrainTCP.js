// <!--GAMFC-->version base on commit 58686d5d125194d34a1137913b3a64ddcf55872f, time is 2024-11-27 09:26:01 UTC<!--GAMFC-END-->.
// @ts-ignore
import { connect } from 'cloudflare:sockets';

// ==================== VLESS 配置 ====================
let userID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
let proxyIP = '';   // 填入代理IP即可强制走代理（不填=使用VLESS地址）

if (!isValidUUID(userID)) {
	throw new Error('uuid is not valid');
}

// ==================== 辅助函数 ====================
const isValidUUID = uuid => {
	const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
	return uuidRegex.test(uuid);
};

const safeCloseWebSocket = socket => {
	try {
		if (socket.readyState === 1 || socket.readyState === 2) {
			socket.close();
		}
	} catch (error) {
		console.error('safeCloseWebSocket error', error);
	}
};

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
	byteToHex.push((i + 256).toString(16).slice(1));
}
function unsafeStringify(arr, offset = 0) {
	return (byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + "-" + byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + "-" + byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + "-" + byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + "-" + byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]).toLowerCase();
}
function stringify(arr, offset = 0) {
	const uuid = unsafeStringify(arr, offset);
	if (!isValidUUID(uuid)) {
		throw TypeError("Stringified UUID is invalid");
	}
	return uuid;
}

const base64ToArrayBuffer = base64Str => {
	if (!base64Str) return { error: null };
	try {
		base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
		const decode = atob(base64Str);
		const arryBuffer = Uint8Array.from(decode, c => c.charCodeAt(0));
		return { earlyData: arryBuffer.buffer, error: null };
	} catch (error) {
		return { error };
	}
};

// ==================== GrainTCP 核心函数 ====================
const hex = c => (c > 64 ? c + 9 : c) & 0xF;
const idB = new Uint8Array(16);
let p = 0;
for (let i = 0; i < 16; i++) {
	let c = CFG.id.charCodeAt(p++);
	if (c === 45) c = CFG.id.charCodeAt(p++);
	const h = hex(c);
	c = CFG.id.charCodeAt(p++);
	if (c === 45) c = CFG.id.charCodeAt(p++);
	idB[i] = h << 4 | hex(c);
}
const [I0, I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15] = idB;

const matchID = c => c[1] === I0 && c[2] === I1 && c[3] === I2 && c[4] === I3 && c[5] === I4 && c[6] === I5 && c[7] === I6 && c[8] === I7 && c[9] === I8 && c[10] === I9 && c[11] === I10 && c[12] === I11 && c[13] === I12 && c[14] === I13 && c[15] === I14 && c[16] === I15;

const addr = (t, b) => t === 1 ? `\( {b[0]}. \){b[1]}.\( {b[2]}. \){b[3]}` : t === 3 ? dec.decode(b) : `[${Array.from({ length: 8 }, (_, i) => ((b[i * 2] << 8) | b[i * 2 + 1]).toString(16)).join(':')}]`;

const dec = new TextDecoder();

const sprout = (f, h, p, s = f.connect({ hostname: h, port: p })) => s.opened.then(() => s);

const raceSprout = (f, h, p) => {
	if (!f?.connect) return Promise.reject(new Error('connect unavailable'));
	if (CFG.concur <= 1) return sprout(f, h, p);
	const ts = Array(CFG.concur).fill().map(() => sprout(f, h, p));
	return Promise.any(ts).then(w => {
		ts.forEach(t => t.then(s => s !== w && s.close(), () => {}));
		return w;
	});
};

const parseAddr = (b, o, t) => {
	const l = t === 3 ? b[o++] : t === 1 ? 4 : t === 4 ? 16 : null;
	if (l === null) return null;
	const n = o + l;
	return n > b.length ? null : { targetAddrBytes: b.subarray(o, n), dataOffset: n };
};

const relay = c => {
	if (c.length < 24 || !matchID(c)) return null;
	let o = 19 + c[17];
	const p = (c[o] << 8) | c[o + 1];
	let t = c[o + 2];
	if (t !== 1) t += 1;
	const a = parseAddr(c, o + 3, t);
	return a ? { addrType: t, ...a, port: p } : null;
};

const mkK = (cap, cpy = 0) => {
	let q = [], h = 0, b = 0, buf = null;
	const e = () => h >= q.length;
	const trim = () => { h > 32 && h * 2 >= q.length && (q = q.slice(h), h = 0); };
	const clear = () => { q = []; h = 0; b = 0; };
	const take = () => { if (e()) return null; const d = q[h]; q[h++] = undefined; b -= d.byteLength; trim(); return d; };
	const sow = d => { const n = d?.byteLength || 0; return !n || (q.push(d), b += n, 1); };
	const pack = d => {
		d ||= take();
		if (!d || e()) return [d, 0];
		let n = d.byteLength, j = h;
		while (j < q.length) {
			const x = q[j];
			const nn = n + x.byteLength;
			if (nn > cap) break;
			n = nn;
			j++;
		}
		if (j === h) return [d, 0];
		const out = buf ||= new Uint8Array(cap);
		out.set(d);
		for (let o = d.byteLength; h < j;) {
			const x = q[h];
			q[h++] = undefined;
			b -= x.byteLength;
			out.set(x, o);
			o += x.byteLength;
		}
		trim();
		const u = out.subarray(0, n);
		return [cpy ? u.slice() : u, 1];
	};
	return { e, get b() { return b; }, clear, take, sow, pack };
};

const mkQ = cap => {
	const k = mkK(cap);
	return { get empty() { return k.e(); }, clear: k.clear, sow: k.sow, bundle: d => k.pack(d) };
};

const mkDn = w => {
	const cap = CFG.dnPack, tail = CFG.dnTail, low = Math.max(4096, tail * 12);
	const k = mkK(cap, 1);
	let tp = 0, gen = 0, qk = 0, qr = 0;
	const reap = () => {
		tp && clearTimeout(tp);
		tp = 0;
		qr = 0;
		for (;;) {
			const [u] = k.pack();
			if (!u) break;
			w.send(u);
		}
	};
	const ripen = () => {
		if (k.e() || tp) return;
		if (k.b >= cap || cap - k.b < tail) return reap();
		tp = setTimeout(() => {
			tp = 0;
			if (k.e()) return;
			if (k.b >= cap || cap - k.b < tail) return reap();
			if (qr < CFG.dnQr && (gen !== qk || k.b < low)) {
				qr++;
				qk = gen;
				return ripen();
			}
			reap();
		}, 1);
	};
	return { send(u) {
		let o = 0, n = u?.byteLength || 0;
		if (!n) return;
		while (o < n) {
			const m = Math.min(cap - k.b, n - o);
			if (!m) { reap(); continue; }
			k.sow(o || m !== n ? u.subarray(o, o + m) : u);
			gen++;
			o += m;
			if (k.b >= cap || cap - k.b < tail) reap();
			else ripen();
		}
	}, reap };
};

const mill = async (rd, w) => {
	const r = rd.getReader({ mode: 'byob' });
	const tx = mkDn(w);
	let buf = new ArrayBuffer(CFG.chunk);
	try {
		for (;;) {
			const { done, value: v } = await r.read(new Uint8Array(buf, 0, CFG.chunk));
			if (done) break;
			if (!v?.byteLength) continue;
			if (v.byteLength >= (CFG.chunk >> 1)) tx.reap(), w.send(v), buf = new ArrayBuffer(CFG.chunk);
			else tx.send(v.slice()), buf = v.buffer;
		}
		tx.reap();
	} catch {}
	finally {
		try { tx.reap(); } catch {}
		try { r.releaseLock(); } catch {}
	}
};

// ==================== VLESS 原版处理函数 ====================
async function vlessOverWSHandler(request) {
	const webSocketPair = new WebSocketPair();
	const [client, webSocket] = Object.values(webSocketPair);
	webSocket.accept();

	let address = '';
	let portWithRandomLog = '';
	const log = (info, event) => {
		console.log(`[\( {address}: \){portWithRandomLog}] ${info}`, event || '');
	};
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

	let remoteSocketWapper = { value: null };
	let udpStreamWrite = null;
	let isDns = false;

	readableWebSocketStream.pipeTo(new WritableStream({
		async write(chunk, controller) {
			if (isDns && udpStreamWrite) return udpStreamWrite(chunk);
			if (remoteSocketWapper.value) {
				const writer = remoteSocketWapper.value.writable.getWriter();
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}

			const { hasError, message, portRemote = 443, addressRemote = '', rawDataIndex, vlessVersion = new Uint8Array([0, 0]), isUDP } = processVlessHeader(chunk, userID);
			address = addressRemote;
			portWithRandomLog = `\( {portRemote}-- \){Math.random()} ${isUDP ? 'udp ' : 'tcp '}`;
			if (hasError) {
				throw new Error(message);
				return;
			}
			if (isUDP) {
				if (portRemote === 53) isDns = true;
				else throw new Error('UDP proxy only enable for DNS which is port 53');
				return;
			}

			const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
			const rawClientData = chunk.slice(rawDataIndex);

			if (isDns) {
				const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
				udpStreamWrite = write;
				udpStreamWrite(rawClientData);
				return;
			}

			handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
		},
		close() { log(`readableWebSocketStream is close`); },
		abort(reason) { log(`readableWebSocketStream is abort`, JSON.stringify(reason)); },
	})).catch(err => log('readableWebSocketStream pipeTo error', err));

	return new Response(null, { status: 101, webSocket: client });
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
	async function connectAndWrite(address, port) {
		const tcpSocket = connect({ hostname: address, port });
		remoteSocket.value = tcpSocket;
		log(`connected to \( {address}: \){port}`);
		const writer = tcpSocket.writable.getWriter();
		await writer.write(rawClientData);
		writer.releaseLock();
		return tcpSocket;
	}

	async function retry() {
		const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);
		tcpSocket.closed.catch(() => {}).finally(() => safeCloseWebSocket(webSocket));
		remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
	}

	const tcpSocket = await connectAndWrite(addressRemote, portRemote);
	remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
	let readableStreamCancel = false;
	const stream = new ReadableStream({
		start(controller) {
			webSocketServer.addEventListener('message', event => {
				if (readableStreamCancel) return;
				controller.enqueue(event.data);
			});
			webSocketServer.addEventListener('close', () => {
				safeCloseWebSocket(webSocketServer);
				if (readableStreamCancel) return;
				controller.close();
			});
			webSocketServer.addEventListener('error', err => controller.error(err));
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) controller.error(error);
			else if (earlyData) controller.enqueue(earlyData);
		},
		pull() {},
		cancel(reason) {
			if (readableStreamCancel) return;
			log(`ReadableStream was canceled, due to ${reason}`);
			readableStreamCancel = true;
			safeCloseWebSocket(webSocketServer);
		}
	});
	return stream;
}

function processVlessHeader(vlessBuffer, userID) {
	if (vlessBuffer.byteLength < 24) return { hasError: true, message: 'invalid data' };
	const version = new Uint8Array(vlessBuffer.slice(0, 1));
	let isValidUser = false;
	let isUDP = false;
	if (stringify(new Uint8Array(vlessBuffer.slice(1, 17))) === userID) isValidUser = true;
	if (!isValidUser) return { hasError: true, message: 'invalid user' };

	const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
	const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
	if (command === 1) {} else if (command === 2) isUDP = true;
	else return { hasError: true, message: `command ${command} is not support` };

	const portIndex = 18 + optLength + 1;
	const portRemote = new DataView(vlessBuffer.slice(portIndex, portIndex + 2)).getUint16(0);

	let addressIndex = portIndex + 2;
	const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
	const addressType = addressBuffer[0];
	let addressValue = '';
	let addressValueIndex = addressIndex + 1;
	let addressLength = 0;

	switch (addressType) {
		case 1:
			addressLength = 4;
			addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
			break;
		case 2:
			addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
			addressValueIndex += 1;
			addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
			break;
		case 3:
			addressLength = 16;
			const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
			const ipv6 = [];
			for (let i = 0; i < 8; i++) ipv6.push(dataView.getUint16(i * 2).toString(16));
			addressValue = ipv6.join(':');
			break;
		default:
			return { hasError: true, message: `invild addressType is ${addressType}` };
	}
	if (!addressValue) return { hasError: true, message: `addressValue is empty` };

	return {
		hasError: false,
		addressRemote: addressValue,
		addressType,
		portRemote,
		rawDataIndex: addressValueIndex + addressLength,
		vlessVersion: version,
		isUDP,
	};
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
	let hasIncomingData = false;
	await remoteSocket.readable.pipeTo(new WritableStream({
		async write(chunk, controller) {
			hasIncomingData = true;
			if (vlessResponseHeader) {
				webSocket.send(await new Blob([vlessResponseHeader, chunk]).arrayBuffer());
				vlessResponseHeader = null;
			} else {
				webSocket.send(chunk);
			}
		},
		close() {
			log(`remoteConnection!.readable is close with hasIncomingData is ${hasIncomingData}`);
		},
		abort() {}
	})).catch(() => safeCloseWebSocket(webSocket));

	if (!hasIncomingData && retry) {
		log(`retry`);
		retry();
	}
}

async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {
	let isVlessHeaderSent = false;
	const transformStream = new TransformStream({
		transform(chunk, controller) {
			for (let index = 0; index < chunk.byteLength;) {
				const lengthBuffer = chunk.slice(index, index + 2);
				const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
				const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
				index = index + 2 + udpPakcetLength;
				controller.enqueue(udpData);
			}
		}
	});
	transformStream.readable.pipeTo(new WritableStream({
		async write(chunk) {
			const resp = await fetch('https://1.1.1.1/dns-query', {
				method: 'POST',
				headers: { 'content-type': 'application/dns-message' },
				body: chunk
			});
			const dnsQueryResult = await resp.arrayBuffer();
			const udpSize = dnsQueryResult.byteLength;
			const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
			if (webSocket.readyState === WS_READY_STATE_OPEN) {
				if (isVlessHeaderSent) webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
				else {
					webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
					isVlessHeaderSent = true;
				}
			}
		}
	})).catch(() => {});

	const writer = transformStream.writable.getWriter();
	return { write(chunk) { writer.write(chunk); } };
}

function getVLESSConfig(userID, hostName) {
	const vlessMain = `vless://\( {userID}@ \){hostName}:443?encryption=none&security=tls&sni=\( {hostName}&fp=randomized&type=ws&host= \){hostName}&path=%2F%3Fed%3D2048#${hostName}`;
	return `
################################################################
v2ray
---------------------------------------------------------------
${vlessMain}
---------------------------------------------------------------
################################################################
clash-meta
---------------------------------------------------------------
- type: vless
  name: ${hostName}
  server: ${hostName}
  port: 443
  uuid: ${userID}
  network: ws
  tls: true
  udp: false
  sni: ${hostName}
  client-fingerprint: chrome
  ws-opts:
    path: "/?ed=2048"
    headers:
      host: ${hostName}
---------------------------------------------------------------
################################################################
`;
}
