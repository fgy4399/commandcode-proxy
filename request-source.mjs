import { BlockList, isIP } from 'node:net';

export function getRequestKind({ method, path } = {}) {
  return method === 'POST' && ['/v1/chat/completions', '/v1/messages', '/v1/responses'].includes(path) ? 'model' : 'other';
}

// Only bare addresses; no ports, zones, brackets, credentials, or retained header slices.
export function normalizeIp(value) {
  if (typeof value !== 'string' || value.length > 45 || value.includes('%')) return null;
  const family = isIP(value);
  if (!family) return null;
  if (family === 4) return Buffer.from(value).toString();
  const canonical = new URL(`http://[${value}]/`).hostname.slice(1, -1);
  const mapped = /^::ffff:([a-f0-9]+):([a-f0-9]+)$/.exec(canonical);
  if (mapped) {
    const bits = parseInt(mapped[1], 16) * 65536 + parseInt(mapped[2], 16);
    return [bits >>> 24, (bits >>> 16) & 255, (bits >>> 8) & 255, bits & 255].join('.');
  }
  return Buffer.from(canonical).toString();
}

// Accept an array of explicit IP/CIDR strings or the comma-separated environment value.
// BlockList implements address/subnet matching, including IPv4-mapped IPv6, in Node.
export function parseTrustedProxies(value = []) {
  const fail = () => { throw new TypeError('invalid CC_TRUSTED_PROXIES: expected explicit IP addresses or CIDRs'); };
  const entries = typeof value === 'string' ? (value.trim() ? value.split(',') : []) : value;
  if (!Array.isArray(entries)) fail();
  const list = new BlockList();
  for (const entry of entries) {
    if (typeof entry !== 'string' || entry.length > 128) fail();
    const parts = entry.trim().split('/');
    const address = parts[0];
    const canonical = normalizeIp(address);
    if (!canonical || parts.length > 2) fail();
    const family = isIP(address);
    if (parts.length === 1) {
      list.addAddress(canonical, isIP(canonical) === 4 ? 'ipv4' : 'ipv6');
    } else {
      if (!/^(?:0|[1-9]\d{0,2})$/.test(parts[1])) fail();
      const prefix = Number(parts[1]);
      if (prefix > (family === 4 ? 32 : 128)) fail();
      // Keep the original family for CIDRs so mapped IPv6 prefixes retain their meaning.
      list.addSubnet(address, prefix, family === 4 ? 'ipv4' : 'ipv6');
    }
  }
  return list;
}

export function isTrustedProxy(address, trustedProxies) {
  const ip = normalizeIp(address);
  return ip !== null && trustedProxies.check(ip, isIP(ip) === 4 ? 'ipv4' : 'ipv6');
}

// Reject duplicate physical headers as well as array-valued headers. No raw header is stored.
function singleHeader(req, name) {
  if (Array.isArray(req.rawHeaders)) {
    let count = 0;
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      if (typeof req.rawHeaders[i] === 'string' && req.rawHeaders[i].toLowerCase() === name && ++count > 1) return null;
    }
  }
  const value = req.headers?.[name];
  return value === undefined ? undefined : typeof value === 'string' ? value : null;
}

export function getRequestSource(req, trustedProxies = new BlockList()) {
  const peerIp = normalizeIp(req.socket?.remoteAddress);
  const source = { peerIp, clientIp: peerIp, ipSource: peerIp === null ? null : 'socket' };
  if (!peerIp || !isTrustedProxy(peerIp, trustedProxies)) return source;
  const forwarded = singleHeader(req, 'x-forwarded-for');
  if (forwarded !== undefined) {
    // Malformed XFF fails closed to the socket, including an otherwise valid prefix.
    if (typeof forwarded !== 'string' || forwarded.length > 4096) return source;
    const parts = forwarded.split(',');
    if (parts.length > 32) return source;
    const addresses = parts.map(part => normalizeIp(part.trim()));
    if (addresses.some(address => address === null)) return source;
    let clientIp = peerIp;
    for (let i = addresses.length - 1; i >= 0 && isTrustedProxy(clientIp, trustedProxies); i--) {
      clientIp = addresses[i];
    }
    return { peerIp, clientIp, ipSource: 'x-forwarded-for' };
  }
  // X-Real-IP is a fallback only when XFF is absent and the socket peer is trusted.
  const real = singleHeader(req, 'x-real-ip');
  const clientIp = typeof real === 'string' && real.length <= 64 ? normalizeIp(real.trim()) : null;
  return clientIp ? { peerIp, clientIp, ipSource: 'x-real-ip' } : source;
}

export function sanitizeRequestSource(value) {
  const peerIp = normalizeIp(value.peerIp);
  const clientIp = normalizeIp(value.clientIp);
  const ipSource = peerIp && clientIp && ['socket', 'x-forwarded-for', 'x-real-ip'].includes(value.ipSource)
    && (value.ipSource !== 'socket' || peerIp === clientIp) ? value.ipSource : null;
  return { peerIp, clientIp, ipSource };
}
