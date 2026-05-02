const fs = require('fs');
const http = require('http');
const net = require('net');
const path = require('path');
const { URL } = require('url');

const APP_NAME = process.env.APP_NAME || 'node-consumer-api';
const PORT = Number(process.env.PORT || 3000);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 2500);
const STATIC_DIR = path.join(__dirname, 'public');
const DEFAULT_LIST_PATTERN = process.env.REDIS_LIST_PATTERN || 'demo:*';
const MAX_BODY_BYTES = 1024 * 1024;

function formatSocketError(error) {
  if (!error) {
    return 'unknown socket error';
  }

  const maybeError = error;
  const message = typeof maybeError.message === 'string' ? maybeError.message.trim() : '';
  const code = typeof maybeError.code === 'string' ? maybeError.code : '';

  if (message) {
    return code ? `${code}: ${message}` : message;
  }

  if (code) {
    return code;
  }

  return String(error);
}

function parseTargetFromUrl(rawUrl, fallbackProtocol, fallbackHost, fallbackPort) {
  const fallbackUrl = `${fallbackProtocol}://${fallbackHost}:${fallbackPort}`;

  try {
    const parsed = new URL(rawUrl || fallbackUrl);
    return {
      url: rawUrl || fallbackUrl,
      host: parsed.hostname,
      port: Number(parsed.port || fallbackPort),
      protocol: parsed.protocol.replace(':', ''),
    };
  } catch (_error) {
    return {
      url: fallbackUrl,
      host: fallbackHost,
      port: Number(fallbackPort),
      protocol: fallbackProtocol,
    };
  }
}

function getRedisTarget() {
  return parseTargetFromUrl(
    process.env.REDIS_URL,
    'redis',
    process.env.REDIS_HOST || 'localhost',
    process.env.REDIS_PORT || 6379
  );
}

function getPostgresTarget() {
  return parseTargetFromUrl(
    process.env.DATABASE_URL,
    'postgres',
    process.env.POSTGRES_HOST || 'localhost',
    process.env.POSTGRES_PORT || 5432
  );
}

function getRedisConnectionConfig() {
  const fallbackHost = process.env.REDIS_HOST || 'localhost';
  const fallbackPort = Number(process.env.REDIS_PORT || 6379);
  const fallbackUrl = `redis://${fallbackHost}:${fallbackPort}`;
  const rawUrl = process.env.REDIS_URL || fallbackUrl;

  try {
    const parsed = new URL(rawUrl);
    const dbSegment = (parsed.pathname || '/').replace('/', '').trim();
    const db = dbSegment === '' ? 0 : Number(dbSegment);

    return {
      url: rawUrl,
      host: parsed.hostname || fallbackHost,
      port: Number(parsed.port || fallbackPort),
      username: parsed.username ? decodeURIComponent(parsed.username) : '',
      password: parsed.password ? decodeURIComponent(parsed.password) : '',
      db: Number.isInteger(db) && db >= 0 ? db : 0,
    };
  } catch (_error) {
    return {
      url: fallbackUrl,
      host: fallbackHost,
      port: fallbackPort,
      username: '',
      password: '',
      db: 0,
    };
  }
}

function readLine(buffer, offset) {
  const lineEnd = buffer.indexOf('\r\n', offset);
  if (lineEnd === -1) {
    const error = new Error('RESP_INCOMPLETE');
    error.code = 'RESP_INCOMPLETE';
    throw error;
  }

  return {
    line: buffer.slice(offset, lineEnd).toString('utf8'),
    nextOffset: lineEnd + 2,
  };
}

function parseRespValue(buffer, offset = 0) {
  if (offset >= buffer.length) {
    const error = new Error('RESP_INCOMPLETE');
    error.code = 'RESP_INCOMPLETE';
    throw error;
  }

  const prefix = String.fromCharCode(buffer[offset]);
  let cursor = offset + 1;

  if (prefix === '+') {
    const { line, nextOffset } = readLine(buffer, cursor);
    return {
      value: { type: 'simple', value: line },
      nextOffset,
    };
  }

  if (prefix === '-') {
    const { line, nextOffset } = readLine(buffer, cursor);
    return {
      value: { type: 'error', message: line },
      nextOffset,
    };
  }

  if (prefix === ':') {
    const { line, nextOffset } = readLine(buffer, cursor);
    return {
      value: { type: 'integer', value: Number(line) },
      nextOffset,
    };
  }

  if (prefix === '$') {
    const { line, nextOffset } = readLine(buffer, cursor);
    const length = Number(line);

    if (length === -1) {
      return {
        value: { type: 'bulk', value: null },
        nextOffset,
      };
    }

    const endOffset = nextOffset + length;
    if (endOffset + 2 > buffer.length) {
      const error = new Error('RESP_INCOMPLETE');
      error.code = 'RESP_INCOMPLETE';
      throw error;
    }

    const terminator = buffer.slice(endOffset, endOffset + 2).toString('utf8');
    if (terminator !== '\r\n') {
      throw new Error('Invalid RESP bulk string terminator');
    }

    return {
      value: {
        type: 'bulk',
        value: buffer.slice(nextOffset, endOffset).toString('utf8'),
      },
      nextOffset: endOffset + 2,
    };
  }

  if (prefix === '*') {
    const { line, nextOffset } = readLine(buffer, cursor);
    const count = Number(line);

    if (count === -1) {
      return {
        value: { type: 'array', value: null },
        nextOffset,
      };
    }

    const items = [];
    cursor = nextOffset;

    for (let i = 0; i < count; i += 1) {
      const parsedItem = parseRespValue(buffer, cursor);
      items.push(parsedItem.value);
      cursor = parsedItem.nextOffset;
    }

    return {
      value: { type: 'array', value: items },
      nextOffset: cursor,
    };
  }

  throw new Error(`Unsupported RESP prefix: ${prefix}`);
}

function respToPlain(value) {
  if (!value || typeof value !== 'object') {
    return value;
  }

  if (value.type === 'simple') {
    return value.value;
  }

  if (value.type === 'integer') {
    return value.value;
  }

  if (value.type === 'bulk') {
    return value.value;
  }

  if (value.type === 'array') {
    if (!Array.isArray(value.value)) {
      return null;
    }
    return value.value.map(respToPlain);
  }

  if (value.type === 'error') {
    throw new Error(value.message || 'Unknown Redis error');
  }

  return value;
}

function encodeRedisCommand(parts) {
  const normalized = parts.map((item) => String(item));
  const lines = [`*${normalized.length}`];

  normalized.forEach((entry) => {
    lines.push(`$${Buffer.byteLength(entry)}`);
    lines.push(entry);
  });

  return `${lines.join('\r\n')}\r\n`;
}

function buildRedisCommandPlan(redisConfig, commands) {
  const plan = [];

  if (redisConfig.username || redisConfig.password) {
    if (redisConfig.username) {
      plan.push(['AUTH', redisConfig.username, redisConfig.password || '']);
    } else {
      plan.push(['AUTH', redisConfig.password || '']);
    }
  }

  if (redisConfig.db > 0) {
    plan.push(['SELECT', String(redisConfig.db)]);
  }

  commands.forEach((command) => {
    plan.push(command);
  });

  return {
    authAndSelectCount: plan.length - commands.length,
    plan,
  };
}

function executeRedisCommands(commands, timeoutMs = REQUEST_TIMEOUT_MS) {
  const redisConfig = getRedisConnectionConfig();
  const { authAndSelectCount, plan } = buildRedisCommandPlan(redisConfig, commands);

  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let responseBuffer = Buffer.alloc(0);
    let settled = false;
    const parsedResponses = [];

    function done(error, payload) {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      if (error) {
        reject(error);
      } else {
        resolve(payload);
      }
    }

    socket.setTimeout(timeoutMs);

    socket.once('connect', () => {
      const payload = plan.map(encodeRedisCommand).join('');
      socket.write(payload);
    });

    socket.once('timeout', () => {
      done(new Error(`Redis request timeout after ${timeoutMs}ms`));
    });

    socket.once('error', (error) => {
      done(new Error(`Redis socket error: ${formatSocketError(error)}`));
    });

    socket.on('data', (chunk) => {
      responseBuffer = Buffer.concat([responseBuffer, chunk]);
      let cursor = 0;

      while (parsedResponses.length < plan.length) {
        try {
          const parsed = parseRespValue(responseBuffer, cursor);
          parsedResponses.push(parsed.value);
          cursor = parsed.nextOffset;
        } catch (error) {
          if (error && error.code === 'RESP_INCOMPLETE') {
            break;
          }
          done(error instanceof Error ? error : new Error(String(error)));
          return;
        }
      }

      if (cursor > 0) {
        responseBuffer = responseBuffer.slice(cursor);
      }

      if (parsedResponses.length === plan.length) {
        try {
          const plainResponses = parsedResponses.map(respToPlain);
          const commandResults = plainResponses.slice(authAndSelectCount);
          done(null, commandResults);
        } catch (error) {
          done(error instanceof Error ? error : new Error(String(error)));
        }
      }
    });

    socket.connect(redisConfig.port, redisConfig.host);
  });
}

function tcpCheck(name, host, port, timeoutMs) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const socket = new net.Socket();
    let settled = false;

    function done(result) {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve({
        name,
        host,
        port,
        durationMs: Date.now() - startedAt,
        ...result,
      });
    }

    socket.setTimeout(timeoutMs);

    socket.once('connect', () => {
      done({ ok: true });
    });

    socket.once('timeout', () => {
      done({ ok: false, error: `timeout after ${timeoutMs}ms` });
    });

    socket.once('error', (error) => {
      done({ ok: false, error: formatSocketError(error) });
    });

    socket.connect(port, host);
  });
}

async function redisPing(host, port, timeoutMs) {
  const startedAt = Date.now();

  try {
    const [result] = await executeRedisCommands([['PING']], timeoutMs);
    return {
      name: 'redis-ping',
      host,
      port,
      durationMs: Date.now() - startedAt,
      ok: result === 'PONG',
      response: result,
    };
  } catch (error) {
    return {
      name: 'redis-ping',
      host,
      port,
      durationMs: Date.now() - startedAt,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
      response: null,
    };
  }
}

async function evaluateDependencies() {
  const redis = getRedisTarget();
  const postgres = getPostgresTarget();

  const [redisTcp, redisPingResult, postgresTcp] = await Promise.all([
    tcpCheck('redis-tcp', redis.host, redis.port, REQUEST_TIMEOUT_MS),
    redisPing(redis.host, redis.port, REQUEST_TIMEOUT_MS),
    tcpCheck('postgres-tcp', postgres.host, postgres.port, REQUEST_TIMEOUT_MS),
  ]);

  return {
    checkedAt: new Date().toISOString(),
    redis: {
      target: redis,
      tcp: redisTcp,
      ping: redisPingResult,
    },
    postgres: {
      target: postgres,
      tcp: postgresTcp,
    },
    overallOk: Boolean(redisTcp.ok && redisPingResult.ok && postgresTcp.ok),
  };
}

function writeJson(res, statusCode, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

function writeText(res, statusCode, text, contentType = 'text/plain; charset=utf-8') {
  res.writeHead(statusCode, {
    'Content-Type': contentType,
    'Cache-Control': 'no-store',
  });
  res.end(text);
}

function getPublicEnvSnapshot() {
  return {
    APP_NAME,
    PORT,
    REDIS_URL: process.env.REDIS_URL || null,
    DATABASE_URL: process.env.DATABASE_URL || null,
    REDIS_HOST: process.env.REDIS_HOST || null,
    REDIS_PORT: process.env.REDIS_PORT || null,
    POSTGRES_HOST: process.env.POSTGRES_HOST || null,
    POSTGRES_PORT: process.env.POSTGRES_PORT || null,
    REDIS_LIST_PATTERN: DEFAULT_LIST_PATTERN,
  };
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];

    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });

    req.on('end', () => {
      resolve(Buffer.concat(chunks).toString('utf8'));
    });

    req.on('error', (error) => {
      reject(error);
    });
  });
}

async function readJsonBody(req) {
  const raw = await readRequestBody(req);
  if (!raw.trim()) {
    return {};
  }
  return JSON.parse(raw);
}

function safeKey(rawKey) {
  const key = String(rawKey || '').trim();
  if (!key) {
    throw new Error('key is required');
  }
  if (key.length > 512) {
    throw new Error('key is too long (max 512 chars)');
  }
  return key;
}

function clampInt(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const rounded = Math.floor(parsed);
  return Math.max(min, Math.min(max, rounded));
}

function contentTypeForFile(filePath) {
  const ext = path.extname(filePath).toLowerCase();

  if (ext === '.html') {
    return 'text/html; charset=utf-8';
  }
  if (ext === '.js') {
    return 'application/javascript; charset=utf-8';
  }
  if (ext === '.css') {
    return 'text/css; charset=utf-8';
  }
  if (ext === '.json') {
    return 'application/json; charset=utf-8';
  }

  return 'application/octet-stream';
}

async function serveStaticFile(res, filePath) {
  const normalizedPath = path.normalize(filePath);
  if (!normalizedPath.startsWith(STATIC_DIR)) {
    writeText(res, 403, 'Forbidden');
    return;
  }

  try {
    const content = await fs.promises.readFile(normalizedPath);
    writeText(res, 200, content, contentTypeForFile(normalizedPath));
  } catch (_error) {
    writeText(res, 404, 'Not found');
  }
}

async function handleRedisApi(req, res, urlObj) {
  if (req.method === 'GET' && urlObj.pathname === '/api/redis/ping') {
    const redis = getRedisTarget();
    const pingResult = await redisPing(redis.host, redis.port, REQUEST_TIMEOUT_MS);
    writeJson(res, pingResult.ok ? 200 : 503, {
      app: APP_NAME,
      target: redis,
      ping: pingResult,
    });
    return true;
  }

  if (req.method === 'POST' && urlObj.pathname === '/api/redis/set') {
    const body = await readJsonBody(req);
    const key = safeKey(body.key);
    const value = body.value == null ? '' : String(body.value);
    const ttlSeconds = body.ttlSeconds == null ? null : clampInt(body.ttlSeconds, 0, 0, 86400 * 30);

    const command = ['SET', key, value];
    if (ttlSeconds && ttlSeconds > 0) {
      command.push('EX', String(ttlSeconds));
    }

    const [result] = await executeRedisCommands([command]);
    writeJson(res, 200, {
      app: APP_NAME,
      key,
      value,
      ttlSeconds: ttlSeconds || null,
      result,
      saved: result === 'OK',
    });
    return true;
  }

  if (req.method === 'GET' && urlObj.pathname === '/api/redis/get') {
    const key = safeKey(urlObj.searchParams.get('key'));
    const [value] = await executeRedisCommands([['GET', key]]);

    writeJson(res, 200, {
      app: APP_NAME,
      key,
      value,
      found: value != null,
    });
    return true;
  }

  if (req.method === 'POST' && urlObj.pathname === '/api/redis/delete') {
    const body = await readJsonBody(req);
    const key = safeKey(body.key);
    const [deleted] = await executeRedisCommands([['DEL', key]]);

    writeJson(res, 200, {
      app: APP_NAME,
      key,
      deletedCount: deleted,
      deleted: Number(deleted) > 0,
    });
    return true;
  }

  if (req.method === 'GET' && urlObj.pathname === '/api/redis/list') {
    const pattern = (urlObj.searchParams.get('pattern') || DEFAULT_LIST_PATTERN).trim() || DEFAULT_LIST_PATTERN;
    const limit = clampInt(urlObj.searchParams.get('limit'), 50, 1, 500);

    const [keysRaw] = await executeRedisCommands([['KEYS', pattern]]);
    const keys = Array.isArray(keysRaw) ? keysRaw.slice(0, limit) : [];

    let values = [];
    if (keys.length > 0) {
      [values] = await executeRedisCommands([['MGET', ...keys]]);
      if (!Array.isArray(values)) {
        values = [];
      }
    }

    const records = keys.map((key, index) => ({
      key,
      value: values[index] == null ? null : values[index],
    }));

    writeJson(res, 200, {
      app: APP_NAME,
      pattern,
      limit,
      count: records.length,
      records,
    });
    return true;
  }

  if (req.method === 'POST' && urlObj.pathname === '/api/redis/load-demo') {
    const body = await readJsonBody(req);
    const prefix = String(body.prefix || 'demo:record:').trim() || 'demo:record:';
    const count = clampInt(body.count, 5, 1, 100);

    const commands = [];
    for (let i = 1; i <= count; i += 1) {
      const key = `${prefix}${i}`;
      const value = JSON.stringify({
        id: i,
        title: `Demo Record ${i}`,
        updatedAt: new Date().toISOString(),
      });
      commands.push(['SET', key, value]);
    }

    const results = await executeRedisCommands(commands);
    const okCount = results.filter((item) => item === 'OK').length;

    writeJson(res, 200, {
      app: APP_NAME,
      prefix,
      requested: count,
      inserted: okCount,
      success: okCount === count,
    });
    return true;
  }

  return false;
}

const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const pathName = urlObj.pathname;

  try {
    if (req.method === 'GET' && pathName === '/health') {
      writeJson(res, 200, {
        status: 'ok',
        app: APP_NAME,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (req.method === 'GET' && pathName === '/env') {
      writeJson(res, 200, {
        app: APP_NAME,
        env: getPublicEnvSnapshot(),
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (req.method === 'GET' && pathName === '/dependencies') {
      const dependencyState = await evaluateDependencies();
      writeJson(res, dependencyState.overallOk ? 200 : 503, {
        app: APP_NAME,
        dependencies: dependencyState,
        hint: 'Link this app to redis-producer and postgres-producer using Server Compass Connect Apps.',
      });
      return;
    }

    const redisApiHandled = await handleRedisApi(req, res, urlObj);
    if (redisApiHandled) {
      return;
    }

    if (req.method === 'GET' && pathName === '/api/env') {
      writeJson(res, 200, {
        app: APP_NAME,
        env: getPublicEnvSnapshot(),
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (req.method === 'GET' && pathName === '/api/dependencies') {
      const dependencyState = await evaluateDependencies();
      writeJson(res, dependencyState.overallOk ? 200 : 503, {
        app: APP_NAME,
        dependencies: dependencyState,
      });
      return;
    }

    if (req.method === 'GET' && pathName === '/') {
      await serveStaticFile(res, path.join(STATIC_DIR, 'index.html'));
      return;
    }

    if (req.method === 'GET' && (pathName === '/app.js' || pathName === '/styles.css')) {
      await serveStaticFile(res, path.join(STATIC_DIR, pathName.replace(/^\//, '')));
      return;
    }

    writeJson(res, 404, {
      app: APP_NAME,
      error: 'Not found',
      endpoints: [
        '/health',
        '/env',
        '/dependencies',
        '/api/env',
        '/api/dependencies',
        '/api/redis/ping',
        '/api/redis/set',
        '/api/redis/get?key=demo:record:1',
        '/api/redis/delete',
        '/api/redis/list?pattern=demo:*&limit=50',
        '/api/redis/load-demo',
      ],
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    writeJson(res, 500, {
      app: APP_NAME,
      error: message,
    });
  }
});

server.listen(PORT, () => {
  console.log(`${APP_NAME} listening on port ${PORT}`);
});
