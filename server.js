const http = require('http');
const net = require('net');
const { URL } = require('url');

const APP_NAME = process.env.APP_NAME || 'node-consumer-api';
const PORT = Number(process.env.PORT || 3000);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 2500);

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

function redisPing(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const socket = new net.Socket();
    let settled = false;
    let response = '';

    function done(result) {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve({
        name: 'redis-ping',
        host,
        port,
        durationMs: Date.now() - startedAt,
        ...result,
      });
    }

    socket.setTimeout(timeoutMs);

    socket.once('connect', () => {
      socket.write('*1\\r\\n$4\\r\\nPING\\r\\n');
    });

    socket.on('data', (chunk) => {
      response += chunk.toString('utf8');
      if (response.includes('PONG')) {
        done({ ok: true, response: response.trim() });
      }
    });

    socket.once('timeout', () => {
      done({ ok: false, error: `timeout after ${timeoutMs}ms`, response: response.trim() || null });
    });

    socket.once('error', (error) => {
      done({
        ok: false,
        error: formatSocketError(error),
        response: response.trim() || null,
      });
    });

    socket.once('close', () => {
      if (!settled) {
        const normalized = response.trim();
        done({
          ok: normalized.includes('PONG'),
          response: normalized || null,
          error: normalized.includes('PONG') ? undefined : 'connection closed before PONG',
        });
      }
    });

    socket.connect(port, host);
  });
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
  };
}

const server = http.createServer(async (req, res) => {
  const path = req.url || '/';

  if (path === '/health') {
    writeJson(res, 200, {
      status: 'ok',
      app: APP_NAME,
      timestamp: new Date().toISOString(),
    });
    return;
  }

  if (path === '/env') {
    writeJson(res, 200, {
      app: APP_NAME,
      env: getPublicEnvSnapshot(),
      timestamp: new Date().toISOString(),
    });
    return;
  }

  if (path === '/dependencies') {
    const dependencyState = await evaluateDependencies();
    writeJson(res, dependencyState.overallOk ? 200 : 503, {
      app: APP_NAME,
      dependencies: dependencyState,
      hint: 'Link this app to redis-producer and postgres-producer using Server Compass Connect Apps.',
    });
    return;
  }

  writeJson(res, 200, {
    app: APP_NAME,
    endpoints: ['/health', '/env', '/dependencies'],
    hint: 'Before linking, dependency checks should fail. After linking, they should pass.',
  });
});

server.listen(PORT, () => {
  console.log(`${APP_NAME} listening on port ${PORT}`);
});
