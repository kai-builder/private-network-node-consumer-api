const connectionStatusEl = document.getElementById('connection-status');
const resultEl = document.getElementById('result');

function pretty(value) {
  return JSON.stringify(value, null, 2);
}

function setResult(value) {
  resultEl.textContent = pretty(value);
}

async function request(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });

  let payload;
  try {
    payload = await response.json();
  } catch (_error) {
    payload = { error: 'Failed to parse JSON response' };
  }

  if (!response.ok) {
    const message = payload && payload.error ? payload.error : `Request failed with ${response.status}`;
    const error = new Error(message);
    error.payload = payload;
    throw error;
  }

  return payload;
}

async function refreshConnection() {
  try {
    const [dependencies, ping] = await Promise.all([
      request('/api/dependencies').catch((error) => error.payload || { error: error.message }),
      request('/api/redis/ping').catch((error) => error.payload || { error: error.message }),
    ]);

    connectionStatusEl.textContent = [
      'Dependencies:',
      pretty(dependencies),
      '',
      'Redis Ping:',
      pretty(ping),
    ].join('\n');
  } catch (error) {
    connectionStatusEl.textContent = `Connection refresh failed: ${error.message}`;
  }
}

async function onSetRecord(event) {
  event.preventDefault();

  const key = document.getElementById('set-key').value;
  const value = document.getElementById('set-value').value;
  const ttlRaw = document.getElementById('set-ttl').value;
  const ttlSeconds = ttlRaw ? Number(ttlRaw) : null;

  const payload = await request('/api/redis/set', {
    method: 'POST',
    body: JSON.stringify({ key, value, ttlSeconds }),
  });

  setResult(payload);
  await refreshConnection();
}

async function onGetRecord(event) {
  event.preventDefault();

  const key = document.getElementById('get-key').value;
  const payload = await request(`/api/redis/get?key=${encodeURIComponent(key)}`);
  setResult(payload);
}

async function onDeleteRecord() {
  const key = document.getElementById('get-key').value;
  const payload = await request('/api/redis/delete', {
    method: 'POST',
    body: JSON.stringify({ key }),
  });

  setResult(payload);
  await refreshConnection();
}

async function onLoadDemo(event) {
  event.preventDefault();

  const prefix = document.getElementById('demo-prefix').value;
  const count = Number(document.getElementById('demo-count').value || 5);

  const payload = await request('/api/redis/load-demo', {
    method: 'POST',
    body: JSON.stringify({ prefix, count }),
  });

  setResult(payload);
  await refreshConnection();
}

async function onListRecords(event) {
  event.preventDefault();

  const pattern = document.getElementById('list-pattern').value;
  const limit = Number(document.getElementById('list-limit').value || 50);

  const payload = await request(
    `/api/redis/list?pattern=${encodeURIComponent(pattern)}&limit=${encodeURIComponent(limit)}`
  );

  setResult(payload);
}

function bindHandlers() {
  document.getElementById('refresh-connection').addEventListener('click', refreshConnection);
  document.getElementById('set-form').addEventListener('submit', (event) => {
    onSetRecord(event).catch((error) => {
      setResult({ error: error.message, details: error.payload || null });
    });
  });
  document.getElementById('get-form').addEventListener('submit', (event) => {
    onGetRecord(event).catch((error) => {
      setResult({ error: error.message, details: error.payload || null });
    });
  });
  document.getElementById('delete-btn').addEventListener('click', () => {
    onDeleteRecord().catch((error) => {
      setResult({ error: error.message, details: error.payload || null });
    });
  });
  document.getElementById('load-demo-form').addEventListener('submit', (event) => {
    onLoadDemo(event).catch((error) => {
      setResult({ error: error.message, details: error.payload || null });
    });
  });
  document.getElementById('list-form').addEventListener('submit', (event) => {
    onListRecords(event).catch((error) => {
      setResult({ error: error.message, details: error.payload || null });
    });
  });
}

bindHandlers();
refreshConnection();
setResult({
  info: 'Use the forms above to add/update/load demo records in Redis.',
});
