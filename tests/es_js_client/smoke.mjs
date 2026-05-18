import assert from 'node:assert/strict';

import { Client } from '@elastic/elasticsearch';

const localUrl = requiredEnv('POWDRR_ES_JS_LOCAL_URL');
const externalUrl = requiredEnv('POWDRR_ES_JS_EXTERNAL_URL');
const index = requiredEnv('POWDRR_ES_JS_INDEX');
const alias = requiredEnv('POWDRR_ES_JS_ALIAS');
const marker = Number.parseInt(requiredEnv('POWDRR_ES_JS_MARKER'), 10);

const expectedMessages = ['Login attempt failed', 'Login successful'];
const sortSpec = [{ index_col: { order: 'desc' } }];

function requiredEnv(name) {
  const value = process.env[name];
  assert.ok(value, `missing required environment variable ${name}`);
  return value;
}

function createClient(node) {
  return new Client({
    node,
    maxRetries: 0,
    requestTimeout: 30_000,
    sniffOnStart: false,
    sniffOnConnectionFault: false,
    sniffInterval: false,
  });
}

function unwrap(result) {
  return result?.body ?? result;
}

function asBoolean(result) {
  const value = unwrap(result);
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof result === 'boolean') {
    return result;
  }
  if (typeof result?.statusCode === 'number') {
    return result.statusCode >= 200 && result.statusCode < 300;
  }
  return Boolean(value);
}

function sorted(values) {
  return [...values].sort();
}

function responseErrorDetails(error) {
  const body = error?.body ?? error?.meta?.body ?? {};
  return {
    status: error?.statusCode ?? error?.meta?.statusCode ?? body?.status,
    type: body?.error?.type,
    reason:
      body?.error?.reason ??
      body?.error?.root_cause?.[0]?.reason ??
      error?.message ??
      '',
  };
}

async function collectSupportedSuite(client, label) {
  const info = unwrap(await client.info());
  assert.equal(typeof info.version?.number, 'string', `${label}: info.version.number missing`);
  assert.ok(asBoolean(await client.ping()), `${label}: ping failed`);

  const health = unwrap(await client.cluster.health({ index }));
  assert.equal(health.status, 'green', `${label}: cluster health was not green`);

  assert.equal(asBoolean(await client.indices.exists({ index })), true, `${label}: index missing`);

  const indexInfo = unwrap(await client.indices.get({ index }));
  assert.ok(indexInfo[index], `${label}: indices.get missing index payload`);

  const mapping = unwrap(await client.indices.getMapping({ index }));
  assert.equal(
    mapping[index].mappings.properties.message.type,
    'text',
    `${label}: getMapping returned wrong message type`
  );

  const settings = unwrap(await client.indices.getSettings({ index }));
  assert.equal(
    settings[index].settings.index.number_of_shards,
    '1',
    `${label}: getSettings returned wrong shard count`
  );

  const aliasInfo = unwrap(await client.indices.getAlias({ name: alias }));
  assert.ok(aliasInfo[index]?.aliases?.[alias] !== undefined, `${label}: getAlias missing alias`);

  const resolveIndex = unwrap(await client.indices.resolveIndex({ name: alias }));
  assert.ok(
    resolveIndex.aliases.some(
      (entry) => entry.name === alias && Array.isArray(entry.indices) && entry.indices.includes(index)
    ),
    `${label}: resolveIndex missing alias binding`
  );

  const fieldCaps = unwrap(await client.fieldCaps({ fields: ['js_client_text', 'js_client_counter'] }));
  assert.equal(
    fieldCaps.fields.js_client_text.text.searchable,
    true,
    `${label}: fieldCaps text field not searchable`
  );
  assert.equal(
    fieldCaps.fields.js_client_counter.long.aggregatable,
    true,
    `${label}: fieldCaps long field not aggregatable`
  );
  if (Array.isArray(fieldCaps.indices)) {
    assert.ok(fieldCaps.indices.includes(index), `${label}: fieldCaps indices missing seeded index`);
  }

  const aliasSearch = unwrap(await client.search({
    index: alias,
    query: { term: { index_col: 2 } },
  }));
  assert.equal(aliasSearch.hits.total.value, 1, `${label}: alias search total mismatch`);
  assert.equal(
    aliasSearch.hits.hits[0]._source.message,
    'Login successful',
    `${label}: alias search returned wrong document`
  );

  const globalSearch = unwrap(await client.search({
    query: { term: { js_client_marker: marker } },
  }));
  const globalMessages = sorted(globalSearch.hits.hits.map((hit) => hit._source.message));
  assert.deepEqual(globalMessages, expectedMessages, `${label}: global search messages mismatch`);
  assert.equal(globalSearch.hits.total.value, 2, `${label}: global search total mismatch`);

  const globalCount = unwrap(await client.count({
    query: { term: { js_client_marker: marker } },
  }));
  assert.equal(globalCount.count, 2, `${label}: global count mismatch`);

  const mget = unwrap(await client.mget({
    index,
    docs: [{ _id: 'doc-2' }, { _id: 'missing' }],
  }));
  assert.equal(mget.docs[0].found, true, `${label}: mget missing expected doc`);
  assert.equal(mget.docs[0]._source.message, 'Login successful', `${label}: mget returned wrong doc`);
  assert.equal(mget.docs[1].found, false, `${label}: mget should report missing doc`);

  const msearch = unwrap(await client.msearch({
    searches: [
      { index },
      { query: { term: { js_client_marker: marker } } },
      { index },
      { query: { term: { index_col: 3 } } },
    ],
  }));
  assert.equal(msearch.responses.length, 2, `${label}: msearch response length mismatch`);
  assert.equal(msearch.responses[0].hits.total.value, 2, `${label}: msearch first total mismatch`);
  assert.equal(msearch.responses[1].hits.total.value, 1, `${label}: msearch second total mismatch`);

  const pit = unwrap(await client.openPointInTime({ index, keep_alive: '1m' }));
  assert.equal(typeof pit.id, 'string', `${label}: openPointInTime did not return an id`);

  const firstPage = unwrap(await client.search({
    index,
    query: { term: { js_client_marker: marker } },
    sort: sortSpec,
    size: 1,
  }));
  assert.equal(firstPage.hits.hits[0]._source.index_col, 2, `${label}: first sorted page mismatch`);
  assert.deepEqual(firstPage.hits.hits[0].sort, [2], `${label}: first sorted page sort tuple mismatch`);

  const secondPage = unwrap(await client.search({
    index,
    query: { term: { js_client_marker: marker } },
    sort: sortSpec,
    size: 1,
    search_after: firstPage.hits.hits[0].sort,
  }));
  assert.equal(secondPage.hits.hits[0]._source.index_col, 1, `${label}: search_after page mismatch`);
  assert.deepEqual(secondPage.hits.hits[0].sort, [1], `${label}: search_after sort tuple mismatch`);

  const closePit = unwrap(await client.closePointInTime({ id: pit.id }));
  assert.equal(closePit.succeeded, true, `${label}: closePointInTime did not succeed`);

  return {
    aliasMessage: aliasSearch.hits.hits[0]._source.message,
    globalMessages,
    globalCount: globalCount.count,
    msearchTotals: msearch.responses.map((response) => response.hits.total.value),
    secondPageIndexCol: secondPage.hits.hits[0]._source.index_col,
  };
}

async function expectUnsupported(client, operation, fn, messageSubstring) {
  try {
    await fn();
    assert.fail(`${operation}: expected unsupported operation error`);
  } catch (error) {
    const details = responseErrorDetails(error);
    assert.equal(details.status, 501, `${operation}: wrong status for unsupported response`);
    assert.equal(
      details.type,
      'unsupported_operation_exception',
      `${operation}: wrong error type for unsupported response`
    );
    assert.match(
      details.reason,
      new RegExp(messageSubstring, 'i'),
      `${operation}: wrong unsupported message`
    );
  }
}

async function main() {
  const localClient = createClient(localUrl);
  const externalClient = createClient(externalUrl);

  const localSummary = await collectSupportedSuite(localClient, 'powdrr');
  const externalSummary = await collectSupportedSuite(externalClient, 'elasticsearch');

  assert.deepEqual(localSummary, externalSummary, 'supported JS client summaries diverged');

  await expectUnsupported(
    localClient,
    'search template',
    () => localClient.transport.request({ method: 'POST', path: '/_search/template', body: {} }),
    'search template API is not supported'
  );
  await expectUnsupported(
    localClient,
    'scroll',
    () => localClient.transport.request({ method: 'POST', path: '/_search/scroll', body: { scroll_id: 'dummy', scroll: '1m' } }),
    'scroll API is not supported'
  );
  await expectUnsupported(
    localClient,
    'cat indices',
    () => localClient.transport.request({ method: 'GET', path: '/_cat/indices' }),
    'cat indices API is not supported'
  );

  console.log('Elasticsearch JS client smoke checks passed');
}

await main();
