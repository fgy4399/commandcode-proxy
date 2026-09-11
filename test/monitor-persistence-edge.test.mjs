import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { createSnapshotPersistence } from '../monitor-history.mjs';

async function temp(t) {
  const directory = await mkdtemp(join(process.env.PI_SCRATCH_DIR || tmpdir(), 'monitor-edge-'));
  t.after(() => rm(directory, {recursive: true, force: true}));
  return directory;
}

test('failed snapshot stays dirty and can flush after disk recovery without new requests', async t => {
  const root = await temp(t), directory = join(root, 'history');
  await writeFile(directory, 'blocks directory creation');
  const disk = createSnapshotPersistence(directory, () => ({version: 1, requests: [], marker: 'retained'}));
  disk.schedule();
  await disk.flush();
  assert.ok(disk.error);
  await rm(directory);
  await disk.flush();
  assert.equal(disk.error, null);
  assert.equal(JSON.parse(await readFile(join(directory, 'history-v1.json'), 'utf8')).marker, 'retained');
});

test('changes arriving during a write wait before the next background snapshot', async t => {
  const directory = await temp(t);
  let writes = 0;
  const disk = createSnapshotPersistence(directory, () => {
    writes++;
    if (writes === 1) disk.schedule();
    return {version: 1, requests: [], writes};
  });
  disk.schedule();
  await disk.flush();
  assert.equal(writes, 1);
  await delay(80);
  assert.equal(writes, 1);
  await disk.flush();
  assert.equal(writes, 2);
  assert.equal(JSON.parse(await readFile(join(directory, 'history-v1.json'), 'utf8')).writes, 2);
});

test('history rejects excessive record count before restore and sorting', async t => {
  const directory = await temp(t);
  await mkdir(directory, {recursive: true});
  await writeFile(join(directory, 'history-v1.json'), JSON.stringify({version: 1, requests: Array(100001).fill(null)}));
  const disk = createSnapshotPersistence(directory, () => ({version: 1, requests: []}));
  assert.equal(await disk.load(), null);
  assert.ok(disk.error);
});
