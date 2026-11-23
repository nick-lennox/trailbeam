'use strict';
// npx trailbeam start (bin/trailbeam.js): the docker run it builds, and the one line it prints when Docker is
// missing or not running. Docker itself is never run here.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const cli = require('../bin/trailbeam.js');
const pkg = require('../package.json');

const BIN = path.join(__dirname, '..', 'bin', 'trailbeam.js');

function tmp() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'trailbeam-cli-'));
}

function runBin(args, env) {
  return spawnSync(process.execPath, [BIN, ...args], { env, encoding: 'utf8', timeout: 30000 });
}

test('the package has a trailbeam bin, and the library does not load it', () => {
  assert.deepStrictEqual(pkg.bin, { trailbeam: 'bin/trailbeam.js' });
  assert.ok(pkg.files.includes('bin'));
  assert.match(fs.readFileSync(BIN, 'utf8'), /^#!\/usr\/bin\/env node\n/);
  const r = spawnSync(process.execPath, ['-e',
    "require('./src/index.js'); process.stdout.write(Object.keys(require.cache).join('\\n'))"],
  { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
  assert.strictEqual(r.status, 0, r.stderr);
  assert.ok(!r.stdout.includes(path.join('node', 'bin')), 'require("trailbeam") must not load the CLI');
  assert.deepStrictEqual(Object.keys(pkg.dependencies).sort(), [
    '@opentelemetry/api', '@opentelemetry/exporter-trace-otlp-proto', '@opentelemetry/resources',
    '@opentelemetry/sdk-trace-base']);
});

test('the image tag is the package version', () => {
  assert.strictEqual(cli.IMAGE, `ghcr.io/nick-lennox/trailbeam:${pkg.version}`);
});

test('missing Docker: one line saying what to install', () => {
  const empty = tmp(); // a PATH with no docker on it
  const r = runBin(['start'], { PATH: empty, HOME: empty, USERPROFILE: empty });
  assert.strictEqual(r.status, 1);
  const lines = r.stderr.trim().split('\n');
  assert.strictEqual(lines.length, 1, r.stderr);
  assert.match(lines[0], /^trailbeam: Docker is not installed .*install Docker Desktop \(https:\/\/docs\.docker\.com\/get-docker\/\)/);
  assert.strictEqual(r.stdout, '');
});

test('Docker installed but not running: one line saying to start it', { skip: process.platform === 'win32' }, () => {
  const dir = tmp();
  const docker = path.join(dir, 'docker');
  fs.writeFileSync(docker, '#!/bin/sh\necho "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. '
    + 'Is the docker daemon running?" >&2\nexit 1\n', { mode: 0o755 });
  const r = runBin(['start'], { PATH: `${dir}:/usr/bin:/bin`, HOME: dir });
  assert.strictEqual(r.status, 1);
  assert.strictEqual(r.stderr.trim().split('\n').length, 1, r.stderr);
  assert.match(r.stderr, /Docker is installed but not running: start Docker Desktop/);
});

test('dockerProblem tells the three failures apart', () => {
  const enoent = Object.assign(new Error('spawn docker ENOENT'), { code: 'ENOENT' });
  assert.match(cli.dockerProblem(() => ({ error: enoent })), /not installed/);
  assert.match(cli.dockerProblem(() => ({ status: 1, stderr: 'permission denied while trying to connect' })),
    /docker group/);
  assert.match(cli.dockerProblem(() => ({ status: 1, stderr: 'Cannot connect to the Docker daemon' })),
    /not running/);
  assert.strictEqual(cli.dockerProblem(() => ({ status: 0, stdout: '29.0.0\n' })), null);
});

test('default: the Docker volume, 127.0.0.1:4318, the matching image', () => {
  const { args } = cli.dockerArgs([], { env: {}, uid: 1000, gid: 1000 });
  const s = args.join(' ');
  assert.ok(s.startsWith('run --rm --init --pull missing -p 127.0.0.1:4318:4318 '), s);
  assert.ok(s.includes('-v trailbeam-data:/data'), s);
  assert.ok(s.includes('-e TRAILBEAM_START_SHOWN_DATA_DIR=the Docker volume trailbeam-data'), s);
  assert.ok(!args.includes('--user'), 'the volume belongs to the image user');
  assert.deepStrictEqual(args.slice(-4), [cli.IMAGE, 'start', '--data-dir', '/data']);
});

test('flags pass through; --host and --port become the published port', () => {
  const { args } = cli.dockerArgs(['--port=5000', '--host', '0.0.0.0', '--retention', '7d', '--stuck-after', '60'],
    { env: {}, uid: 1000, gid: 1000 });
  assert.deepStrictEqual(args.slice(args.indexOf('-p'), args.indexOf('-p') + 2), ['-p', '0.0.0.0:5000:5000']);
  assert.ok(args.includes('TRAILBEAM_PORT=5000') && args.includes('TRAILBEAM_START_SHOWN_HOST=0.0.0.0'));
  assert.deepStrictEqual(args.slice(-4), ['--retention', '7d', '--stuck-after', '60']);
  assert.ok(!args.includes('--port') && !args.includes('--host'));
});

test('TRAILBEAM_ variables: translated or passed by name, never by value', () => {
  const { args } = cli.dockerArgs([], {
    env: { TRAILBEAM_PORT: '4400', TRAILBEAM_RETENTION: '3d', TRAILBEAM_TOKEN: 'secret-token-0123456789', PATH: '/x' },
    uid: 1, gid: 1,
  });
  assert.ok(args.join(' ').includes('-p 127.0.0.1:4400:4400'));
  assert.ok(args.includes('TRAILBEAM_RETENTION') && args.includes('TRAILBEAM_TOKEN'));
  assert.ok(!args.join(' ').includes('secret-token') && !args.includes('PATH'));
});

test('--data-dir and --db: mounted from this machine, run as this user', () => {
  const dir = tmp();
  const { args } = cli.dockerArgs(['--data-dir', path.join(dir, 'tb'), '--db', path.join(dir, 'db', 'x.sqlite')],
    { env: {}, uid: 501, gid: 20 });
  assert.ok(fs.statSync(path.join(dir, 'tb')).isDirectory());
  assert.ok(args.join(' ').includes(`-v ${path.join(dir, 'tb')}:/data`));
  assert.ok(args.join(' ').includes(`-v ${path.join(dir, 'db')}:/trailbeam-db`));
  assert.ok(args.join(' ').includes('--user 501:20'));
  assert.ok(args.join(' ').includes(`TRAILBEAM_START_SHOWN_DATA_DIR=${path.join(dir, 'tb')}`));
  assert.deepStrictEqual(args.slice(-6), [cli.IMAGE, 'start', '--data-dir', '/data', '--db', '/trailbeam-db/x.sqlite']);
});

test('--token-file is read here and given as TRAILBEAM_TOKEN; --config is mounted read-only', () => {
  const dir = tmp();
  fs.writeFileSync(path.join(dir, 'token'), 'my-own-token-0123456789\n', { mode: 0o600 });
  fs.writeFileSync(path.join(dir, 'trailbeam.toml'), '[defaults]\n');
  const { args, env } = cli.dockerArgs(['--token-file', path.join(dir, 'token'), '--config',
    path.join(dir, 'trailbeam.toml')], { env: {}, uid: 1, gid: 1 });
  assert.strictEqual(env.TRAILBEAM_TOKEN, 'my-own-token-0123456789');
  assert.ok(args.includes('TRAILBEAM_TOKEN') && !args.join(' ').includes('my-own-token'));
  assert.ok(args.join(' ').includes(`-v ${path.join(dir, 'trailbeam.toml')}:/run/trailbeam/trailbeam.toml:ro`));
  assert.deepStrictEqual(args.slice(-2), ['--config', '/run/trailbeam/trailbeam.toml']);
});

test('usage errors stop before Docker is asked', () => {
  assert.throws(() => cli.dockerArgs(['--store-url', 'postgres://x'], { env: {} }), /SQLite only/);
  assert.throws(() => cli.dockerArgs(['--port', 'abc'], { env: {} }), /--port/);
  assert.throws(() => cli.dockerArgs(['--port'], { env: {} }), /needs a value/);
  const r = runBin(['start', '--port', '0'], { PATH: tmp() });
  assert.strictEqual(r.status, 2);
  assert.match(r.stderr, /^trailbeam start: --port: expected a port/);
});

test('no command or an unknown one prints the usage', () => {
  const r = runBin([], { PATH: tmp() });
  assert.strictEqual(r.status, 2);
  assert.match(r.stdout, /Usage: npx trailbeam start/);
  assert.strictEqual(runBin(['stop'], { PATH: tmp() }).status, 2);
  assert.strictEqual(runBin(['--version'], { PATH: tmp() }).stdout.trim(), pkg.version);
});
