#!/usr/bin/env node
'use strict';
// npx trailbeam start: runs the Trailbeam server from its container image (ghcr.io/nick-lennox/trailbeam, the
// tag matching this package's version), so a Node.js team can try it without Python. Inside the container it
// runs the Python `trailbeam start`, so the first-run output is the same. It takes the same flags; the ones that
// name something on this machine (--host, --port, --data-dir, --db, --config, --token-file) are turned into
// Docker's port mapping, mounts and environment. Nothing here is loaded by require('trailbeam').

const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');

const VERSION = require('../package.json').version;
const IMAGE = `ghcr.io/nick-lennox/trailbeam:${VERSION}`;
const VOLUME = 'trailbeam-data';
const INSTALL = 'install Docker Desktop (https://docs.docker.com/get-docker/), or Docker Engine on Linux';

// The flags whose value is something on this machine, and the environment variable each defaults to, as in
// the Python command.
const HOST_FLAGS = {
  '--host': 'TRAILBEAM_HOST',
  '--port': 'TRAILBEAM_PORT',
  '--data-dir': 'TRAILBEAM_DATA_DIR',
  '--db': 'TRAILBEAM_DB',
  '--config': 'TRAILBEAM_CONFIG',
  '--token-file': 'TRAILBEAM_TOKEN_FILE',
};
// Not passed into the container: the ones above are translated, and the image sets its own host, port and db.
const NOT_PASSED = new Set([...Object.values(HOST_FLAGS), 'TRAILBEAM_IMAGE',
  'TRAILBEAM_START_SHOWN_HOST', 'TRAILBEAM_START_SHOWN_DATA_DIR', 'TRAILBEAM_START_SHOWN_CAT',
  'TRAILBEAM_START_SHOWN_TOKEN_FILE']);

class UsageError extends Error {}

const USAGE = `Usage: npx trailbeam start [flags]

Runs the Trailbeam server in Docker (${IMAGE}) with its data in the Docker volume ${VOLUME}:
the first run creates an access token and a receipt-hash key, then it prints the web screen's address,
the token and the environment variables for your services. Later runs reuse them. Stop with Ctrl-C.

Every flag of the Python trailbeam start works (npx trailbeam start --help lists them). On this machine:
  --host ADDR       the address the port is published on (default 127.0.0.1)
  --port N          the port (default 4318)
  --data-dir DIR    keep the data in this directory instead of the Docker volume
  --db, --config, --token-file FILE   files on this machine; they are mounted into the container
Needs Docker; no Python.`;

/** Splits argv into the flags handled on this machine and the rest, passed to the container unchanged. */
function parseArgs(argv, env = process.env) {
  const local = {};
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--') { rest.push(...argv.slice(i)); break; }
    const eq = arg.indexOf('=');
    const name = eq > 0 ? arg.slice(0, eq) : arg;
    if (name === '--store-url') {
      throw new UsageError('--store-url: npx trailbeam start runs the image with SQLite only. For Postgres, use the '
        + 'team install (docker-compose.yaml, README "Team install")');
    }
    if (!(name in HOST_FLAGS)) { rest.push(arg); continue; }
    let value;
    if (eq > 0) value = arg.slice(eq + 1);
    else if (i + 1 < argv.length) value = argv[++i];
    else throw new UsageError(`${name} needs a value`);
    local[name] = value;
  }
  for (const [flag, name] of Object.entries(HOST_FLAGS)) {
    if (local[flag] === undefined && (env[name] || '').trim()) local[flag] = env[name].trim();
  }
  if ((env.TRAILBEAM_STORE_URL || '').trim()) {
    throw new UsageError('TRAILBEAM_STORE_URL is set: npx trailbeam start runs the image with SQLite only. For '
      + 'Postgres, use the team install (docker-compose.yaml, README "Team install")');
  }
  return { local, rest };
}

function expand(p) {
  return path.resolve(p.replace(/^~(?=$|[\\/])/, os.homedir()));
}

/** The `docker run` arguments, and the environment to give the docker command, for `start` with argv. */
function dockerArgs(argv, { env = process.env, image = (env.TRAILBEAM_IMAGE || '').trim() || IMAGE,
  uid = process.getuid ? process.getuid() : undefined, gid = process.getgid ? process.getgid() : undefined } = {}) {
  const { local, rest } = parseArgs(argv, env);
  const host = local['--host'] || '127.0.0.1';
  const port = local['--port'] || '4318';
  if (!/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
    throw new UsageError(`--port: expected a port from 1 to 65535, got ${JSON.stringify(port)}`);
  }
  const childEnv = { ...env };
  const args = ['run', '--rm', '--init', '--pull', 'missing',
    '-p', `${host.includes(':') ? `[${host}]` : host}:${port}:${port}`,
    // The container listens on every address inside it; Docker publishes the port on --host only.
    '-e', 'TRAILBEAM_HOST=0.0.0.0', '-e', `TRAILBEAM_PORT=${port}`,
    '-e', `TRAILBEAM_START_SHOWN_HOST=${host}`];
  // A directory or file of this machine: the container runs as this user, so the files it creates are yours
  // (the image's own user can't write a directory you own). The data then has to be on this machine too:
  // the Docker volume belongs to the image's user. As in the Python command, that's ~/.trailbeam by default.
  const bind = local['--data-dir'] !== undefined || local['--db'] !== undefined;
  let shown;
  if (bind) {
    const data = expand(local['--data-dir'] || '~/.trailbeam');
    fs.mkdirSync(data, { recursive: true, mode: 0o700 });
    args.push('-v', `${data}:/data`);
    shown = data;
    if (uid !== undefined) args.push('--user', `${uid}:${gid}`);
  } else {
    args.push('-v', `${VOLUME}:/data`);
    shown = `the Docker volume ${VOLUME}`;
    // After the first run the token and key are not printed, only a command that shows them: here they are in
    // the volume, so that's a container that prints the file ({} is its name).
    args.push('-e', `TRAILBEAM_START_SHOWN_CAT=docker run --rm -v ${VOLUME}:/data --entrypoint cat ${image} /data/{}`);
  }
  args.push('-e', `TRAILBEAM_START_SHOWN_DATA_DIR=${shown}`);
  const passed = [];
  if (local['--db'] !== undefined) {
    const db = expand(local['--db']);
    fs.mkdirSync(path.dirname(db), { recursive: true });
    // The directory, not the file: SQLite writes its journal next to it.
    args.push('-v', `${path.dirname(db)}:/trailbeam-db`);
    passed.push('--db', `/trailbeam-db/${path.basename(db)}`);
  }
  if (local['--config'] !== undefined) {
    const config = expand(local['--config']);
    if (!fs.existsSync(config)) throw new UsageError(`--config: ${config} does not exist`);
    args.push('-v', `${config}:/run/trailbeam/trailbeam.toml:ro`);
    passed.push('--config', '/run/trailbeam/trailbeam.toml');
  }
  if (local['--token-file'] !== undefined) {
    // Read here and given as TRAILBEAM_TOKEN (by name: the value is in docker's environment, not on its
    // command line), so a file only you can read still works when the container runs as the image's user.
    const file = expand(local['--token-file']);
    let token;
    try { token = fs.readFileSync(file, 'utf8').trim(); } catch (e) {
      throw new UsageError(`--token-file: can't read ${file}: ${e.code || e.message}`);
    }
    if (!token) throw new UsageError(`--token-file: ${file} is empty`);
    childEnv.TRAILBEAM_TOKEN = token;
    args.push('-e', `TRAILBEAM_START_SHOWN_TOKEN_FILE=${file}`);  // the banner names the file, not the token
  }
  // The other TRAILBEAM_ variables (TRAILBEAM_RETENTION, TRAILBEAM_TOKEN, ...) work as with the Python command.
  // By name only, so their values are not on docker's command line.
  for (const name of Object.keys(childEnv).sort()) {
    if (name.startsWith('TRAILBEAM_') && !NOT_PASSED.has(name)) args.push('-e', name);
  }
  args.push(image, 'start', '--data-dir', '/data', ...passed, ...rest);
  return { args, env: childEnv, host, port: Number(port) };
}

/** null when Docker answers, else the one line to print. `run` is spawnSync, replaceable in tests. */
function dockerProblem(run = spawnSync, env = process.env) {
  const r = run('docker', ['version', '--format', '{{.Server.Version}}'], { env, encoding: 'utf8', timeout: 30000 });
  if (r.error && r.error.code === 'ENOENT') {
    return `trailbeam: Docker is not installed (no docker command found): ${INSTALL}, then run npx trailbeam start again.`;
  }
  if (r.error) return `trailbeam: can't run docker (${r.error.code || r.error.message}): ${INSTALL}.`;
  if (r.status === 0) return null;
  const err = `${r.stderr || ''}`;
  if (/permission denied/i.test(err)) {
    return 'trailbeam: Docker is installed but this user may not use it (permission denied on the Docker socket): '
      + 'add yourself to the docker group (sudo usermod -aG docker $USER, then log in again), or start Docker Desktop.';
  }
  return 'trailbeam: Docker is installed but not running: start Docker Desktop (or the Docker service: '
    + 'sudo systemctl start docker), then run npx trailbeam start again.';
}

/** Resolves to null when host:port is free here, else the one line to print. Docker would otherwise fail to
 * publish the port with its own message; 4318 is the standard OTLP/HTTP port, so an OpenTelemetry Collector,
 * Jaeger or another OTLP receiver often holds it. Other errors resolve to null: docker reports those. */
function portProblem(host, port) {
  return new Promise((resolve) => {
    const srv = net.createServer();
    srv.once('error', (e) => {
      if (e.code !== 'EADDRINUSE') { resolve(null); return; }
      const other = port < 65535 ? port + 1 : port - 1;
      resolve(`trailbeam: port ${port} on ${host} is already in use (another OTLP receiver?). Stop it or run `
        + `npx trailbeam start --port ${other} and set TRAILBEAM_OTLP_TRACES_ENDPOINT to match.`);
    });
    srv.listen({ host, port, exclusive: true }, () => srv.close(() => resolve(null)));
  });
}

function main(argv = process.argv.slice(2)) {
  const [command, ...rest] = argv;
  if (command === '--version' || command === '-V') { console.log(VERSION); return 0; }
  if (command === undefined || command === '-h' || command === '--help' || command === 'help') {
    console.log(USAGE);
    return command === undefined ? 2 : 0;
  }
  if (command !== 'start') {
    console.error(`trailbeam: unknown command ${JSON.stringify(command)}\n\n${USAGE}`);
    return 2;
  }
  let plan;
  try {
    plan = dockerArgs(rest);
  } catch (e) {
    if (!(e instanceof UsageError)) throw e;
    console.error(`trailbeam start: ${e.message}`);
    return 2;
  }
  const problem = dockerProblem();
  if (problem) { console.error(problem); return 1; }
  portProblem(plan.host, plan.port).then((taken) => {
    if (taken) { console.error(taken); process.exitCode = 2; return; }
    run(plan);
  });
  return undefined;
}

function run(plan) {
  const child = spawn('docker', plan.args, { env: plan.env, stdio: 'inherit' });
  // Ctrl-C reaches docker too (same process group), and docker passes it to the server, which stops cleanly:
  // wait for that instead of exiting first. A signal sent to this process only is passed on.
  process.on('SIGINT', () => {});
  for (const sig of ['SIGTERM', 'SIGHUP']) process.on(sig, () => child.kill(sig));
  child.on('error', (e) => { console.error(`trailbeam: can't run docker: ${e.message}`); process.exit(1); });
  child.on('exit', (code, signal) => process.exit(code === null ? (signal === 'SIGINT' ? 130 : 1) : code));
}

module.exports = { parseArgs, dockerArgs, dockerProblem, portProblem, main, IMAGE, VOLUME, UsageError };

if (require.main === module) {
  const code = main();
  if (code !== undefined) process.exitCode = code;
}
