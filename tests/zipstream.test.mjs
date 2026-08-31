/* Node test for js/zipstream.js — run with:  node tests/zipstream.test.mjs
 *
 * Writes archives with the real writer and asserts a DIFFERENT implementation
 * (Python's zipfile, invoked by the caller) can read them. This file checks the
 * structural invariants that Python's reader alone would not distinguish, and
 * drives the ZIP64 branches by lowering their thresholds — they fire for real at
 * ~4 GB, which a test cannot honestly reach, and an unexercised branch that only
 * runs in production is the failure mode this exists to prevent.
 */
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

function loadModule() {
  const src = fs.readFileSync(path.join(here, '..', 'js', 'zipstream.js'), 'utf8');
  const win = { TextEncoder };
  vm.runInContext(src, vm.createContext({ window: win, TextEncoder, console }));
  return win.HFDZip;
}

function sink() {
  const chunks = [];
  return {
    chunks,
    writer: {
      write: (c) => { chunks.push(Buffer.from(c)); return Promise.resolve(); },
      close: () => Promise.resolve()
    },
    bytes: () => Buffer.concat(chunks)
  };
}

let failures = 0;
function check(label, cond, extra) {
  if (cond) { console.log('  PASS  ' + label); }
  else { failures++; console.log('  FAIL  ' + label + (extra ? '  ' + extra : '')); }
}

const MTIME = new Date(2026, 7, 31, 12, 34, 56);

// ── 1. classic (non-ZIP64) archive ────────────────────────────────────────────
{
  const Z = loadModule();
  const s = sink();
  const z = new Z.ZipWriter(s.writer);
  await z.add('AAPL_clean_1min.parquet', new Uint8Array(Buffer.from('PAR1' + 'x'.repeat(5000) + 'PAR1')), MTIME);
  await z.add('sub dir/MSFT_clean_daily.csv', new Uint8Array(Buffer.from('date,open\n2026-01-02,1\n')), MTIME);
  await z.add('unicode_ünïcode_Ω.txt', new Uint8Array(Buffer.from('hello', 'utf8')), MTIME);
  await z.add('empty.bin', new Uint8Array(0), MTIME);
  await z.finish();
  const buf = s.bytes();
  const CLASSIC = path.join(os.tmpdir(), 'hfd-zip-classic.zip'); fs.writeFileSync(CLASSIC, buf);
  console.log('classic archive:');
  check('crc32 check vector', Z.crc32(new TextEncoder().encode('123456789')) === 0xcbf43926);
  check('no ZIP64 EOCD present', buf.indexOf(Buffer.from('504b0606', 'hex')) === -1);
  check('EOCD present', buf.readUInt32LE(buf.length - 22) === 0x06054B50);
  check('entry count in EOCD == 4', buf.readUInt16LE(buf.length - 22 + 10) === 4);
  // Bit 3 (data descriptor) must NOT be set — the whole point of buffering entries.
  check('general-purpose flags = 0x0800 (UTF-8, no data descriptor)', buf.readUInt16LE(6) === 0x0800);
  check('method = 0 (store)', buf.readUInt16LE(8) === 0);
}

// ── 2. ZIP64 archive (thresholds lowered so kilobytes trigger every branch) ────
{
  const Z = loadModule();
  Z._setZip64ThresholdsForTest(1024, 2048, 2);   // entry>=1KB, offset>=2KB, count>2
  const s = sink();
  const z = new Z.ZipWriter(s.writer);
  for (let i = 0; i < 5; i++) {
    await z.add('big_' + i + '.bin', new Uint8Array(Buffer.alloc(1500, i)), MTIME);
  }
  await z.finish();
  const buf = s.bytes();
  const Z64F = path.join(os.tmpdir(), 'hfd-zip-zip64.zip'); fs.writeFileSync(Z64F, buf);
  console.log('zip64 archive:');
  // Signatures are stored little-endian, so 0x06064B50 lands as 50 4B 06 06 and
  // the locator's 0x07064B50 as 50 4B 06 07 — NOT 50 4B 07 06.
  check('ZIP64 EOCD record present', buf.indexOf(Buffer.from('504b0606', 'hex')) !== -1);
  check('ZIP64 EOCD locator present', buf.indexOf(Buffer.from('504b0607', 'hex')) !== -1);
  check('first local header declares version 45', buf.readUInt16LE(4) === 45);
  check('first entry size slot is the 0xFFFFFFFF sentinel', buf.readUInt32LE(18) === 0xFFFFFFFF);
  check('zip64 extra field id 0x0001 in first local header', buf.readUInt16LE(30 + 'big_0.bin'.length) === 0x0001);
}

console.log(failures === 0 ? '\nALL PASS' : '\n' + failures + ' FAILED');
process.exit(failures === 0 ? 0 : 1);
