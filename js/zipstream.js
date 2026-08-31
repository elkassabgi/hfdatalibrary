/* ──────────────────────────────────────────────────────────────────────────────
 * zipstream.js — a dependency-free, streaming ZIP writer for the bundle builder.
 *
 * WHY THIS EXISTS. "Download All" used to call a.click() once per ticker. Each
 * click is a separate cross-origin navigation download, so Chrome raised its
 * "Download multiple files?" permission prompt — repeatedly, because the loop
 * kept issuing new ones. A user who answered Block once has ALL programmatic
 * downloads on hfdatalibrary.com refused from then on, site-wide and silently:
 * the site looks broken and nothing in the page can detect or undo it. Ahmed hit
 * exactly that on 2026-08-31.
 *
 * One click must therefore produce exactly ONE file. This writes the selected
 * tickers into a single .zip, streamed to disk through the File System Access
 * API where it exists (Chrome, Edge — no size ceiling, nothing buffered beyond
 * the current entry) and collected into a Blob elsewhere, where the caller caps
 * the total because a Blob lives in memory.
 *
 * FORMAT NOTES (read before editing — these choices are load-bearing):
 *  · Method 0 (store). The payloads are parquet and gzip-backed CSV; deflating
 *    already-compressed bytes costs CPU and saves ~nothing.
 *  · Each entry is buffered whole (they are ~20 MB; the largest in the corpus is
 *    ~50 MB) so the CRC-32 is known BEFORE its local header is written. That is
 *    what lets us skip data descriptors and general-purpose bit 3 entirely, and
 *    a plain 32-bit local header is the most widely readable thing we can emit —
 *    Windows Explorer's built-in zip reader included.
 *  · ZIP64 is written only where it is actually required (an entry ≥ 4 GB, a
 *    local-header offset ≥ 4 GB, or ≥ 65,535 entries). A bundle that fits in the
 *    classic structures gets the classic structures.
 * ────────────────────────────────────────────────────────────────────────────── */
(function (global) {
  'use strict';

  var U32_MAX = 0xFFFFFFFF;   // the sentinel value written into a 32-bit slot
  var U16_MAX = 0xFFFF;

  // The thresholds at which ZIP64 becomes REQUIRED. Normally identical to the
  // sentinels above, but kept separate and overridable so the ZIP64 branches can
  // be exercised by a test on kilobytes instead of on the 4 GB that would be
  // needed to reach them honestly. A full-corpus bundle is ~27 GB, so these
  // branches DO fire for real users — an untested path here is a path that first
  // runs in production (ledger R344).
  var Z64 = { entrySize: 0xFFFFFFFF, offset: 0xFFFFFFFF, count: 0xFFFF };

  // ── CRC-32 (IEEE 802.3), incremental ────────────────────────────────────────
  var CRC_TABLE = (function () {
    var t = new Uint32Array(256);
    for (var n = 0; n < 256; n++) {
      var c = n;
      for (var k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
      t[n] = c >>> 0;
    }
    return t;
  })();

  function crc32(bytes) {
    var c = 0xFFFFFFFF;
    for (var i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
    return (c ^ 0xFFFFFFFF) >>> 0;
  }

  // Same result as crc32(), computed in slices with a yield between them.
  //
  // A full-corpus bundle is tens of GB and the checksum is a byte-at-a-time loop,
  // so doing it in one pass would block the main thread for whole seconds per
  // entry — the progress bar would stop moving and the tab would look hung at
  // exactly the moment the user most wants to see it working. Yielding every
  // YIELD_BYTES keeps the frame loop alive; the cost is ~1 ms per yield, which is
  // noise next to the network time for a 20 MB file.
  var SLICE_BYTES = 1 << 20;        // 1 MiB per uninterrupted pass
  var YIELD_BYTES = 8 << 20;        // hand the thread back every 8 MiB

  async function crc32Async(bytes) {
    var c = 0xFFFFFFFF;
    var i = 0, sinceYield = 0;
    while (i < bytes.length) {
      var end = Math.min(i + SLICE_BYTES, bytes.length);
      for (; i < end; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
      sinceYield += SLICE_BYTES;
      if (sinceYield >= YIELD_BYTES && i < bytes.length) {
        sinceYield = 0;
        await new Promise(function (r) { setTimeout(r, 0); });
      }
    }
    return (c ^ 0xFFFFFFFF) >>> 0;
  }

  // ── little-endian writers ───────────────────────────────────────────────────
  function Buf(n) { this.b = new Uint8Array(n); this.v = new DataView(this.b.buffer); this.p = 0; }
  Buf.prototype.u16 = function (x) { this.v.setUint16(this.p, x, true); this.p += 2; return this; };
  Buf.prototype.u32 = function (x) { this.v.setUint32(this.p, x >>> 0, true); this.p += 4; return this; };
  // Sizes and offsets can exceed 2^32 but never 2^53, so the high word is derived
  // by division rather than by a BigInt — exact for every value we can produce.
  Buf.prototype.u64 = function (x) {
    this.v.setUint32(this.p, x >>> 0, true);
    this.v.setUint32(this.p + 4, Math.floor(x / 4294967296), true);
    this.p += 8; return this;
  };
  Buf.prototype.bytes = function (a) { this.b.set(a, this.p); this.p += a.length; return this; };

  var UTF8 = new TextEncoder();

  // MS-DOS packed date/time. Pre-1980 is unrepresentable; clamp rather than wrap.
  function dosDateTime(d) {
    var y = d.getFullYear();
    if (y < 1980) return { time: 0, date: 33 };  // 1980-01-01 00:00:00
    return {
      time: (d.getHours() << 11) | (d.getMinutes() << 5) | (Math.floor(d.getSeconds() / 2)),
      date: ((y - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate()
    };
  }

  /**
   * ZipWriter — feed it entries in order, then call finish().
   *
   * @param {WritableStreamDefaultWriter} writer  sink for the archive bytes
   */
  function ZipWriter(writer) {
    this.w = writer;
    this.offset = 0;          // bytes written so far == next local header offset
    this.entries = [];
    this.closed = false;
  }

  ZipWriter.prototype._write = async function (chunk) {
    await this.w.write(chunk);
    this.offset += chunk.length;
  };

  /**
   * Append one stored entry.
   * @param {string} name      path inside the archive (UTF-8, forward slashes)
   * @param {Uint8Array} data  the complete file contents
   * @param {Date} [mtime]
   */
  ZipWriter.prototype.add = async function (name, data, mtime) {
    if (this.closed) throw new Error('ZipWriter already finished');
    var nameBytes = UTF8.encode(name);
    var dt = dosDateTime(mtime || new Date());
    var crc = await crc32Async(data);
    var size = data.length;
    var localOffset = this.offset;
    // Bit 11 (0x0800) declares the name is UTF-8. Bit 3 is deliberately NOT set:
    // the CRC and sizes above are already known, so no data descriptor follows.
    var flags = 0x0800;
    var needsZip64Entry = size >= Z64.entrySize;

    var extra = null;
    if (needsZip64Entry) {
      extra = new Buf(20);
      extra.u16(0x0001).u16(16).u64(size).u64(size);
      extra = extra.b;
    }

    var h = new Buf(30 + nameBytes.length + (extra ? extra.length : 0));
    h.u32(0x04034B50)
     .u16(needsZip64Entry ? 45 : 20)      // version needed to extract
     .u16(flags)
     .u16(0)                              // method: store
     .u16(dt.time).u16(dt.date)
     .u32(crc)
     .u32(needsZip64Entry ? U32_MAX : size)   // compressed
     .u32(needsZip64Entry ? U32_MAX : size)   // uncompressed
     .u16(nameBytes.length)
     .u16(extra ? extra.length : 0)
     .bytes(nameBytes);
    if (extra) h.bytes(extra);

    await this._write(h.b);
    await this._write(data);

    this.entries.push({
      name: nameBytes, crc: crc, size: size,
      offset: localOffset, time: dt.time, date: dt.date, flags: flags
    });
  };

  ZipWriter.prototype.finish = async function () {
    if (this.closed) return;
    this.closed = true;
    var cdStart = this.offset;

    for (var i = 0; i < this.entries.length; i++) {
      var e = this.entries[i];
      var bigSize = e.size >= Z64.entrySize;
      var bigOffset = e.offset >= Z64.offset;
      var z64 = bigSize || bigOffset;
      var extraLen = 0, extra = null;
      if (z64) {
        // Order is fixed by the spec: uncompressed, compressed, local offset —
        // and only the fields whose 32-bit slot was set to 0xFFFFFFFF appear.
        var n = (bigSize ? 16 : 0) + (bigOffset ? 8 : 0);
        extra = new Buf(4 + n);
        extra.u16(0x0001).u16(n);
        if (bigSize) extra.u64(e.size).u64(e.size);
        if (bigOffset) extra.u64(e.offset);
        extra = extra.b;
        extraLen = extra.length;
      }
      var c = new Buf(46 + e.name.length + extraLen);
      c.u32(0x02014B50)
       .u16(z64 ? 45 : 20)                 // version made by
       .u16(z64 ? 45 : 20)                 // version needed
       .u16(e.flags)
       .u16(0)                             // method: store
       .u16(e.time).u16(e.date)
       .u32(e.crc)
       .u32(bigSize ? U32_MAX : e.size)
       .u32(bigSize ? U32_MAX : e.size)
       .u16(e.name.length)
       .u16(extraLen)
       .u16(0)                             // comment length
       .u16(0)                             // disk number start
       .u16(0)                             // internal attributes
       .u32(0)                             // external attributes
       .u32(bigOffset ? U32_MAX : e.offset)
       .bytes(e.name);
      if (extra) c.bytes(extra);
      await this._write(c.b);
    }

    var cdSize = this.offset - cdStart;
    var count = this.entries.length;
    var needZip64End = count > Z64.count || cdStart >= Z64.offset || cdSize >= Z64.offset;

    if (needZip64End) {
      var z = new Buf(56 + 20);
      z.u32(0x06064B50).u64(44)            // zip64 EOCD, size of record after this field
       .u16(45).u16(45)
       .u32(0).u32(0)
       .u64(count).u64(count)
       .u64(cdSize).u64(cdStart);
      z.u32(0x07064B50).u32(0).u64(cdStart + cdSize).u32(1);   // zip64 EOCD locator
      await this._write(z.b);
    }

    var eocd = new Buf(22);
    eocd.u32(0x06054B50).u16(0).u16(0)
        .u16(count > U16_MAX ? U16_MAX : count)
        .u16(count > U16_MAX ? U16_MAX : count)
        .u32(cdSize >= U32_MAX ? U32_MAX : cdSize)
        .u32(cdStart >= U32_MAX ? U32_MAX : cdStart)
        .u16(0);
    await this._write(eocd.b);
    await this.w.close();
  };

  /**
   * Open a sink for the archive.
   *
   * Chrome/Edge get showSaveFilePicker: the user names the file up front, bytes
   * go straight to disk, there is no size ceiling, and — the part that matters
   * for the bug this replaced — the File System Access API does not go through
   * Chrome's DownloadRequestLimiter at all, so it still works for someone who
   * previously answered Block to the "download multiple files" prompt.
   *
   * Everywhere else the archive is collected in memory and handed over as a
   * single object-URL download at the end. That is one download rather than N,
   * so it will not raise the multi-file prompt — but be clear that a blob:
   * download IS subject to the same limiter (it is a content-initiated download
   * like any other), so a persisted Block still defeats this path and only the
   * user can lift it. It is also bounded by RAM, which is why the caller checks
   * `streaming` and caps the selection when it is false.
   *
   * Returns null if the user cancels the save dialog.
   */
  async function openArchiveSink(suggestedName) {
    if (typeof global.showSaveFilePicker === 'function') {
      var handle;
      try {
        handle = await global.showSaveFilePicker({
          suggestedName: suggestedName,
          types: [{ description: 'ZIP archive', accept: { 'application/zip': ['.zip'] } }]
        });
      } catch (e) {
        return null;                                   // AbortError == user cancelled
      }
      var fileStream = await handle.createWritable();
      return {
        streaming: true,
        writer: fileStream.getWriter(),
        deliver: async function () { /* already on disk */ }
      };
    }

    var parts = [];
    return {
      streaming: false,
      writer: {
        write: function (chunk) { parts.push(chunk.slice()); return Promise.resolve(); },
        close: function () { return Promise.resolve(); }
      },
      deliver: async function () {
        var blob = new Blob(parts, { type: 'application/zip' });
        parts.length = 0;
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url; a.download = suggestedName;
        document.body.appendChild(a); a.click(); a.remove();
        setTimeout(function () { URL.revokeObjectURL(url); }, 60000);
      }
    };
  }

  global.HFDZip = {
    ZipWriter: ZipWriter,
    openArchiveSink: openArchiveSink,
    crc32: crc32,
    supportsStreaming: typeof global.showSaveFilePicker === 'function',
    // Test-only: lower the ZIP64 thresholds so js/zipstream.test.mjs can drive the
    // ZIP64 branches with kilobytes. Never called by the site.
    _setZip64ThresholdsForTest: function (entrySize, offset, count) {
      Z64 = { entrySize: entrySize, offset: offset, count: count };
    }
  };
})(window);
