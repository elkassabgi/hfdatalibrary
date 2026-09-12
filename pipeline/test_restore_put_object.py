"""restore() is the rollback, and R864 found it could not verify what it put back.

The tools that write served objects all go through r2_client.upload_from_buffer -> put_object,
so every served object carries a single-part ETag, which IS the MD5 of the content. restore()
used boto3's client.upload_file, which switches to MULTIPART above 8 MB; a multipart ETag has a
"-N" suffix and is not an MD5, so the read-back fell through to "size verified; ETag is
multipart, not an MD5, so not compared" for exactly the objects most worth checking - and the
next snapshot() of that object skipped its own MD5 check too, permanently.

These tests exercise the rollback path against a stub client. The 9 MB object is deliberately
over boto3's 8 MB multipart threshold: under the old write it came back unverified, and a test
that only used small objects would have passed either way.
"""
from __future__ import annotations

import hashlib
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import seam_rebase as sr               # noqa: E402

OVER_MULTIPART = 9 * 1024 * 1024       # boto3's default multipart threshold is 8 MB


class StubR2:
    """Records what restore() sends and answers HEAD from what it holds."""

    def __init__(self):
        self.objects = {}
        self.puts = []

    def put_object(self, Bucket=None, Key=None, Body=None, ContentType=None):   # noqa: N803
        data = Body.read() if hasattr(Body, "read") else Body
        self.objects[Key] = (data, ContentType)
        self.puts.append(Key)
        return {"ETag": '"' + hashlib.md5(data).hexdigest() + '"'}

    def upload_file(self, *a, **kw):
        raise AssertionError("restore() must not use upload_file: above 8 MB it uploads multipart, "
                             "and a multipart ETag cannot be compared with the manifest's MD5")

    def head_object(self, Bucket=None, Key=None):                                # noqa: N803
        if Key not in self.objects:
            raise KeyError(Key)
        data, _ct = self.objects[Key]
        return {"ContentLength": len(data), "ETag": '"' + hashlib.md5(data).hexdigest() + '"'}


def _snap(tmp_path, bodies, manifest=None):
    """Write a snapshot directory the way snapshot() does: one file per key, name = key with the
    slashes flattened, plus a size/ETag/key manifest."""
    lines = []
    for key, body in bodies.items():
        p = tmp_path / key.replace("/", "__")
        p.write_bytes(body)
        lines.append("\t".join([str(len(body)), hashlib.md5(body).hexdigest(), key]))
    (tmp_path / "_MANIFEST.txt").write_text("\n".join(manifest or lines) + "\n", encoding="utf-8")
    return str(tmp_path)


def test_restore_puts_every_object_back_byte_for_byte(tmp_path):
    bodies = {"raw/GOLD.parquet": os.urandom(1024) + b"x" * (OVER_MULTIPART - 1024),
              "csv/raw/GOLD.csv": b"datetime,Open,High,Low,Close,Volume,source\n"}
    d = _snap(tmp_path, bodies)
    c = StubR2()
    n = sr.restore(c, d)
    assert n == 2
    assert sorted(c.puts) == sorted(bodies)
    for key, body in bodies.items():
        assert c.objects[key][0] == body, key


def test_the_over_8mb_object_is_single_part_so_the_etag_is_an_md5(tmp_path):
    body = b"y" * OVER_MULTIPART
    d = _snap(tmp_path, {"raw/GOLD.parquet": body})
    c = StubR2()
    sr.restore(c, d)
    etag = c.head_object(Bucket=sr.BUCKET, Key="raw/GOLD.parquet")["ETag"].strip('"')
    assert "-" not in etag
    assert etag == hashlib.md5(body).hexdigest()


def test_content_type_is_what_the_writers_set(tmp_path):
    bodies = {"raw/T.parquet": b"PAR1", "csv/raw/T.csv": b"a,b\n", "raw/T.json": b"{}"}
    c = StubR2()
    sr.restore(c, _snap(tmp_path, bodies))
    assert c.objects["raw/T.parquet"][1] == "application/octet-stream"
    assert c.objects["csv/raw/T.csv"][1] == "text/csv"
    assert c.objects["raw/T.json"][1] == "application/json"
    assert sr._content_type("x/y.csv.gz") == "application/octet-stream"


def test_restore_refuses_before_writing_when_the_local_copy_is_the_wrong_size(tmp_path):
    body = b"z" * 100
    d = _snap(tmp_path, {"raw/T.parquet": body},
              manifest=["\t".join(["101", hashlib.md5(body).hexdigest(), "raw/T.parquet"])])
    c = StubR2()
    with pytest.raises(SystemExit):
        sr.restore(c, d)
    assert c.puts == []


def test_restore_is_loud_when_r2_does_not_read_back_as_the_snapshot(tmp_path):
    body = b"w" * 64
    d = _snap(tmp_path, {"raw/T.parquet": body})

    class Liar(StubR2):
        def head_object(self, Bucket=None, Key=None):                            # noqa: N803
            return {"ContentLength": len(body), "ETag": '"' + hashlib.md5(b"different").hexdigest() + '"'}

    with pytest.raises(SystemExit) as ex:
        sr.restore(Liar(), d)
    assert "NOT known to be the pre-resync state" in str(ex.value)
