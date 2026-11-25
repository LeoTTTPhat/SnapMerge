#!/usr/bin/env python3
"""Verify and expand the compact reference experiment records."""
from __future__ import annotations

import hashlib
import stat
import zipfile
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    expected = {}
    for line in (ROOT / "MANIFEST.sha256").read_text().splitlines():
        digest, relative = line.split("  ", 1)
        expected[relative] = digest
    count = 0
    for archive in sorted((ROOT / "reference").glob("*.zip")):
        relative = archive.relative_to(ROOT).as_posix()
        if sha256(archive) != expected.get(relative):
            raise ValueError(f"archive checksum mismatch: {relative}")
        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                path = PurePosixPath(info.filename)
                if path.is_absolute() or ".." in path.parts or path.parts[0] != "results" or stat.S_ISLNK(info.external_attr >> 16):
                    raise ValueError(f"invalid archive path: {info.filename}")
                destination = ROOT.joinpath(*path.parts)
                data = bundle.read(info)
                if destination.exists():
                    if destination.read_bytes() != data:
                        raise ValueError(f"existing record differs: {info.filename}")
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(data)
                count += 1
        print(f"verified and unpacked {archive.name}")
    if not count:
        raise FileNotFoundError("no reference archives found")
    print(f"Ready: {count} reference files")


if __name__ == "__main__":
    main()
