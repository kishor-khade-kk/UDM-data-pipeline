import shutil
import tempfile
import zipfile
from collections.abc import Generator
from pathlib import Path


def iter_parquets_from_zip(
    zip_path: Path, tables: list[str]
) -> Generator[tuple[str, Path], None, None]:
    """Yield (table_name, parquet_path) one at a time from a zip of parquet files.

    Streams each entry to a temp file on disk using a small copy buffer —
    no full-file bytes are loaded into Python memory. The temp file is
    deleted automatically before the next table is processed.
    """
    with zipfile.ZipFile(zip_path) as zf:
        for table in tables:
            tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            tmp_path = Path(tmp.name)
            try:
                with zf.open(f"{table}.parquet") as src:
                    shutil.copyfileobj(src, tmp)  # streams in ~16 KB buffer chunks
                tmp.close()
                yield table, tmp_path
            finally:
                tmp.close()
                tmp_path.unlink(missing_ok=True)
