import gzip
import logging
import re
import shutil
from pathlib import Path

from marctable import to_parquet
from podbucket.resourcesync import download
from pymarc.marcxml import parse_xml_to_array


logger = logging.getLogger(__name__)


def marcxml_to_parquet(marcxml_url: str, output_dir: Path):
    filename = marcxml_url.split("/")[-1]

    marcxml_gz_path = output_dir / filename
    marcxml_path = output_dir / re.sub(r"\.xml.gz$", ".xml", filename)
    parquet_path = output_dir / re.sub(r"\.xml\.gz$", ".parquet", filename)

    logger.info(f"downloading {marcxml_url} to {marcxml_gz_path}")
    download(marcxml_url, marcxml_gz_path)
    decompress(marcxml_gz_path, marcxml_path)
    records = parse_xml_to_array(marcxml_path)
    to_parquet(records, parquet_path)

    return parquet_path


def decompress(in_path: Path, out_path: Path) -> None:
    with gzip.open(in_path, "rb") as in_fh:
        with out_path.open("wb") as out_fh:
            shutil.copyfileobj(in_fh, out_fh)
