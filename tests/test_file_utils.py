import sys
from hashlib import sha1
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.append(str(SCRIPT_DIR.parent))

from file_utils import generate_file_sha1  # noqa: E402


def test_generate_file_sha1():
    for file_path in (SCRIPT_DIR.parent / "test_data").glob("**/*"):
        print(f"Testing {file_path}")
        if file_path.is_dir():
            continue
        sha1_hash = sha1()
        sha1_hash.update(file_path.read_bytes())
        assert generate_file_sha1(file_path, 1024) == sha1_hash.hexdigest()
