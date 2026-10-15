import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "glue", ROOT / "producer", ROOT / "scripts"):
    sys.path.insert(0, str(p))


def load_module(name: str, rel_path: str):
    """Load a file as a module under a unique name (both Lambdas are called handler.py)."""
    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod
