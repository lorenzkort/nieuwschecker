import os
import sys
from pathlib import Path
import dagster as dg
from dotenv import load_dotenv
from dagster_polars import PolarsParquetIOManager

# Get the current file's directory (where definitions.py is located)
codelocation_root = Path(__file__).resolve().parent

# Load environment variables
load_dotenv(codelocation_root / ".env")

# Add the parent directory of cw_codelocation to sys.path
# This allows Python to find the cw_codelocation module
parent_dir = codelocation_root.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Get the parent directory of DAGSTER_HOME and add /data
data_dir = codelocation_root.parent / "data"
os.environ["DATA_DIR"] = str(data_dir)

# Load Code Environment variable
code_env = os.environ.get("code_env")

# Entrypoint to Dagster
defs = dg.Definitions.merge(
    dg.Definitions(
        resources={
            "io_manager": PolarsParquetIOManager(
                base_dir= str(data_dir)
            )
        },
    ),
    dg.load_from_defs_folder(project_root=codelocation_root),
)
