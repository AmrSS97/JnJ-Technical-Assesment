import os
import sys
import shutil
import uuid
import pytest

# ----------------------------
# Config
# ----------------------------
repo_root = "/Workspace/Users/amrsaadeldin97@gmail.com/JnJ-Technical-Assessment"
tests_src = os.path.join(repo_root, "Tests")

# Prevent writes into Workspace paths
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.environ["PYTHONPYCACHEPREFIX"] = "/tmp/pycache"

# Make your repo importable (so `from PySpark.pipeline import ...` works)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
os.environ["PYTHONPATH"] = repo_root + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else "")

# ----------------------------
# Provide Databricks SparkSession as pytest fixture
# ----------------------------
class DatabricksSparkPlugin:
    @pytest.fixture(scope="session")
    def spark(self):
        return globals()["spark"]

# ----------------------------
# Copy tests to a unique temp folder (avoids /tmp/Tests permission/collision issues)
# ----------------------------
run_id = uuid.uuid4().hex[:8]
tests_dst = f"/tmp/Tests_{run_id}"
os.makedirs(tests_dst, exist_ok=False)

test_files = [
    "test_clean_manufacturing_function.py",
    "test_build_fact_table_flags.py",
]

for fname in test_files:
    shutil.copy2(os.path.join(tests_src, fname), os.path.join(tests_dst, fname))

# Run from /tmp so pytest temp output is always writable
os.chdir("/tmp")

# ----------------------------
# Run tests
# ----------------------------
args = [
    "-q",
    "-p", "no:cacheprovider",
    "--basetemp=/tmp/pytest",
    os.path.join(os.path.basename(tests_dst), test_files[0]),
    os.path.join(os.path.basename(tests_dst), test_files[1]),
]

exit_code = pytest.main(args, plugins=[DatabricksSparkPlugin()])

if exit_code != 0:
    raise AssertionError(f"❌ pytest failed with exit code {exit_code}")

print("✅ Tests passed")
