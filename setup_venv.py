import os
import argparse
import subprocess
import sys
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_DIR = ROOT / ".venv"
REQUIREMENTS_DIR = ROOT / "requirements"
LOCK_FILE = ROOT / "requirements-lock.txt"
REQUIRED_PYTHON = (3, 11)


def run(cmd: list[str], cwd: Path | None = None, *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    print("$", " ".join(str(part) for part in cmd))
    return subprocess.run(cmd, cwd=str(cwd or ROOT), env=env, text=True, capture_output=True)


REQUIREMENTS_FILE_NAMES = ("ui.txt", "notebook.txt", "docs.txt", "airflow.txt", "spark.txt")

# Imports the local (non-Docker) run paths depend on: Spark Connect client,
# Arrow bridge, gRPC transport, API server, and the dashboards.
LOCAL_MODE_IMPORTS = (
    ("pyspark", "pyspark"),
    ("pyarrow", "pyarrow"),
    ("grpc", "grpcio"),
    ("google.protobuf", "protobuf"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("streamlit", "streamlit"),
    ("yaml", "pyyaml"),
)


def get_venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def verify_local_mode(venv_python: Path) -> bool:
    """Fail fast when the environment cannot run the platform in local mode.

    A successful pip run is not proof that the Spark Connect, Arrow, and gRPC
    stack actually imports together, which is the failure the pinned versions in
    ``requirements/`` exist to prevent.
    """
    checks = "; ".join(f"import {module}" for module, _package in LOCAL_MODE_IMPORTS)
    script = (
        f"{checks}\n"
        "import pyspark, pyarrow, grpc\n"
        "print(f'pyspark={pyspark.__version__} pyarrow={pyarrow.__version__} grpcio={grpc.__version__}')\n"
    )
    result = run([str(venv_python), "-c", script])
    if result.returncode != 0:
        print("\nLocal-mode dependency check failed:", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        print(
            "Expected packages: " + ", ".join(package for _module, package in LOCAL_MODE_IMPORTS),
            file=sys.stderr,
        )
        return False
    print("\nLocal-mode dependency check passed: " + result.stdout.strip())
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create or reuse .venv, install requirements, and write requirements-lock.txt. "
            "Use --update-libraries to upgrade dependency versions in the environment."
        )
    )
    parser.add_argument(
        "--update-libraries",
        action="store_true",
        help="Upgrade packages when installing requirements files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if sys.version_info[:2] != REQUIRED_PYTHON:
        detected = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        print(
            f"Python 3.11 is required to match the Docker runtime images; found {detected}. "
            "Install Python 3.11 and run this script with that interpreter.",
            file=sys.stderr,
        )
        return 1

    venv_python = get_venv_python()

    if not VENV_DIR.exists():
        print(f"Creating virtual environment at {VENV_DIR}")
        # Do not upgrade bootstrap tooling implicitly; dependency versions are
        # captured after installation in the repository-level lock file.
        venv.EnvBuilder(with_pip=True).create(VENV_DIR)

    if not venv_python.exists():
        print(f"Virtual environment Python was not created at {venv_python}", file=sys.stderr)
        return 1

    requirements_files = [path for path in (REQUIREMENTS_DIR / name for name in REQUIREMENTS_FILE_NAMES) if path.exists()]
    missing = [name for name in REQUIREMENTS_FILE_NAMES if not (REQUIREMENTS_DIR / name).exists()]
    for name in missing:
        print(f"Skipping missing requirement file: {REQUIREMENTS_DIR / name}")

    # Every group is installed in a single pip invocation so pip resolves them
    # together. Installing them one after another lets a later file silently
    # downgrade a package an earlier one pinned, which is how the local venv used
    # to drift away from the Docker images.
    print("\nInstalling: " + ", ".join(str(path.relative_to(ROOT)) for path in requirements_files))
    pip_install_cmd = [str(venv_python), "-m", "pip", "install"]
    if args.update_libraries:
        pip_install_cmd.append("--upgrade")
    for req_file in requirements_files:
        pip_install_cmd.extend(["-r", str(req_file)])

    result = run(pip_install_cmd)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        print("\nDependency installation failed; no lock file was generated.", file=sys.stderr)
        return 1

    if not verify_local_mode(venv_python):
        return 1

    # ``pip freeze --all`` records direct and transitive packages for this exact
    # Python 3.11 environment. Commit the output after a reviewed dependency
    # update; it is the reproducible reference for host-side tooling.
    lock_result = run([str(venv_python), "-m", "pip", "freeze", "--all"])
    if lock_result.returncode != 0:
        print(lock_result.stderr, file=sys.stderr)
        return lock_result.returncode
    LOCK_FILE.write_text(
        "# Generated by setup_venv.py with Python 3.11. Do not edit manually.\n"
        "# Run `py -3.11 setup_venv.py --update-libraries` to upgrade packages and refresh this lock.\n"
        + lock_result.stdout,
        encoding="utf-8",
    )
    print(f"\nWrote resolved dependency lock file: {LOCK_FILE.relative_to(ROOT)}")

    print("\nVirtual environment is ready.")
    activation_path = VENV_DIR / ("Scripts" if os.name == "nt" else "bin")
    print(f"Activate it with: {activation_path / 'activate'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
