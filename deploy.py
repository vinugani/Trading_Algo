#!/usr/bin/env python3
"""
deploy.py — Cross-platform automated deployment script for the Delta Exchange Trading Bot.

Supports: Windows, Linux, macOS
Steps:
  1. Detect OS
  2. Validate prerequisites (Python, Git, Docker / pip)
  3. Pull latest code from GitHub
  4. Install / update dependencies
  5. Validate environment (.env)
  6. Run pre-flight checks
  7. Deploy (Docker-compose OR native Python process)

Usage:
  python deploy.py                  # Default: paper mode, Docker if available
  python deploy.py --mode live      # Live trading
  python deploy.py --no-docker      # Force native Python deployment
  python deploy.py --strategy rsi_scalping
  python deploy.py --branch main    # Git branch to pull
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path


# ─────────────────────────────────────────────
# ANSI colour helpers (disabled on Windows < 10)
# ─────────────────────────────────────────────
_USE_COLOR = sys.stdout.isatty() and (platform.system() != "Windows" or os.environ.get("WT_SESSION"))

def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text

def info(msg: str)    -> None: print(_c(f"[INFO]  {msg}", "34"))
def ok(msg: str)      -> None: print(_c(f"[OK]    {msg}", "32"))
def warn(msg: str)    -> None: print(_c(f"[WARN]  {msg}", "33"))
def error(msg: str)   -> None: print(_c(f"[ERROR] {msg}", "31"))
def header(msg: str)  -> None: print(_c(f"\n{'═'*60}\n  {msg}\n{'═'*60}", "1;36"))


# ─────────────────────────────────────────────
# OS Detection
# ─────────────────────────────────────────────
def detect_os() -> str:
    """Returns 'windows', 'linux', or 'darwin'."""
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    elif system == "linux":
        return "linux"
    elif system == "darwin":
        return "darwin"
    else:
        raise RuntimeError(f"Unsupported operating system: {system}")


def os_label(os_name: str) -> str:
    return {"windows": "Windows", "linux": "Linux", "darwin": "macOS"}.get(os_name, os_name)


# ─────────────────────────────────────────────
# Shell command runner
# ─────────────────────────────────────────────
def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    capture: bool = False,
    check: bool = True,
    env: dict | None = None,
) -> subprocess.CompletedProcess:
    """Run a subprocess command with optional output capture."""
    display = " ".join(str(c) for c in cmd)
    info(f"Running: {display}")
    merged_env = {**os.environ, **(env or {})}
    result = subprocess.run(
        [str(c) for c in cmd],
        cwd=str(cwd) if cwd else None,
        capture_output=capture,
        text=True,
        check=False,
        env=merged_env,
    )
    if check and result.returncode != 0:
        stderr = result.stderr.strip() if result.stderr else ""
        error(f"Command failed (exit {result.returncode}): {display}")
        if stderr:
            error(f"Stderr: {stderr}")
        sys.exit(result.returncode)
    return result


# ─────────────────────────────────────────────
# Prerequisite checks
# ─────────────────────────────────────────────
def check_prerequisites(os_name: str, use_docker: bool) -> dict[str, str]:
    """Check and report on required tools. Returns a dict of found executables."""
    header("Step 1 — Checking Prerequisites")
    found: dict[str, str] = {}

    def _which(name: str) -> str | None:
        path = shutil.which(name)
        return path

    # Python (always required)
    py = _which("python3") or _which("python")
    if py:
        result = run([py, "--version"], capture=True, check=False)
        ver = (result.stdout.strip() or result.stderr.strip())
        ok(f"Python   : {py} ({ver})")
        found["python"] = py
    else:
        error("Python not found. Install Python 3.12+ and add it to PATH.")
        sys.exit(1)

    # Git (always required)
    git = _which("git")
    if git:
        result = run([git, "--version"], capture=True, check=False)
        ok(f"Git      : {git} ({result.stdout.strip()})")
        found["git"] = git
    else:
        error("Git not found. Install Git: https://git-scm.com/")
        sys.exit(1)

    # Docker + docker-compose (optional, preferred)
    if use_docker:
        docker = _which("docker")
        if docker:
            result = run([docker, "--version"], capture=True, check=False)
            ok(f"Docker   : {docker} ({result.stdout.strip()})")
            found["docker"] = docker
        else:
            warn("Docker not found. Falling back to native Python deployment.")
            use_docker = False

        compose = _which("docker-compose") or _which("docker")
        dc_cmd: list[str] = []
        if compose:
            # Prefer `docker compose` (v2 plugin) over legacy `docker-compose`
            test_v2 = run([compose, "compose", "version"], capture=True, check=False)
            if test_v2.returncode == 0:
                dc_cmd = [compose, "compose"]
                ok(f"Docker Compose v2 detected")
            else:
                dc_cmd = [compose + "-compose"] if compose != _which("docker-compose") else [compose]
                legacy = shutil.which("docker-compose")
                if legacy:
                    dc_cmd = [legacy]
                    ok("docker-compose (legacy) detected")
                else:
                    warn("docker-compose not found. Falling back to native Python.")
                    use_docker = False
        if dc_cmd:
            found["docker_compose"] = " ".join(dc_cmd)

    # Poetry (optional)
    poetry = _which("poetry")
    if poetry:
        result = run([poetry, "--version"], capture=True, check=False)
        ok(f"Poetry   : {poetry} ({result.stdout.strip()})")
        found["poetry"] = poetry
    else:
        warn("Poetry not found. Will use pip + requirements.txt instead.")

    # pip (fallback)
    pip = _which("pip3") or _which("pip")
    if pip:
        result = run([pip, "--version"], capture=True, check=False)
        ok(f"pip      : {pip} ({result.stdout.strip()})")
        found["pip"] = pip

    return found


# ─────────────────────────────────────────────
# Git Pull
# ─────────────────────────────────────────────
def git_pull(project_root: Path, found: dict[str, str], branch: str) -> None:
    header(f"Step 2 — Pulling Latest Code (branch: {branch})")
    git = found["git"]

    # Verify this is a git repo
    git_dir = project_root / ".git"
    if not git_dir.exists():
        error(f"No .git directory found in {project_root}. Clone the repo first.")
        sys.exit(1)

    # Show current commit before pull
    result = run([git, "rev-parse", "--short", "HEAD"], cwd=project_root, capture=True, check=False)
    old_sha = result.stdout.strip()
    info(f"Current commit: {old_sha}")

    # Fetch and pull
    run([git, "fetch", "--all", "--prune"], cwd=project_root)
    run([git, "checkout", branch], cwd=project_root)
    run([git, "pull", "origin", branch], cwd=project_root)

    result = run([git, "rev-parse", "--short", "HEAD"], cwd=project_root, capture=True, check=False)
    new_sha = result.stdout.strip()

    if old_sha == new_sha:
        ok(f"Already up-to-date at commit {new_sha}")
    else:
        ok(f"Updated: {old_sha} → {new_sha}")

    # Show last commit message
    result = run([git, "log", "-1", "--pretty=format:%s (%an, %ar)"], cwd=project_root, capture=True, check=False)
    info(f"Latest commit: {result.stdout.strip()}")


# ─────────────────────────────────────────────
# Dependency installation
# ─────────────────────────────────────────────
def install_dependencies(project_root: Path, found: dict[str, str], use_docker: bool) -> None:
    if use_docker:
        # Docker build handles installation; skip native install
        ok("Docker deployment — skipping native dependency install (handled by Dockerfile).")
        return

    header("Step 3 — Installing Dependencies")

    if "poetry" in found:
        run([found["poetry"], "install", "--no-interaction"], cwd=project_root)
        ok("Dependencies installed via Poetry.")
    elif "pip" in found:
        req = project_root / "requirements.txt"
        if not req.exists():
            error("requirements.txt not found. Cannot install dependencies.")
            sys.exit(1)
        run([found["pip"], "install", "--upgrade", "-r", str(req)], cwd=project_root)
        ok("Dependencies installed via pip.")
    else:
        error("Neither Poetry nor pip available. Cannot install dependencies.")
        sys.exit(1)


# ─────────────────────────────────────────────
# .env validation
# ─────────────────────────────────────────────
def validate_env(project_root: Path, mode: str) -> None:
    header("Step 4 — Validating Environment Configuration")

    env_file = project_root / ".env"
    env_example = project_root / ".env.example"

    if not env_file.exists():
        if env_example.exists():
            warn(".env not found. Copying .env.example → .env")
            shutil.copy(env_example, env_file)
            warn("Please fill in DELTA_API_KEY and DELTA_API_SECRET in .env before live trading!")
        else:
            error(".env and .env.example both missing. Cannot configure the bot.")
            sys.exit(1)
    else:
        ok(".env file found.")

    # Parse .env and validate required variables
    required_vars = ["DELTA_API_KEY", "DELTA_API_SECRET"]
    env_vars: dict[str, str] = {}
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env_vars[k.strip()] = v.strip()

    missing: list[str] = []
    placeholder_values = {"your_delta_api_key_here", "your_delta_api_secret_here", "", "changeme"}
    for var in required_vars:
        val = env_vars.get(var, "")
        if not val or val.lower() in placeholder_values:
            missing.append(var)

    if missing:
        if mode == "live":
            error(f"Missing or placeholder values for: {', '.join(missing)}")
            error("Live mode requires real API credentials in .env. Aborting.")
            sys.exit(1)
        else:
            warn(f"Placeholder values detected for: {', '.join(missing)}")
            warn("Paper mode will work but public API calls may fail if URL is wrong.")
    else:
        ok("API credentials: present")

    # Show non-secret config
    for key in ("DELTA_EXCHANGE_ENV", "DELTA_BASE_URL", "DELTA_MODE"):
        val = env_vars.get(key, os.environ.get(key, "<not set>"))
        info(f"  {key} = {val}")


# ─────────────────────────────────────────────
# Pre-flight checks (native only)
# ─────────────────────────────────────────────
def run_preflight(project_root: Path, found: dict[str, str], mode: str) -> None:
    if mode != "live":
        info("Pre-flight checks skipped (paper mode). Run live_preflight.py manually before switching to live.")
        return

    header("Step 5 — Running Live Pre-flight Checks")

    python = found["python"]
    src_path = str(project_root / "src")
    env = {"PYTHONPATH": src_path}
    script = project_root / "scripts" / "live_preflight.py"

    result = run([python, str(script)], cwd=project_root, check=False, env=env)
    if result.returncode != 0:
        error("Pre-flight checks FAILED. Fix the reported issues before deploying live.")
        sys.exit(1)
    ok("Pre-flight checks PASSED.")


# ─────────────────────────────────────────────
# Docker deployment
# ─────────────────────────────────────────────
def deploy_docker(project_root: Path, found: dict[str, str], mode: str, strategy: str) -> None:
    header("Step 6 — Deploying via Docker Compose")

    dc_cmd = found["docker_compose"].split()

    # Tear down existing containers gracefully
    run([*dc_cmd, "down", "--remove-orphans"], cwd=project_root, check=False)

    # Build fresh image
    run([*dc_cmd, "build", "--no-cache"], cwd=project_root)

    # Start services (detached)
    compose_env = {
        "DELTA_MODE": mode,
        "STRATEGY": strategy,
    }
    run(
        [*dc_cmd, "up", "-d"],
        cwd=project_root,
        env=compose_env,
    )

    # Brief wait to confirm containers came up
    time.sleep(3)
    result = run([*dc_cmd, "ps"], cwd=project_root, capture=True, check=False)
    print(result.stdout)

    ok("Docker deployment complete.")
    print()
    info("Useful commands:")
    info(f"  View logs  : {'docker compose logs -f' if len(dc_cmd) == 2 else 'docker-compose logs -f'}")
    info(f"  Stop bot   : {'docker compose down' if len(dc_cmd) == 2 else 'docker-compose down'}")
    info(f"  Metrics    : http://localhost:8000  (Prometheus raw)")
    info(f"  Prometheus : http://localhost:9090")


# ─────────────────────────────────────────────
# Native Python deployment (OS-aware)
# ─────────────────────────────────────────────
def deploy_native(
    project_root: Path,
    found: dict[str, str],
    os_name: str,
    mode: str,
    strategy: str,
) -> None:
    header(f"Step 6 — Native Python Deployment ({os_label(os_name)})")

    python = found["python"]
    src_path = str(project_root / "src")
    script = str(project_root / "scripts" / "run_bot.py")
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"bot_{mode}_{int(time.time())}.log"

    base_cmd = [python, script, "--mode", mode, "--strategy", strategy]
    env_extra = {"PYTHONPATH": src_path}

    if os_name == "windows":
        # ── Windows: launch detached process via START or pythonw ──────────
        import tempfile

        launcher_content = f"""@echo off
set PYTHONPATH={src_path}
{python} {script} --mode {mode} --strategy {strategy} >> "{log_file}" 2>&1
"""
        launcher = tempfile.NamedTemporaryFile(suffix=".bat", delete=False, mode="w", encoding="utf-8")
        launcher.write(launcher_content)
        launcher.close()

        ok(f"Launching bot in background on Windows...")
        subprocess.Popen(
            ["cmd.exe", "/C", "start", "DeltaTradingBot", "/MIN", launcher.name],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            close_fds=True,
        )

    elif os_name in ("linux", "darwin"):
        # ── Linux / macOS: nohup or screen ─────────────────────────────────
        nohup = shutil.which("nohup")
        screen = shutil.which("screen")

        if screen:
            # Preferred: screen session (re-attachable)
            session_name = f"delta_bot_{mode}"
            screen_cmd = [
                screen, "-dmS", session_name,
                "bash", "-c",
                f"PYTHONPATH={src_path} {python} {script} --mode {mode} --strategy {strategy} 2>&1 | tee -a {log_file}",
            ]
            run(screen_cmd, check=False)
            ok(f"Started inside screen session '{session_name}'.")
            info(f"  Attach  : screen -r {session_name}")
            info(f"  Detach  : Ctrl+A then D")

        elif nohup:
            # Fallback: nohup background
            nohup_cmd = (
                f"PYTHONPATH={src_path} nohup {python} {script} "
                f"--mode {mode} --strategy {strategy} >> {log_file} 2>&1 &"
            )
            subprocess.Popen(["bash", "-c", nohup_cmd])
            ok("Started with nohup (background).")

        else:
            # Last resort: foreground (user must Ctrl+C to stop)
            warn("nohup and screen not found. Running in foreground (Ctrl+C to stop).")
            merged = {**os.environ, **env_extra}
            subprocess.run(base_cmd, env=merged, cwd=str(project_root))
            return

    ok(f"Bot started in {mode} mode with strategy '{strategy}'.")
    ok(f"Logs: {log_file}")
    print()
    info("Prometheus metrics: http://localhost:8000")
    info(f"Stop bot (Linux/macOS): kill $(pgrep -f run_bot.py)")
    info(f"Stop bot (Windows):     taskkill /F /IM python.exe  (or close the MinTTY window)")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Automated cross-platform deploy script for Delta Exchange Trading Bot"
    )
    parser.add_argument("--mode", choices=["paper", "live"], default="paper",
                        help="Trading mode (default: paper)")
    parser.add_argument("--strategy",
                        choices=["momentum", "rsi_scalping", "ema_crossover", "portfolio"],
                        default="rsi_scalping",
                        help="Strategy to run (default: rsi_scalping)")
    parser.add_argument("--branch", default="main",
                        help="Git branch to pull (default: main)")
    parser.add_argument("--no-docker", action="store_true",
                        help="Skip Docker; use native Python deployment")
    parser.add_argument("--skip-pull", action="store_true",
                        help="Skip git pull (use current working tree)")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip live pre-flight checks (not recommended)")
    args = parser.parse_args()

    # ── Resolve project root (deploy.py lives in project root) ──────────────
    project_root = Path(__file__).resolve().parent

    print()
    header("Delta Exchange Trading Bot — Auto Deployment")

    # 0. Detect OS
    os_name = detect_os()
    ok(f"Detected OS: {os_label(os_name)} ({platform.version()})")
    info(f"Project root: {project_root}")
    info(f"Mode: {args.mode.upper()}  |  Strategy: {args.strategy}  |  Branch: {args.branch}")

    # 1. Prerequisites
    use_docker = not args.no_docker
    found = check_prerequisites(os_name, use_docker)
    use_docker = "docker_compose" in found  # update based on what was actually found

    # 2. Git pull
    if not args.skip_pull:
        git_pull(project_root, found, args.branch)
    else:
        warn("--skip-pull set: using current working tree without pulling.")

    # 3. Dependencies
    install_dependencies(project_root, found, use_docker)

    # 4. .env validation
    validate_env(project_root, args.mode)

    # 5. Pre-flight (live mode only)
    if not args.skip_preflight:
        run_preflight(project_root, found, args.mode)
    else:
        warn("--skip-preflight set: pre-flight checks skipped.")

    # 6. Deploy
    if use_docker:
        deploy_docker(project_root, found, args.mode, args.strategy)
    else:
        deploy_native(project_root, found, os_name, args.mode, args.strategy)

    print()
    ok("╔══════════════════════════════════════╗")
    ok("║   Deployment completed successfully  ║")
    ok("╚══════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()
