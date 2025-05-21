#!/usr/bin/env python3
import subprocess, sys, os

BASE_DIR = os.path.dirname(__file__)
FETCH = os.path.join(BASE_DIR, "fetch_usgs.py")
LOAD = os.path.join(BASE_DIR, "load_staging.py")
TRANSFORM = os.path.join(BASE_DIR, "transform.py")


def run(script_path):
    print(f"▶️ Running {os.path.basename(script_path)}")
    res = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    print(res.stdout, res.stderr, sep="")
    if res.returncode != 0:
        print(f"❌ {os.path.basename(script_path)} failed (exit {res.returncode})")
        sys.exit(res.returncode)


def main():
    run(FETCH)
    run(LOAD)
    run(TRANSFORM)
    print("✅ Full ELT pipeline completed successfully.")


if __name__ == "__main__":
    main()
