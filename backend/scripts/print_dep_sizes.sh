import site
import subprocess
import sys
from pathlib import Path

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run(cmd, check=False)

def main():
    try:
        sp = site.getsitepackages()[0]
    except Exception:
        import sysconfig
        sp = sysconfig.get_paths()["purelib"]

    print("Python:", sys.version)
    print("site-packages:", sp)

    sp_path = Path(sp)
    if not sp_path.exists():
        print("site-packages path does not exist.")
        return

    # Largest folders/files inside site-packages
    run(["bash", "-lc", f"du -sh {sp}/* 2>/dev/null | sort -h | tail -n 60"])

    # pip freeze (trimmed)
    run(["bash", "-lc", "python -m pip freeze | head -n 300"])

if __name__ == "__main__":
    main()