import sys
from shutil import rmtree
from subprocess import check_call

if __name__ == "__main__":
    check_call([sys.executable, "-m", "dev.get_code_snippets"])
    rmtree("dist", ignore_errors=True)
    check_call(["uv", "build"])
    check_call(["uv", "publish"])
