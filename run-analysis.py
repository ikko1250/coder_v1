import sys

from analysis_backend.cli import main as cli_main
from analysis_backend.worker import main as worker_main


if __name__ == "__main__":
    if "--worker" in sys.argv[1:]:
        raise SystemExit(worker_main())
    raise SystemExit(cli_main())
