from __future__ import annotations

import os
import socket
import sys
import threading
import webbrowser
import logging
from pathlib import Path

import uvicorn

from app.main import create_app


def _find_open_port(default_port: int = 8000) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if sock.connect_ex(("127.0.0.1", default_port)) != 0:
            return default_port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _set_working_directory() -> None:
    if getattr(sys, "frozen", False):
        os.chdir(Path(sys.executable).resolve().parent)
        return
    os.chdir(Path(__file__).resolve().parent.parent)


def main() -> None:
    _set_working_directory()
    app = create_app()
    port = _find_open_port(8000)
    url = f"http://127.0.0.1:{port}/?tab=pm"

    # In Windows no-console EXE mode, stdout/stderr can be None.
    # Uvicorn's default formatter expects a TTY-capable stream and crashes.
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")  # noqa: SIM115

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        access_log=False,
        log_config=None,
    )


if __name__ == "__main__":
    main()
