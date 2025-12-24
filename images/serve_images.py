#!/usr/bin/env python3

from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import os


def main() -> None:
    root = Path(__file__).resolve().parent / "ufc-cartoons"
    if not root.exists():
        raise SystemExit(f"Image directory not found: {root}")

    os.chdir(root)
    server = ThreadingHTTPServer(("0.0.0.0", 8888), SimpleHTTPRequestHandler)
    print("Serving UFC images at http://localhost:8888/")
    server.serve_forever()


if __name__ == "__main__":
    main()
