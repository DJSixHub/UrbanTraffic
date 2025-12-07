from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict

from .webhdfs_client import WebHDFSClient, WebHDFSException

LOG = logging.getLogger("producer.manage")


# Configura la salida de logging para la utilidad de gestión.
def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


# Construye el parser de comandos para las operaciones disponibles.
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--webhdfs-url",
        default=os.environ.get("WEBHDFS_URL", "http://namenode:9870"),
        help="Base URL for the WebHDFS endpoint",
    )
    parser.add_argument(
        "--hdfs-user",
        default=os.environ.get("HDFS_USER", "hdfs"),
        help="HDFS username used for WebHDFS calls",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list", help="List files stored under a given HDFS path")
    list_parser.add_argument(
        "path",
        nargs="?",
        default=os.environ.get("HDFS_BASE_PATH", "/data/gold/synthetic"),
        help="Directory to inspect (defaults to HDFS_BASE_PATH)",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Return raw JSON metadata instead of a human readable table",
    )

    delete_parser = sub.add_parser("delete", help="Delete a file or directory from HDFS")
    delete_parser.add_argument("path", help="HDFS path to delete")
    delete_parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Do not delete recursively (default behaviour is recursive)",
    )

    return parser


# Presenta tamaños en formato legible para humanos.
def _format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} PB"


# Lista archivos en HDFS y muestra metadatos según la opción elegida.
def _print_list(client: WebHDFSClient, path: str, as_json: bool) -> None:
    statuses = client.list_status(path)
    if as_json:
        print(json.dumps(statuses, indent=2))
        return
    if not statuses:
        print(f"(empty directory) {path}")
        return
    header = f"Listing for {path}"
    print(header)
    print("-" * len(header))
    for status in statuses:
        kind = status.get("type", "?").upper()
        size = _format_bytes(int(status.get("length", 0)))
        name = status.get("pathSuffix", "")
        modification = status.get("modificationTime")
        print(f"{kind:>4}  {size:>12}  {modification}  {name}")


# Elimina rutas en HDFS controlando borrados recursivos.
def _delete_path(client: WebHDFSClient, path: str, recursive: bool) -> int:
    try:
        success = client.delete(path, recursive=recursive)
    except WebHDFSException as exc:
        LOG.error("Deletion failed: %s", exc)
        return 1
    if success:
        LOG.info("Deleted %s (recursive=%s)", path, recursive)
        return 0
    LOG.warning("Path %s not found or not deleted", path)
    return 1


# Ejecuta la CLI despachando a los subcomandos disponibles.
def main(argv: Any = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)

    client = WebHDFSClient(base_url=args.webhdfs_url, user=args.hdfs_user)

    if args.command == "list":
        _print_list(client, args.path, args.json)
        return 0
    if args.command == "delete":
        recursive = not args.no_recursive
        return _delete_path(client, args.path, recursive)
    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
