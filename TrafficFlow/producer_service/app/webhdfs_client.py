from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlencode

import requests

LOG = logging.getLogger("producer.webhdfs")


# Representa un fallo devuelto por el API de WebHDFS.
class WebHDFSException(RuntimeError):
    pass


# Implementa operaciones básicas de WebHDFS usadas por el productor.
class WebHDFSClient:

    # Inicializa el cliente con configuración base.
    def __init__(
        self,
        base_url: str,
        user: str = "hdfs",
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.session = session or requests.Session()

    # Construye la URL completa para una operación WebHDFS.
    def _build_url(self, path: str, op: str, **params: Any) -> str:
        normalized = path.lstrip("/")
        encoded_path = quote(normalized, safe="/=")
        query: Dict[str, Any] = {"op": op, "user.name": self.user}
        query.update({key: value for key, value in params.items() if value is not None})
        return f"{self.base_url}/webhdfs/v1/{encoded_path}?{urlencode(query)}"

    # Ejecuta una petición HTTP usando la sesión almacenada.
    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        return self.session.request(method, url, timeout=self.timeout, **kwargs)

    # Valida la respuesta HTTP y convierte errores en excepciones.
    def _handle_error(self, response: requests.Response) -> Dict[str, Any]:
        if response.status_code < 400:
            try:
                return response.json()
            except json.JSONDecodeError:
                return {}
        try:
            data = response.json()
            message = data.get("RemoteException", {}).get("message")
        except json.JSONDecodeError:
            data = {}
            message = response.text or response.reason
        raise WebHDFSException(message or f"WebHDFS error {response.status_code}")

    # Crea directorios en HDFS asegurando su existencia.
    def mkdirs(self, path: str) -> None:
        url = self._build_url(path, "MKDIRS")
        response = self._request("PUT", url)
        data = self._handle_error(response)
        if not data.get("boolean", False):
            raise WebHDFSException(f"Failed to ensure directory exists at {path}")

    # Sube un archivo local a HDFS respetando la política de overwrite.
    def upload_file(self, local_path: str, hdfs_path: str, overwrite: bool = True) -> None:
        create_url = self._build_url(hdfs_path, "CREATE", overwrite=str(overwrite).lower())
        response = self._request("PUT", create_url, allow_redirects=False)
        if response.status_code == 307:
            upload_url = response.headers.get("Location")
            if not upload_url:
                raise WebHDFSException("Missing redirect target for WebHDFS upload")
            with open(local_path, "rb") as payload:
                upload_resp = self._request("PUT", upload_url, data=payload)
            self._handle_error(upload_resp)
            return
        if response.status_code in (200, 201):
            self._handle_error(response)
            return
        self._handle_error(response)

    # Elimina un recurso de HDFS devolviendo si la operación tuvo éxito.
    def delete(self, path: str, recursive: bool = True) -> bool:
        url = self._build_url(path, "DELETE", recursive=str(recursive).lower())
        response = self._request("DELETE", url)
        data = self._handle_error(response)
        return bool(data.get("boolean", False))

    # Recupera los metadatos de los elementos bajo una ruta.
    def list_status(self, path: str) -> List[Dict[str, Any]]:
        url = self._build_url(path, "LISTSTATUS")
        response = self._request("GET", url)
        data = self._handle_error(response)
        statuses: Iterable[Dict[str, Any]] = data.get("FileStatuses", {}).get("FileStatus", [])
        return list(statuses)

    # Obtiene los metadatos de un archivo individual si existe.
    def get_file_status(self, path: str) -> Optional[Dict[str, Any]]:
        url = self._build_url(path, "GETFILESTATUS")
        response = self._request("GET", url)
        try:
            data = self._handle_error(response)
        except WebHDFSException:
            return None
        return data.get("FileStatus")
