from __future__ import annotations

import json
from typing import Dict, List, Optional
from urllib.parse import quote, urlencode

import requests


class WebHDFSException(RuntimeError):
    """Indica errores reportados por el servicio WebHDFS."""


class WebHDFSClient:
    """Cliente mínimo para interactuar con WebHDFS mediante HTTP."""

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

    def _build_url(self, path: str, op: str, **params: object) -> str:
        normalised = path.lstrip("/")
        encoded_path = quote(normalised, safe="/=")
        query: Dict[str, object] = {"op": op, "user.name": self.user}
        query.update({key: value for key, value in params.items() if value is not None})
        return f"{self.base_url}/webhdfs/v1/{encoded_path}?{urlencode(query)}"

    def _request(self, method: str, url: str, **kwargs: object) -> requests.Response:
        return self.session.request(method, url, timeout=self.timeout, **kwargs)

    def _parse_json(self, response: requests.Response) -> Dict[str, object]:
        try:
            return response.json()
        except json.JSONDecodeError:
            return {}

    def mkdirs(self, path: str) -> None:
        url = self._build_url(path, "MKDIRS")
        response = self._request("PUT", url)
        if response.status_code >= 400:
            payload = self._parse_json(response)
            message = payload.get("RemoteException", {}).get("message") if isinstance(payload, dict) else None
            raise WebHDFSException(message or f"WebHDFS MKDIRS failed ({response.status_code})")
        payload = self._parse_json(response)
        if not payload.get("boolean"):
            raise WebHDFSException(f"Failed to ensure directory exists at {path}")

    def write_file(self, path: str, data: str, overwrite: bool = True) -> None:
        payload = data.encode("utf-8")
        create_url = self._build_url(path, "CREATE", overwrite=str(overwrite).lower())
        response = self._request("PUT", create_url, allow_redirects=False)
        if response.status_code == 307:
            upload_url = response.headers.get("Location")
            if not upload_url:
                raise WebHDFSException("Missing redirect location for WebHDFS CREATE")
            upload_response = self._request("PUT", upload_url, data=payload)
            if upload_response.status_code >= 400:
                raise WebHDFSException(f"WebHDFS WRITE failed ({upload_response.status_code})")
            return
        if response.status_code in (200, 201):
            upload_response = self._request("PUT", create_url, data=payload)
            if upload_response.status_code >= 400:
                raise WebHDFSException(f"WebHDFS WRITE failed ({upload_response.status_code})")
            return
        payload_json = self._parse_json(response)
        message = payload_json.get("RemoteException", {}).get("message") if isinstance(payload_json, dict) else None
        raise WebHDFSException(message or f"WebHDFS CREATE failed ({response.status_code})")

    def read_file(self, path: str) -> str:
        open_url = self._build_url(path, "OPEN")
        response = self._request("GET", open_url, allow_redirects=False)
        if response.status_code == 307:
            download_url = response.headers.get("Location")
            if not download_url:
                raise WebHDFSException("Missing redirect location for WebHDFS OPEN")
            download_response = self._request("GET", download_url)
            if download_response.status_code >= 400:
                raise WebHDFSException(f"WebHDFS READ failed ({download_response.status_code})")
            return download_response.text
        if response.status_code < 400:
            return response.text
        payload = self._parse_json(response)
        message = payload.get("RemoteException", {}).get("message") if isinstance(payload, dict) else None
        raise WebHDFSException(message or f"WebHDFS OPEN failed ({response.status_code})")

    def delete(self, path: str, recursive: bool = False) -> None:
        delete_url = self._build_url(path, "DELETE", recursive=str(recursive).lower())
        response = self._request("DELETE", delete_url)
        if response.status_code >= 400:
            payload = self._parse_json(response)
            message = payload.get("RemoteException", {}).get("message") if isinstance(payload, dict) else None
            raise WebHDFSException(message or f"WebHDFS DELETE failed ({response.status_code})")

    def list_status(self, path: str) -> List[Dict[str, object]]:
        list_url = self._build_url(path, "LISTSTATUS")
        response = self._request("GET", list_url)
        if response.status_code == 404:
            return []
        if response.status_code >= 400:
            payload = self._parse_json(response)
            message = payload.get("RemoteException", {}).get("message") if isinstance(payload, dict) else None
            raise WebHDFSException(message or f"WebHDFS LISTSTATUS failed ({response.status_code})")
        payload = self._parse_json(response)
        statuses = (
            payload.get("FileStatuses", {}).get("FileStatus")
            if isinstance(payload, dict)
            else None
        )
        if isinstance(statuses, list):
            return [entry for entry in statuses if isinstance(entry, dict)]
        return []
