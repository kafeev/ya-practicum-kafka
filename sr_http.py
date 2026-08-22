import httpx
import requests

from confluent_kafka.schema_registry._sync.schema_registry_client import _RestClient


class RequestsRestClient(_RestClient):
    def __init__(self, conf):
        super().__init__(conf)
        self.session = requests.Session()

    def send_http_request(self, base_url, url, method, headers, body=None, query=None):
        full = "/".join([base_url.rstrip("/"), url.lstrip("/")])
        resp = self.session.request(
            method,
            full,
            headers=headers,
            data=body,
            params=query,
            auth=self.auth or None,
            proxies=self.proxy,
            timeout=self.timeout,
        )
        skip = {"content-encoding", "content-length", "transfer-encoding"}
        headers = {k: v for k, v in resp.headers.items() if k.lower() not in skip}
        return httpx.Response(
            status_code=resp.status_code,
            content=resp.content,
            headers=headers,
        )


import confluent_kafka.schema_registry._sync.schema_registry_client as _src

_src._RestClient = RequestsRestClient
