"""Minimal Nessie REST client; isolated from Spark business logic."""
from __future__ import annotations

import requests


class NessieClient:
    """Small HTTP boundary for the catalog operations used by risk publication.

    Keeping this client deliberately narrow prevents REST details from leaking
    into the business calculation and makes branch behavior straightforward to
    unit test without a running Nessie service.
    """
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def references(self) -> list[dict]:
        response = requests.get(f"{self.base_url}/trees", timeout=10)
        response.raise_for_status()
        return response.json().get("references", [])

    def branch_exists(self, name: str) -> bool:
        return any(ref["name"] == name for ref in self.references())

    def create_branch(self, name: str, from_ref: str = "main") -> None:
        """Create ``name`` at the current hash of ``from_ref`` for isolation."""
        source = requests.get(f"{self.base_url}/trees/{from_ref}", timeout=10)
        source.raise_for_status()
        source_body = source.json()
        source_ref = source_body.get("reference", source_body)
        payload = {"type": "BRANCH", "name": name, "hash": source_ref["hash"]}
        response = requests.post(f"{self.base_url}/trees/branch", json=payload, timeout=10)
        response.raise_for_status()

    def merge(self, from_ref: str, to_ref: str = "main") -> None:
        """Publish a completed source reference into the target reference."""
        response = requests.post(f"{self.base_url}/trees/branch/{to_ref}/merge", json={"fromRefName": from_ref}, timeout=20)
        response.raise_for_status()
