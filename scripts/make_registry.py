#!/usr/bin/env python3
"""Produit registry.json à partir des binaires d'une release.

Usage : make_registry.py <tag> <owner/repo> <dossier des binaires>

Chaque binaire se nomme quarry-connector-<id>-<cible>[.exe]. Le registre
donne, par connecteur et par cible, l'URL de téléchargement, la taille et
l'empreinte SHA-256, que Quarry vérifie après téléchargement.
"""

import hashlib
import json
import os
import re
import sys

DESCRIPTIONS = {
    "postgres": ("PostgreSQL", "PostgreSQL 10 et plus, plan et coût via EXPLAIN."),
    "mysql": ("MySQL", "MySQL 8 et MariaDB ; le plan nécessite MySQL 8.0.18 ou plus."),
    "bigquery": ("BigQuery", "Google BigQuery en SQL standard, coût estimé par dry run."),
}

NAME = re.compile(r"^quarry-connector-(?P<id>[a-z0-9]+)-(?P<target>[a-z0-9_]+-[a-z0-9_-]+?)(?P<ext>\.exe)?$")


def main(tag: str, repo: str, folder: str) -> None:
    connectors: dict[str, dict] = {}
    for name in sorted(os.listdir(folder)):
        m = NAME.match(name)
        if not m:
            continue
        cid, target = m.group("id"), m.group("target")
        path = os.path.join(folder, name)
        with open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        engine, description = DESCRIPTIONS.get(cid, (cid, ""))
        entry = connectors.setdefault(
            cid,
            {"id": cid, "engine": engine, "description": description, "version": tag.lstrip("v"), "assets": {}},
        )
        entry["assets"][target] = {
            "url": f"https://github.com/{repo}/releases/download/{tag}/{name}",
            "sha256": digest,
            "size": os.path.getsize(path),
        }
    print(json.dumps({"protocol": 1, "tag": tag, "connectors": list(connectors.values())}, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
