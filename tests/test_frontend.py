"""Structural checks on the frontend: the page loads its assets and modules resolve."""

from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).parent.parent / "slurm_dashboard" / "static"


def test_page_serves_and_references_existing_assets(client):
    page = client.get("/").get_data(as_text=True)
    assets = re.findall(r"/static/([\w/.-]+)", page)
    assert "js/main.js" in assets and "css/app.css" in assets
    for asset in assets:
        response = client.get(f"/static/{asset}")
        assert response.status_code == 200, asset
        # Modules import each other by file name, so they must revalidate rather than be cached.
        assert "no-cache" in response.headers["Cache-Control"]


def test_every_module_import_resolves_to_an_export():
    modules = {path.name: path.read_text() for path in (STATIC / "js").glob("*.js")}
    assert "main.js" in modules
    for name, source in modules.items():
        for names, target in re.findall(r"import\s*\{([^}]+)\}\s*from\s*'\./([\w.]+)'", source):
            assert target in modules, f"{name} imports missing module {target}"
            for imported in (
                part.strip().split(" as ")[0] for part in names.split(",") if part.strip()
            ):
                declaration = r"export\s+(?:async\s+)?(?:function|const|let|class)\s+"
                pattern = declaration + re.escape(imported) + r"(?![\w$])"
                assert re.search(
                    pattern, modules[target]
                ), f"{name}: {target} does not export {imported}"


def test_frontend_never_blocks_with_native_dialogs():
    for path in (STATIC / "js").glob("*.js"):
        assert not re.search(r"(?<![\w.])(alert|confirm|prompt)\(", path.read_text()), path.name
