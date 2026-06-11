import json
from pathlib import Path

from data_platform_api.main import create_app


def test_openapi_matches_snapshot():
    """Guard against accidental contract changes.

    If this test fails, either (a) the change is intentional and you should
    regenerate openapi.json (see README), or (b) the change is unintentional
    and needs fixing.
    """
    snapshot_path = Path(__file__).parent / "openapi.json"
    expected = json.loads(snapshot_path.read_text())
    actual = create_app().openapi()
    assert actual == expected, (
        "OpenAPI spec drifted. Regenerate with:\n"
        "  python -c 'import json; from data_platform_api.main import create_app; "
        "print(json.dumps(create_app().openapi(), indent=2))' > tests/openapi.json"
    )
