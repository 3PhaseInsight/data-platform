from pathlib import Path
import yaml


def load_dag_config(dag_file: str) -> dict:
    path = Path(dag_file)
    config_path = path.parent / "configs" / f"{path.stem}_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
