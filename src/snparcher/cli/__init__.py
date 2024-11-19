from pathlib import Path
from snk_cli import CLI
from snk_cli.config import SnkConfig

PARENT_DIR = Path(__file__).parent.parent

SNK_CONFIG_DIR = Path(PARENT_DIR / "snk_config")

snparcher = CLI(
    PARENT_DIR, snk_config=SnkConfig.from_path(Path(SNK_CONFIG_DIR / "main.yaml"))
)
snparcher_qc = CLI(
    Path(PARENT_DIR / "workflow" / "modules" / "qc" / "Snakefile"),
    snk_config=SnkConfig.from_path(Path(SNK_CONFIG_DIR / "qc.yaml")),
)
snparcher.register_group(snparcher_qc, name="qc", help="Run the qc workflow.")
