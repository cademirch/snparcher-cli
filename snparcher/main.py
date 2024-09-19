import typer
from pathlib import Path
from snakemake import cli as snakemake_cli
from typing_extensions import Annotated
from typing import List

app = typer.Typer(name="snparcher", help="snparcher!")


WORKFLOW_DIR = Path(__file__).parent / "workflow"


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def qc(
    vcf: Annotated[Path, typer.Argument(help="Path to vcf file")],
    fai: Annotated[Path, typer.Argument(help="Path to fai file")],
    coords_file: Annotated[
        Path, typer.Option(help="File containing coordinates for samples in VCF.")
    ],
    ctx: typer.Context,
    min_depth: Annotated[int, typer.Option(help="Min depth of SNPs to keep")],
    exclude_chrs: Annotated[
        List[str], typer.Option(help="Comma seperated list of chromosomes to exclude.")
    ] = [""],
    nclusters: Annotated[int, typer.Option(help="Number of clusters for PCA")] = 3,
    google_api_key: Annotated[
        str, typer.Option(help="Google API key for satellite map")
    ] = "",
):

    parser, snakemake_args = snakemake_cli.parse_args(ctx.args)
    prefix = vcf.stem.removesuffix(".vcf")
    config = [
        f"fai={fai}",
        f"vcf={vcf}",
        f"final_prefix={prefix}",
        "standalone=True",
        f"min_depth={min_depth}",
        f"scaffolds_to_exclude={exclude_chrs}",
        f"nClusters={nclusters}",
        f"GoogleAPIKey={google_api_key}",
        f"coords_file={coords_file}",
    ]
    if snakemake_args.config is not None:
        snakemake_args.config.extend(config)
    else:
        snakemake_args.config = config

    snakemake_args.snakefile = WORKFLOW_DIR / "modules" / "qc" / "Snakefile"

    snakemake_cli.args_to_api(snakemake_args, parser)

@app.callback()
def callback():
    pass