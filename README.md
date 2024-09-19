# snparcher CLI 

This is a proof of concept for making snparcher a CLI tool. There are a handful of reasons why we are considering moving to a CLI:
- Easier to install via `pip`
- Minimize user overhead of setting up config/samplesheet files
- Allow direct access to QC/Postprocessing/etc modules without having to setup directory structure
- More developer control of user experience overall.

## Design 

Overall this is a relatively simple CLI Python app, it takes arguments/options from the command line and then does stuff. For snparcher, "doing stuff" means running our workflow files. To achieve this, I use the entrypoint function from Snakemake itself: `snakemake.cli:args_to_api` (see [here](https://github.com/snakemake/snakemake/blob/56a1f207ecf8343deab2b1583709fc9effc0ffb1/snakemake/cli.py#L1864) and [here](https://github.com/snakemake/snakemake/blob/56a1f207ecf8343deab2b1583709fc9effc0ffb1/snakemake/cli.py#L2168-L2177) for more info.) Essentialy this function takes all the CLI args and constructs the Snakemake API and executes the workflow. 


I've used [`typer`](https://typer.tiangolo.com/) for parsing CLI arguments for its simplicity over the stdlib `argparse`. `typer` allows you to capture all CLI arguments, including unknown ones: 

```
@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
```

This allows us to pass Snakemake args (`--use-conda, --profile, etc`) to Snakemake while capturing arguments for our command. It also allows us to specify Snakemake arguments on behalf of the user, such as the workflow file. 

So when using snparcher commands, a user will pass the snparcher command arguments first, then the Snakemake arguments/options. See below for example.



## Setup

To try this out:

1. Clone this repo:
```console
git clone https://github.com/cademirch/snparcher-cli.git
cd snparcher-cli
```
2. Create Conda env, or use [uv](https://docs.astral.sh/uv/) to setup env.

- **Conda**:`conda create -n snparcher-cli-env "python>=3.12"`
- **uv**:`uv venv --python 3.12 && source .venv/bin/activate`
3. Install `snparcher` locally and editable

- **Conda**:` conda activate snparcher-cli-env && pip install -e .`
- **uv**:` uv pip install -e .`

4. Check it worked! `snparcher --help`


## Testing

You can test the cli like so:
```console
cd test/cli
snparcher qc --coords-file cli-coords.txt --min-depth 4 test_qc_raw.vcf.gz genome1.fna.fai --use-conda --cores 8
```

The workflow still works when run using Snakemake:
```
# from root of this repo
snakemake -s snparcher/workflow/modules/qc/Snakefile -d test/run_with_snakemake --use-conda --cores 8
```

## CLI Usage
# `snparcher`

snparcher!

**Usage**:

```console
$ snparcher [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `qc`

## `snparcher qc`

**Usage**:

```console
$ snparcher qc [OPTIONS] VCF FAI
```

**Arguments**:

* `VCF`: Path to vcf file  [required]
* `FAI`: Path to fai file  [required]

**Options**:

* `--coords-file PATH`: File containing coordinates for samples in VCF.  [required]
* `--min-depth INTEGER`: Min depth of SNPs to keep  [required]
* `--exclude-chrs TEXT`: Comma seperated list of chromosomes to exclude.
* `--nclusters INTEGER`: Number of clusters for PCA  [default: 3]
* `--google-api-key TEXT`: Google API key for satellite map
* `--help`: Show this message and exit.