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

1. Create/activate Snakemake env: `conda create -c conda-forge -c bioconda -n snakemake snakemake>8.25`
2. Install snparcher: `pip install snparcher`
3. Run snparcher: `snparcher run`

## Sample Sheets

The workflow adapts to the sample sheet. Currently starting from fastqs (local/SRA) and BAMs (local) is supported. Here are examples:

Starting from BAMs:
```csv
sample_id,bam
sample1,test/data/bams/sample1.bam
```

Starting from fastqs:
```csv
sample_id,read_1,read_2,library_id
sample1,test/data/fastq/my_sample1_1.fastq.gz,test/data/fastq/my_sample1_2.fastq.gz,lib1
sample1,test/data/fastq/my_sample2_1.fastq.gz,test/data/fastq/my_sample2_2.fastq.gz,lib1
```

## Testing
After installing with pip, you can test it out:
1. Clone this repo: `git clone https://github.com/cademirch/snparcher-cli.git`
2. Change dirs into the repo: `cd snparcher-cli`
3. Run the workflow: `snparcher run --samples test/start_from_fastq/local.csv --reference test/data/genome/local_genome.fna.gz -F --cores 1`
