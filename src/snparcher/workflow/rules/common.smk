# ruff: noqa: F821
from pathlib import Path
from os.path import relpath
import pandas as pd
from collections import namedtuple

include: "preflight.smk"

if config["outdir"] is not None:
    config["samples"] = relpath(config["samples"],config["outdir"])
    config["reference"] = relpath(config["reference"],config["outdir"])
    workflow.workdir(config["outdir"])



samples = (
    SampleSheetValidator(config["samples"])
    .validate_samplesheet(config["outdir"])
    .set_index("sample_id")
    .reindex()
)


def get_reference(wc: namedtuple) -> dict:
    ref = _reference(wc)
    idxs = _reference_idx(wc)

    idxs["ref"] = ref
    return idxs


def _reference(wc: namedtuple) -> str:
    # get ref genome from config
    ref = config["reference"]
    if Path(ref).exists():
        return ref
    else:
        # we have an accession
        return f"data/genome/{ref}.fna"


def _reference_idx(wc: namedtuple) -> dict:
    ref =_reference(wc)
    prefix = Path(ref)
    while prefix.suffix in {'.fna', '.gz', '.fa'}:
        prefix = prefix.with_suffix('')
    
    

    out = dict(
        bwa_indexes=[f"{ref}.{ext}" for ext in ["sa", "pac", "bwt", "ann", "amb"]],
        fai=f"{ref}.fai",
        dictf=f"{prefix}.dict",
    )

    return out


def concat_reads(wc):
    """
    Get all reads for sample, library id combo to merge them.
    Does not support sra_accesion specified reads.
    """
    rows = samples.loc[[wc.sample]]
    row = rows.loc[rows["library_id"] == wc.library_id]
    return {
        "read_1": sorted(row["read_1"].tolist()),
        "read_2": sorted(row["read_2"].tolist()),
    }


def fastp_reads(wc):
    """
    Gets reads for fastp.
    - If sra_accession in sample sheet and has value for current sample, returns that
    - Then checks read_1,read_2 in sample sheet, returns those
    - Returns concat_reads output if more than 1 row for given sample,library_id combo
    """
    rows = samples.loc[[wc.sample]]
    out = {}
    if len(rows) > 1:
        row = rows.loc[rows["library_id"] == wc.library_id]
        if len(row) > 1:
            out["r1"] = f"fastq/concat/{wc.sample}/{wc.library_id}_1.fastq.gz"
            out["r2"] = f"fastq/concat/{wc.sample}/{wc.library_id}_2.fastq.gz"
            return out
    else:
        row = rows.iloc[0]
        if "sra_accesssion" in samples.columns:
            if not pd.isna(row["sra_accesssion"]):
                srr = row["sra_accesssion"]
                out["r1"] = f"fastq/raw/{srr}/{srr}_1.fastq.gz"
                out["r2"] = f"fastq/raw/{srr}/{srr}_2.fastq.gz"
        elif not pd.isna(row["read_1"]) and not pd.isna(row["read_2"]):
            out["r1"] = row["read_1"]
            out["r2"] = row["read_2"]
        else:
            raise ValueError(
                f"Row for sample {wc.sample} lacks valid read information."
            )

        return out


def aggregate_bams(wc: namedtuple) -> list[str]:
    row = samples.loc[wc.sample]
    return expand(
        "bams/pre_merge/{sample}/{library_id}.bam",
        sample=wc.sample,
        library_id=row["library_id"],
    )


def dedup_bams(wc: namedtuple) -> dict[str, str]:
    rows = samples.loc[[wc.sample]]
    out = {}

    if len(rows) == 1:
        library_id = (
            rows.iloc[0]["library_id"]
            if not pd.isna(rows.iloc[0]["library_id"])
            else wc.sample
        )

        out["bam"] = f"bams/pre_merge/{wc.sample}/{library_id}.bam"
        out["bai"] = f"bams/pre_merge/{wc.sample}/{library_id}.bam.bai"
    elif len(rows) >= 2:
        out["bam"] = f"bams/post_merge/{wc.sample}.bam"
        out["bai"] = f"bams/post_merge/{wc.sample}.bam.bai"

    return out


def bam_gatk_hc(wc: namedtuple):
    rows = samples.loc[[wc.sample]]
    row = rows.iloc[0]
    out = {}
    if "bam" in samples.columns:
        if not pd.isna(row["bam"]):
            out["bam"] = row["bam"]
            out["bai"] = row["bam"] + ".bai"
    else:
        out["bam"] = "bams/dedup/{sample}.bam"
        out["bai"] = "bams/dedup/{sample}.bam.bai"

    return out


def interval_gvcfs_idx(wc: namedtuple) -> list[str]:
    tbis = [f + ".tbi" for f in interval_gvcfs(wc)]
    return tbis


def interval_gvcfs(wc: namedtuple) -> dict:
    """
    Unpack doesnt work with checkpoints so need to split this up because we need both
    index and gvcf.
    """
    checkpoint_output = checkpoints.create_gvcf_intervals.get(**wc).output[0]
    with checkpoint_output.open() as f:
        lines = [l.strip() for l in f.readlines()]
    list_files = [Path(x).name for x in lines]
    list_numbers = [f.replace("-scattered.interval_list", "") for f in list_files]

    return expand("gvcfs/intervals/{{sample}}/{l}.raw.g.vcf.gz", l=list_numbers)


def read_group(wc: namedtuple) -> str:
    return r"'@RG\tID:{lib}\tSM:{sample}\tLB:{lib}\tPL:ILLUMINA'".format(
        sample=wc.sample, lib=wc.library_id
    )
