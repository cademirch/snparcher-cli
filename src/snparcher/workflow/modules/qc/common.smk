import sys
from pathlib import Path

# Get utils. This is not great, but we can move to setup.py and install later if want
# utils_path = (Path(workflow.main_snakefile).parent.parent.parent).resolve()
# if str(utils_path) not in sys.path:
#     sys.path.append(str(utils_path))

import pandas as pd
try:
    import snparcher.utils as utils
except ImportError:
    sys.path.append(str(Path(workflow.main_snakefile).parent.parent.parent.parent.parent))
    import snparcher.utils as utils

@utils.standalone_fallback(config, "coords_file")
def get_coords_if_available(wildcards):
        if 'lat' in samples.columns and 'long' in samples.columns:
            return "results/{refGenome}/QC/{prefix}.coords.txt"
        return []
@utils.standalone_fallback(config, "vcf")
def get_input_vcf(wildcards):
    return "results/{refGenome}/{prefix}_raw.vcf.gz"

@utils.standalone_fallback(config, "fai")
def get_input_fai(wildcards):
    return "results/{refGenome}/data/genome/{refGenome}.fna.fai"

def check_contig_names(fai, touch_file):
    
    dffai = pd.read_table(fai, sep='\t', header = None)
    fai_result=pd.to_numeric(dffai[0], errors='coerce').notnull().all()
    if fai_result==True:
        print("QC plots not generated because contig names are numeric and plink does not accept numeric contig names")
    elif fai_result==False:
        with open(touch_file, "w") as writer:
            writer.write("contigs are strings")

@utils.standalone_fallback(config, "sumstats", allow_missing=True)
def get_bam_stats(wildcards):
    return "results/{refGenome}/summary_stats/{prefix}_bam_sumstats.txt"