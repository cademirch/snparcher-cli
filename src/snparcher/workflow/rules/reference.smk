ruleorder: download_reference > index_reference


rule download_reference:
    output:
        ref="data/genome/{refGenome}.fna",
    params:
        dataset="data/genome/{refGenome}_dataset.zip",
        outdir="data/genome/{refGenome}",
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/download_reference/{refGenome}.txt",
    benchmark:
        "benchmarks/download_reference/{refGenome}.txt"
    shell:
        """
        mkdir -p {params.outdir}
        datasets download genome accession --exclude-gff3 --exclude-protein --exclude-rna --filename {params.dataset} {wildcards.refGenome} \
        && (7z x {params.dataset} -aoa -o{params.outdir} || unzip -o {params.dataset} -d {params.outdir}) \
        && cat {params.outdir}/ncbi_dataset/data/{wildcards.refGenome}/*.fna > {output.ref}
        """


rule index_reference:
    input:
        ref=_reference,
    output:
        indexes=expand(
            "{{refGenome}}.{ext}",
            ext=["sa", "pac", "bwt", "ann", "amb"],
        ),
        fai="{refGenome}.fai",
        dictf="{refGenome}.dict",
    conda:
        "../envs/fastq2bam.yml"
    # since {refGenome} here is a full path log and benchmark paths are weird
    # is worth it imo since user can supply refgenome that is anywhere on their fs
    # has to be this way because some commands (picard) require genome idxs next to the fasta file
    # and give no way to specify a different location. stupid gatk.
    log:
        "logs/index_reference/{refGenome}.txt",
    benchmark:
        "benchmarks/index_reference/{refGenome}.txt"
    shell:
        """
        bwa index {input.ref}  2> {log}
        samtools faidx {input.ref} --output {output.fai} >> {log}
        samtools dict {input.ref} -o {output.dictf} >> {log} 2>&1
        """
