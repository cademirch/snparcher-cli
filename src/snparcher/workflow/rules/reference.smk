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
        
    conda:
        "../envs/fastq2bam.yml"
    
    log:
        "logs/index_reference/{refGenome}.txt",
    benchmark:
        "benchmarks/index_reference/{refGenome}.txt"
    shell:
        """
        bwa index {input.ref}  2> {log}
        samtools faidx {input.ref} --output {output.fai} >> {log}
        """
rule samtools_sequence_dict:
    input:
        ref=_reference,
    output:
        dictf="{refGenome}.dict",
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/samtools_sequence_dict/{refGenome}.txt",
    benchmark:
        "benchmarks/samtools_sequence_dict/{refGenome}.txt"
    shell:
        """
        samtools dict {input.ref} -o {output.dictf} >> {log} 2>&1
        """