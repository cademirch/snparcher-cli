rule bwa_mem:
    input:
        unpack(get_reference),
        r1="fastq/filtered/{sample}/{library_id}_1.fastq.gz",
        r2="fastq/filtered/{sample}/{library_id}_2.fastq.gz",
    output:
        bam=temp("bams/pre_merge/{sample}/{library_id}.bam"),
        bai=temp("bams/pre_merge/{sample}/{library_id}.bam.bai"),
    params:
        rg=read_group,
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/bwa_mem/{sample}/{library_id}.txt",
    benchmark:
        "benchmarks/bwa_mem/{sample}/{library_id}.txt"
    threads: 8
    shell:
        "bwa mem -M -t {threads} -R {params.rg} {input.ref} {input.r1} {input.r2} 2> {log} | samtools sort -o {output.bam} - && samtools index {output.bam} {output.bai}"


rule merge_bams:
    input:
        aggregate_bams,
    output:
        bam=temp("bams/post_merge/{sample}.bam"),
        bai=temp("bams/post_merge/{sample}.bam.bai"),
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/merge_bams/{sample}.txt",
    benchmark:
        "benchmarks/merge_bams/{sample}.txt"
    shell:
        "samtools merge {output.bam} {input} && samtools index {output.bam} > {log}"


rule dedup:
    input:
        unpack(dedup_bams),
    output:
        dedupBam="bams/dedup/{sample}.bam",
        dedupBai="bams/dedup/{sample}.bam.bai",
    conda:
        "../envs/sambamba.yml"
    log:
        "logs/dedup/{sample}.txt",
    benchmark:
        "benchmarks/dedup/{sample}.txt"
    shell:
        "sambamba markdup -t {threads} {input.bam} {output.dedupBam} 2> {log}"
