from pathlib import Path


rule get_fastq_pe:
    output:
        temp("fastq/raw/{srr}/{srr}_1.fastq.gz"),
        temp("fastq/raw/{srr}/{srr}_2.fastq.gz"),
    params:
        outdir="fastq/raw/{srr}",
    conda:
        "../envs/fastq2bam.yml"
    benchmark:
        "benchmarks/get_fastq_pe/{srr}.txt"
    log:
        "logs/get_fastq_pe/{srr}.txt",
    threads: 6
    shadow:"minimal"
    shell:
        """
        set +e
        # Delete existing prefetch file in case of previous run failure
        rm -rf {wildcards.srr}

        ## Attempt to get SRA file from NCBI (prefetch) or ENA (wget)
        prefetch --max-size 1T {wildcards.srr} &>> {log}
        prefetchExit=$?

        if [[ $prefetchExit -ne 0 ]]
        then
            # Capture stderr for ffq and curl commands in case of failure
            ffq --ftp {wildcards.srr} 2>> {log} | grep -Eo '"url": "[^"]*"' | grep -o '"[^"]*"$' | grep "fastq" | xargs curl --remote-name-all --output-dir {params.outdir} 2>> {log}
        else
            fasterq-dump {wildcards.srr} -O {params.outdir} -e {threads} -t {resources.tmpdir} &>> {log}
            pigz -p {threads} {params.outdir}/{wildcards.srr}*.fastq &>> {log}
        fi
        """

rule concat_reads:
    input:
        unpack(concat_reads)
    output:
        r1=temp("fastq/concat/{sample}/{library_id}_1.fastq.gz"),
        r2=temp("fastq/concat/{sample}/{library_id}_2.fastq.gz"),
    log:
        "logs/concat_reads/{sample}/{library_id}.txt",
    benchmark:
        "benchmarks/concat_reads/{sample}/{library_id}.txt"
    shell:
        """
        cat {input.read_1} > {output.r1} 2> {log}
        cat {input.read_2} > {output.r2} 2>> {log}
        """

rule fastp:
    input:
        unpack(fastp_reads),
    output:
        r1="fastq/filtered/{sample}/{library_id}_1.fastq.gz",
        r2="fastq/filtered/{sample}/{library_id}_2.fastq.gz",
        summ="summary_stats/fastp/{sample}/{library_id}.out",
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/fastp/{sample}/{library_id}.txt",
    benchmark:
        "benchmarks/fastp/{sample}/{library_id}.txt"
    shell:
        """
        fastp --in1 {input.r1} --in2 {input.r2} \
        --out1 {output.r1} --out2 {output.r2} \
        --thread {threads} \
        --detect_adapter_for_pe \
        -j {output.summ} -h /dev/null \
        &>{log}
        """

rule sort_reads:
    input:
        r1="fastq/filtered/{sample}_1/{library_id}.fastq.gz",
        r2="fastq/filtered/{sample}_2/{library_id}.fastq.gz"
    output:
        r1=temp("fastq/sorted/{sample}_1/{library_id}.fastq.gz"),
        r2=temp("fastq/sorted/{sample}_2/{library_id}.fastq.gz"),
    conda:
        "../envs/fastq2bam.yml"
    log:
        "logs/sort_reads/{sample}/{library_id}.txt",
    benchmark:
        "benchmarks/sort_reads/{sample}/{library_id}.txt"
    shell:
        """
        sortbyname.sh in={input.r1} out={output.r1} &> {log}
        sortbyname.sh in={input.r2} out={output.r2} &>> {log}
        """