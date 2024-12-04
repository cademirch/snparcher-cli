rule picard_intervals:
    input:
        unpack(get_reference),
    output:
        intervals=temp("intervals/intervals.list"),
    params:
        minNmer=50,
    conda:
        "../envs/bam2vcf.yml"
    log:
        "logs/picard_intervals/log.txt",
    benchmark:
        "benchmarks/picard_intervals/benchmark.txt"
    shell:
        "picard ScatterIntervalsByNs REFERENCE={input.ref} OUTPUT={output.intervals} MAX_TO_MERGE={params.minNmer} OUTPUT_TYPE=ACGT &> {log}"


rule format_interval_list:
    input:
        "intervals/intervals.list"
    output:
        "intervals/intervals.formatted.list"
    shell:
        """
        awk '!/^@/ {{print $1 ":" $2 "-" $3}}' {input} > {output}
        """

checkpoint create_db_intervals:
    input:
        unpack(get_reference),
        intervals="intervals/intervals.formatted.list",
    output:
        fof="intervals/db_intervals/interval_fof.txt",
        out_dir=directory("results/intervals/db_intervals"),
    params:
        max_intervals=10000000,
    log:
        "logs/db_intervals/log.txt",
    benchmark:
        "benchmarks/db_intervals/benchmark.txt"
    conda:
        "../envs/bam2vcf.yml"
    shell:
        """
        gatk SplitIntervals -L {input.intervals} \
        -O {output.out_dir} -R {input.ref} -scatter {params} \
        -mode INTERVAL_SUBDIVISION \
        --interval-merging-rule OVERLAPPING_ONLY &> {log}
        ls -l {output.out_dir}/*scattered.interval_list > {output.fof}
        """


checkpoint create_gvcf_intervals:
    input:
        unpack(get_reference),
        intervals="intervals/intervals.formatted.list",
    output:
        fof="intervals/gvcf_intervals/intervals_fof.txt",
        out_dir=directory("intervals/gvcf_intervals"),
    params:
        max_intervals=10000000,
    log:
        "logs/gvcf_intervals/log.txt",
    benchmark:
        "benchmarks/gvcf_intervals/benchmark.txt"
    conda:
        "../envs/bam2vcf.yml"
    shell:
        """
        gatk SplitIntervals -L {input.intervals} \
        -O {output.out_dir} -R {input.ref} --scatter {params} \
        -mode BALANCING_WITHOUT_INTERVAL_SUBDIVISION \
        --interval-merging-rule OVERLAPPING_ONLY  &> {log}
        ls -l {output.out_dir}/*scattered.interval_list > {output.fof}
        """
