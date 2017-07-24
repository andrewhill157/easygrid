# easygrid
This is currently a work in progress. The goal is to make a simple python-based pipeline execution tool for grid engine distributed computing platforms.

Many pipelining tools such as `Queue`, `snakemake`, `ruffus`, etc. provide powerful tools for specifying inputs and outputs of a tool and automating inference of dependencies, and I use some of them quite a bit for large pipelines.

I have found that for many small projects specifying all inputs and outputs becomes quite cumbersome depending on the task at hand and I wanted something easier to use even if it didn't have all the same features like automatically finding the right place to start execution after partial run, etc. 

This tool allows you to add commands and give them a stage name. You can make jobs assigned to a given stage dependent on any list of other stages. easygrid will figure out which order to execute your commands, monitor their status on grid engine, and then report back detailed statistics on their completion status and resource usage.

This makes things very simple even when specifying all inputs and outputs is not trivial. The tradeoff is that you lose nice features that the more complicated solutions offer.

I'm still experimenting with it vs. other solutions, YMMV.

# Example
Let's say you have a list of fastq files and you want to align them all to a reference, compute basic statistics on each one, and then make some plots all in parallel:

```
import easygrid

fastq_list = ['fastq1.fq', 'fastq2.fq', 'fastq3.fq']
reference = 'hg19.fa'

pipeline = easygrid.JobManager()

for fastq in fastq_list:
    bam_file = easygrid.swap_ext(fastq, '.fq', '.bam')
    secondary_results = easygrid.swap_ext(fastq, '.bam', '.results.txt')
    plot_name = easygrid.swap_ext(fastq, '.txt', '.png')

    align_command = 'align_reads.sh %s %s %s' % (fastq, reference, bam_file)
    secondary_analysis_command = 'python analyze_reads.py %s %s' % (bam_file, secondary_results)
    plot_command = 'Rscript plot_data.R %s %s' % (secondary_results, plot_name)
   
    pipeline.add_job(align_command, name='align', memory='5G')
    pipeline.add_job(secondary_analysis_command, name='secondary_analysis', dependencies=['align'])
    pipeline.add_job(plot_command, name='plot', dependencies=['secondary_analysis'])

pipeline.run_jobs()
```

By default `run_jobs` will log progress and a report will be saved to `.easygrid/job_report.txt` with information about completion status, runtime, memory usage, etc. Log files from all jobs will also be saved to this directory.

More detailed docs may come down the line if others are interested, but that is the basic idea.
