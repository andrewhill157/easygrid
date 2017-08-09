# easygrid
Many pipelining tools such as `Queue`, `snakemake`, `ruffus`, etc. provide powerful tools for specifying inputs and outputs of a tool and automating inference of dependencies, and I use some of them quite a bit for large pipelines.

I have found that for many small projects specifying all inputs and outputs becomes quite cumbersome depending on the task at hand and. I wanted something easier to use that still retains some of the benefits of the tools above.

`easygrid` allows you to add commands and give them a stage name and (optionally) a set of expected output files. You can make specify dependencies between stages and easygrid will figure out which order to execute them, monitor their status on grid engine, and then report back detailed statistics on their completion status and resource usage.

# Example
Let's say you have a list of fastq files and you want to align them all to a reference, compute basic statistics on each one, and then make some plots all in parallel:

```
import easygrid

# These could also be provided as script arguments, a samplesheet, etc.
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
   
    pipeline.add(align_command, name='align', outputs=[bam_file], memory='5G')
    pipeline.add(secondary_analysis_command, name='secondary_analysis', dependencies=['align'], outputs=[secondary_results])
    pipeline.add(plot_command, name='plot', dependencies=['secondary_analysis'], outputs=[plot_name])

pipeline.run()
```

By default `run_jobs` will log progress and a report will be saved to `.easygrid/job_report.txt` with information about completion status, runtime, memory usage, etc. Log files from all jobs will also be saved to this directory.

Specifying outputs of each job with the `outputs` argument is optional and need not be complete, these are just files that `easygrid` will look for when the job completes if you would like as a basic check. `easygrid` will also skip jobs and not re-run them if their outputs are already present.

`easygrid` also provides a handy `swap_ext` function for swapping file extensions much like other tools such as `Queue`.

# Job Report
`easygrid` generates a file `.easygrid/job_report.txt` after pipeline completion that logs several pieces of information about each job:

- `jobid`: the job ID assigned by SGE
- `stage`: the name of the stage this job belonged to
- `status`: one of `FAILED`, `COMPLETE`, `KILLED_BY_USER`, `FAILED_DEPENDENCY` (an entire stage failed before this job, so job was not scheduled), `SYSTEM_FAILED` (an error that is likely an technical failure not an actual program error), `COMPLETE_MISSING_OUTPUTS`.
- `was_aborted`: True if job was aborted (memory overage, for example) and False otherwise
- `exit status`: the exit code (or a string description for common exit codes)
- `memory_request`: the amount of memory requested
- `max_vmem_gb`: the max virtual memory used by the job in gigabytes
- `duration_hms`: the duration of job execution in hours:minutes:seconds notation
- `log_file`: the `.e*` and `*.o` log files for the job (comma separated)
- `command`: the command that was run

# Logging

After calling the `run` function, `easygrid` will log basic information about the jobs that are running, pending, completed, failed, and system failed.

```
[EASYGRID]: 2017-08-09 09:44:34,360: 9 jobs running (stages: align_reads)      0 jobs pending  33 jobs completed    1 jobs failed    0 system/user failures
```
