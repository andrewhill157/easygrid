# easygrid
`easygrid`is a pipelining tool for python and is inspired by `Queue` from the GATK team. I made it as a more convenient alternative to queue for my typical usage (IMO). Both `Queue` and `easygrid` take the approach of providing pipelining capabilities in what is otherwise a fairly normal looking script, which is very different from many other tools.

Queue is currently compatible with grid engine systems, but in principle could be made compatible with any `DRMAA` compatible system (LSF, for example). I don't currently have a need for this, but would be willing to work on it if there was interest.

# Installation
You must have `drmaa` available. On UW GS cluster you may simply:
```
module load drmaa/latest
```

You may then install `drmaa-python`:
```
pip install --user drmaa
```

Finally, you can install easygrid, for example:
```
git clone git@github.com:andrewhill157/easygrid.git
cd easygrid
python setup.py install --user
```

# Usage
There are two main ways to use `easygrid`. I described one below that might be easiest for beginners to understand, but recommend the `class`-based method described a bit later.

First, say you have a list of fastq files and you want to align them all to a reference, compute basic statistics on each one, and then make some plots all in parallel:

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

    pipeline.add(align_command, name='align', inputs=[reference, fastq], outputs=[bam_file], memory='5G')
    pipeline.add(secondary_analysis_command, name='secondary_analysis', inputs=[bam_file], outputs=[secondary_results])
    pipeline.add(plot_command, name='plot', inputs=[secondary_results], outputs=[plot_name])

pipeline.run()
```

Dependencies between jobs are automatically inferred from inputs/outputs to determine the order of execution.

As shown above, `easygrid` also provides a handy `swap_ext` function for swapping file extensions much like other tools such as `Queue`. There are a few other handy functions in there was well that are not yet fully documented.

# More Organized Way to Add Jobs
`easygrid` also allows specification of jobs via classes using the `add_job()` command (alternative to add()). This is more similar to the interface used in the `Queue` tool from the GATK team, and it is more nicely organized.

Here is a partial example of just a step to align reads:
```
class AlignReads:
    def __init__(self, fastq, reference, bam_file):
        self.inputs = [fastq, reference]
        self.outputs = [bam_file]
        self.memory = '5G'
        self.command = "align_reads.sh %s %s %s" % (fastq, reference, bam_file)

pipeline = easygrid.JobManager()
pipeline.add_job(AlignReads(myfastq, myreference, mybam))

...
```

The object you add must simply specify the command attribute and may optionally specify any attributes that match the name of arguments to the add function such as self.inputs, self.outputs, etc. If you do not provide a name command, the name will be the name of the class. All others are optional as with the `add()` command.

This allows for easier code reuse and is more organized. It may not be as intuitive to some people, which is why both add() and add_job() are supported.

# Lazy Mode
Sometimes specifying all the inputs and outputs can be cumbersome for certain tools. If you just want to run a few things according a known chain of dependencies, you can do it by specifying dependencies manually via the `dependencies` argument.

When adding to the pipeline (as above in the first example) you would just do:
```
...
    pipeline.add(align_command, name='align', memory='5G')
    pipeline.add(secondary_analysis_command, name='secondary_analysis', dependencies=['align'])
    pipeline.add(plot_command, name='plot', dependencies=['secondary_analysis'])

pipeline.run(infer_dependencies=False) # note infer_dependencies=False in this mode
```

In this mode you could also optionally specify any set of inputs and/or outputs that you want checked at the beginning and end of job execution respectively, but they will not be used to infer dependencies.

This mode is not recommended in most cases.

# Other Details
- Commands may contain pipes/redirects, etc. and span more than one line. You can even have comments as long as they are the only thing on their line.

- When jobs fail, but write partial output, you don't need to worry about deleting the partial files, `easygrid` writes hidden `.done` files for each output file only when the output is present and the job returned no error code, which means these jobs will rerun on restart if there was an error.

- If you say figured out that an intermediate step was incorrect and removed its outputs, if you run the pipeline any downstream steps that depend on that stage will rerun as well.

- Unlike queue, this tool is centered on sets of jobs all with the same name rather than dependencies between individual jobs. So each stage will run all at once and is finished only once all jobs within that set of jobs completes. Multiple independent stages may all run at once.

# Logging
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

After calling the `run` function, `easygrid` will log basic information about the jobs that are running, scheduled but not running yet (qw), completed, and failed.

```
[EASYGRID]: 2017-08-09 09:44:34,360: 9 jobs running (stages: align_reads)	0 jobs qw | 0 stages pending | 0 jobs completed | 0 jobs failed
```

Log files from grid engine for each job will also be output to the same directory -- `.easygrid/`.

# Dry Run
You may also provide `dry=True` to the `run` function and a text description of jobs that would be run will print to the screen. This "dry run" feature is handy for checking that everything is set up correctly before executing the pipeline.

