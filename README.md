# easygrid
`easygrid`is a pipelining tool written in python inspired by `Queue` from the GATK team.

I wanted but could not find a tool that was both:
1. simple and easy enough to use that I wouldn't be put off using it for one-off analyses
2. fully featured enough that I could use it for complex pipelines

Another motivation was to learn more about how one would build out a workflow management tool and the different challenges to consider. Fortunately, I have also come to use `easygrid` extensively in my own work.

`easygrid` is currently compatible with grid engine systems, but in principle could be made compatible with any `DRMAA` compatible system (LSF, for example) with some work. It does not currently support `docker` and will likely not ever be as fully featured as other tools like `snakemake`, `nextflow`, `WDL/cromwell` and others that have been widely adopted by the community, but I personally find it much easier to use (your mileage may vary).

# Installation
You must have `drmaa` available. On UW GS cluster which has a module system available you may simply:
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
`easygrid` allows you to specify different stages of a pipeline via classes. If you have used tools like `Queue` from the GATK team, the overall look and feel of the code below should be fairly familiar.

For example. say we wanted to define a stage that aligns reads to the genome.
```
class AlignReads:
    def __init__(self, fastq, reference, bam_file):
        self.inputs = [fastq, reference]
        self.outputs = [bam_file]
        self.threads = 8
        self.memory = '5G'
        
        self.command = """
        myaligner {fastq} \
        {reference} \
        {bam_file} \
        --threads {threads}
        """.format(fastq=fastq, reference=reference, bam_file=bam_file, threads=self.threads)

pipeline = easygrid.JobManager('_logs')
pipeline.add_job(AlignReads(myfastq, myreference, mybam))
pipeline.run(dry=False)
...
```

So you intialize a pipeline via the `JobManager` class and then use the `add_job` method to add specific jobs to the pipeline. 

In a given class, you can specify:
- `self.inputs`: list of file names or wildcards representing inputs to that job
- `self.outputs`: same as inputs but for outputs of the job
- `self.threads`: integer indicating the number of slots/threads the job should request
- `self.memory`: string such as `5G` that indicates the amount of memory requires (5 gigabytes here)
- `self.batch_size`: integer indicating the number of jobs from this stage that be run per process on the cluster. This allows multiple commands to be run as a single job on the cluster without having to manually batch them into groups)
- `self.walltime`: walltime string such as `24:00:00` as passed to say `-l h_rt=24:00:00` in SGE, for example.
- `self.command`: the actual command to be run. With some exceptions this can be pretty much any single or multi-line command that you would run in bash. Any comments must be on their own line and some awk commands or other more exotic multi-line commands can sometimes cause issues because all multi-line commands have to be converted to a valid single line command within easygrid.

# Other Details
- Commands may contain pipes/redirects, etc.

- When jobs fail, but write partial output, you don't need to worry about deleting the partial files, `easygrid` writes hidden `.done` files for each output file only when the output is present and the job returned no error code, which means these jobs will rerun on restart if there was an error.

- If you say figured out that an intermediate step was incorrect and removed its outputs, if you run the pipeline any downstream steps that depend on that stage will rerun as well.

- Unlike some other tools, timestamps on files are not used to determine when downstream steps should be run. If a file and its `.done` file are present, the pipeline considers that job to be complete unless one of the jobs it depends on had to be rerun.

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

