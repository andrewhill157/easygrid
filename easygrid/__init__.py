import drmaa
import os
import logging
import time
import sys
import itertools
import tempfile
import re
import datetime
from glob import glob
import stat
import copy
import collections
import gzip

ARRAY_JOB_SCRIPT = """
#!/bin/bash
#$ -S /bin/bash
#$ -shell y
#$ -cwd
#$ -V
#$ -l h_rt=24:00:00

if [ -z "$SGE_TASK_ID" ] ; then
    echo "WARNING: NO $SGE_TASK_ID"
    exit 1
fi

CMD_FILE=$1

COMMAND=$(head -n $SGE_TASK_ID $CMD_FILE | tail -n 1)

if [ -n "$COMMAND" ] ; then
    $COMMAND
else
    echo "command \"$COMMAND\"not found"
fi

# $Id: generic_array_job.csh,v 1.1 2006/05/16 14:11:32 sbender Exp $
"""

# Set up a basic logger
LOGGER = logging.getLogger('something')
myFormatter = logging.Formatter('%(asctime)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(myFormatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)
myFormatter._fmt = "[EASYGRID]: " + myFormatter._fmt

# Run state definitions
RUNNING = 'RUNNING'
PENDING = 'PENDING'
FINISHED = 'FINISHED'
SKIPPED = 'SKIPPED'

# Completion status definitions
FAILED = 'FAILED'
COMPLETE = 'COMPLETE'
KILLED_BY_USER = 'KILLED_BY_USER'
FAILED_DEPENDENCY = 'FAILED_DEPENDENCY'
SYSTEM_FAILED = 'SYSTEM_FAILED'
COMPLETE_MISSING_OUTPUTS = 'COMPLETE_MISSING_OUTPUTS'

# Buffer between job completion and completion status check to prevent race conditions
COMPLETION_OUTPUT_CHECK_DELAY = 20000 # 10 seconds (time in ms)

# Table to translate common error codes into more useful text
exit_codes = {
	127: 'command not found',
	126: 'command invoked cannot execute',
	2: 'misuse of shell builtins',
	1: 'general error',
	0: 0
}

def swap_ext(filename, old_extension, new_extension):
    """
    Utility function to swap file extensions on files.

    Args:
        filename (str): name of original file
        old_extension (str): name of the current file extension
        new_extension (str): name of the new file extension

    Returns:
        str: new filename

    """
    if (not new_extension or len(new_extension) == 0) or (not old_extension or len(old_extension) == 0):
        raise ValueError('Old or new extension is invalid: %s, %s' % (old_extension, new_extension))

    if old_extension not in filename:
        raise ValueError('Old extension not found in filename: %s' % old_extension)

    return filename.replace(old_extension, new_extension)


def mkdir(directory):
    """
    Simple utility function to make directories that don't already exist.

    Args:
        directory (str): directory to make

    Modifies:
        directory is made on disk unless it already exists

    """
    if not os.path.exists(directory):
        os.mkdir(directory)

def check_exists(path):
    """
    Utility function to check if a file exists and throw and error if not.

    Args:
        path (str): path to check

    Raises:
        ValueError if path does not exist.

    """
    if not os.path.exists(path):
        raise ValueError('Path provided as input was not found: %s' % path)

def read_delim(file_path, header=True, columns=None, types=None, sep='\t'):
    """
    Utility function for parsing delimited files into iterators of dictionaries with keys as column names.

    Args:
        file_path (str): path to file
        header (bool): True if column names are present in file and false otherwise
        columns (list): List of keys to use for dictionaries as column names
        types (dict): a dict with column names as keys and conversion functions such as int, float, etc. as values
        sep (str): delimiter used in file

    Yields:
        dict: yields dict where keys are column names and values are values for a given row in file

    """
    # Choose gz open if needed
    if file_path.endswith('.gz'):
        open_function=gzip.open
    else:
        open_function=open

    # Vlalidate columns
    if columns and not isinstance(columns, list):
        raise ValueError('columns argument must be a list.')
    
    # Validate types and column headers
    if types and not isinstance(types, dict):
        raise ValueError('types argument must be a dict with column names as keys and a conversion function as values (such as str, int, float)')

    fh = open_function(file_path)

    if header:
        if not columns:
            columns = fh.next().strip().split(sep)
        else:
            fh.next()
    elif not columns:
        raise ValueError('Header argument specified as False, so must provide column names.')

    if types:
        typed_columns = set(types.keys())
        if False in [column in typed_columns for column in columns]:
            raise ValueError('Provided a type for %s column in types argument, but column does not appear in file or columns argument.' % column)

    # Parse file
    for line_number,line in enumerate(fh):
        entries = line.strip().split(sep)
        
        if len(entries) != len(columns):
            raise ValueError('Length of entries: %s, not equal to length of columns %s, in file: %s, line number %s' % (len(entries), len(columns), file_path, line_number))

        entries_dict = dict(zip(columns, entries))
	
        if types:
            for column in types:
                try:
                    entries_dict[column] = types[column](entries_dict[column])
                except ValueError:
                    raise ValueError('Type conversion of column %s failed on line %s' % (column, line_number))

        yield entries_dict


def _topological_sort(joblist):
    """
    Topological sort of a list of jobs and their dependencies
    """
    graph_unsorted = list(set([(job.name, tuple(job.dependencies)) for job in joblist]))

    graph_sorted = []

    graph_unsorted = dict(graph_unsorted)

    # Run until the unsorted graph is empty.
    while graph_unsorted:
        acyclic = False
        for node, edges in list(graph_unsorted.items()):
            for edge in edges:
                if edge in graph_unsorted:
                    break
            else:
                acyclic = True
                del graph_unsorted[node]
                graph_sorted.append((node, edges))

        if not acyclic:
            raise RuntimeError("Cyclic dependency detected. Dependencies may not be circular (A dependent on B and B dependent on A is invalid, for example.")

    # Sort job list according to topological ordering
    order = dict([(job[0], i) for i,job in enumerate(graph_sorted)])
    joblist = sorted(joblist, key=lambda x: order[x.name])
    return joblist

class Job:
	"""
	Class for holding metadata about a job.
	"""

	def __init__(self, command, name, dependencies=[], memory='1G', walltime='100:00:00', outputs = []):
		if not isinstance(name, str):
			raise ValueError('Provided job name must be a string, but found: %s' % name)

		if isinstance(dependencies, str):
			dependencies = [dependencies]

		if isinstance(outputs, str):
			outputs = [outputs]

		if not isinstance(memory, str):
			raise ValueError('Memory request for job is not a string: %s. Must be a string such as "5G", where G indicates GB.' % memory)

		if not re.search('[0-9]+[kKmMgG]$', memory):
			raise ValueError('Invalid memory request: %s, valid multipliers are k, m, or g (case insensitive).' % memory)

		if not isinstance(walltime, str):
			raise ValueError('Walltime request for job is not a string: %s. Must be a string such as "100:00:00" for units in hours:minutes:seconds.' % walltime)

		self.command = command
		self.memory = memory
		self.walltime = walltime
		self.dependencies = dependencies
		self.name = name
		self.outputs = outputs
		self.id = None
		self.exit_status = {}
		self.run_state = None

	def set_id(self, id):
		self.id = id

	def set_exit_status(self, exit_status):
		self.exit_status = exit_status

	def set_run_state(self, state):
		self.run_state = state

	def outputs_exist(self):
		"""
		Helper function to check if the requested set of output files exist.
		"""

		if len(self.outputs) == 0:
			return True
		else:
			return not False in [os.path.exists(path) for path in self.outputs]


class JobManager:
	"""
	Class for constructing and executing simple pipelines of jobs on distributed computing systems
	where different stages of the pipeline are dependent on one another.
	"""

	def __init__(self, temp_directory='.easygrid'):
		"""
		Constructor

		Args:
			temp_directory: A temporary directory for job manager output (temp files, job reports, etc.)

		"""
		self.joblist = []
		self.temp_directory = os.path.abspath(temp_directory)
		self.session = drmaa.Session()
		self.session.initialize()
		self.submitted_jobs = collections.OrderedDict()
		self.queued_jobs = collections.OrderedDict()
		self.completed_jobs = collections.OrderedDict()
		self.completed_stages = set()
		self.skipped_jobs = []
		self.job_templates = []

	def __del__(self):
		# Clean everything up
		self.clear()

		# Close the DRMAA session
		self.session.exit()

	def clear(self):
		# Remove temp files
		for file in glob(os.path.join(self.temp_directory, 'tmp*')):
			os.remove(file)

		# Remove the the job template files
		for template in self.job_templates:
			self.session.deleteJobTemplate(template)

		# Remove bash helper script
		if os.path.exists(self._get_job_array_helper_path()):
			os.remove(self._get_job_array_helper_path())

		self.joblist = []
		self.submitted_jobs = collections.OrderedDict()
		self.queued_jobs = collections.OrderedDict()
		self.completed_jobs = collections.OrderedDict()
		self.completed_stages = set()
		self.complete = False
		self.skipped_jobs = []
		self.job_templates = []

	def set_temp_directory(self, path):
		"""
		Setter for the temp directory for pipeline runs.
		"""
		self.temp_directory = os.path.abspath(path)

	def add(self, command, name, dependencies=[], memory='1G', walltime='100:00:00', outputs=[]):
		"""
		Adds a command to be run as a job in the job manager.
		Must be called at least once prior to calling run_jobs.

		Args:
			command (str): Command to be run in terminal for this job.
			name (str): a name to identify the stage of the pipeline this job belongs to
			memory (str): memory request for job such as '1G'
			walltime (str): wall time request such as '100:00:00'
			dependencies (list of str): a list of names for other stages that this job is dependent on.
			outputs (list of str): a list of output files to check for before scheduling (if all are present, job not scheduled)

		"""
		# Check output files

		job = Job(command, name, dependencies=dependencies, memory=memory, walltime=walltime, outputs=outputs)

		if len(job.outputs) == 0 or not job.outputs_exist():
			self.joblist.append(job)
		else:
			self.skipped_jobs.append(job)

	def run(self, queue=None, logging=True, dry=False):
		"""
		After adding jobs with add_jobs, this function executes them as a pipeline on Grid Engine.

		Modifies:
			Prints status to screen periodically as jobs change status.
			Saves job report to temp directory on completion.

		Returns:
			bool: True if no failed jobs and False otherwise

		"""
		# Error if no jobs added
		if not self.joblist and not self.skipped_jobs:
			raise ValueError('No jobs added. Must call add_jobs prior to running jobs.')

		# Make sure no dependencies that are not in the scheduled or skipped set of jobs
		self._check_dependencies()

		# Make temp directory for job files and reports
		self._prep_temp_dir()

        # Write out Job Array helper script to temp directory
		self._write_job_array_helper()

		# Sort jobs in required order by dependency
		self.joblist = _topological_sort(self.joblist)

		# Infer any implicit dependencies on sorted data
		self._infer_all_dependencies()

		# If any stages and entirely skipped, add to completed stages
		queued_stages = set([job.name for job in self.joblist])
		skipped_stages = set([job.name for job in self.skipped_jobs])
		self.completed_stages.update(skipped_stages.difference(queued_stages))

		# Submit each group of jobs as an array (or perform a dry run)
		if dry:
			print('DRY RUN: would skip %s jobs that already have outputs present...' % len(self.skipped_jobs))

		# Build up the queue
		for group, joblist in itertools.groupby(self.joblist, key=lambda x: x.name):
			joblist = list(joblist)

			if dry:
				print(self._get_group_dry_run_message(group, joblist))

				for job in joblist[0:10]:
					print(self._get_job_dry_run_message(job))
				if len(joblist) > 10:
					print('... (%s jobs not shown)' % (len(joblist) - 10))
			else:
				joblist = self.queued_jobs[group] = joblist

		if dry:
			return

		#####################################
		# Now start scheduling jobs
		#####################################
		if not self.queued_jobs:
			LOGGER.info('All %s jobs have outputs present. Nothing to do.' % len(self.skipped_jobs))

		# Log skipped jobs if there are any
		if len(self.skipped_jobs) > 0:
			LOGGER.info('Skipping %s jobs because specified outputs already present...' % len(self.skipped_jobs))

		last_log = None

		while True:
			# Update status of any running jobs
			for group, joblist in self.submitted_jobs.items():
				for job in joblist:
					jobstatus = self._get_run_pass_fail(job)
					job.set_run_state(jobstatus)

					if jobstatus == FINISHED:
						job.set_exit_status(self._get_exit_status(job))

			# Get any failed or finished stages
			failed_stages = set(self._get_failed_stages())
			self.completed_stages.update(self._get_finished_stages())

			# Move any completed jobs to completed queue and remove from scheduled
			for stage in self.completed_stages:
				if stage in self.submitted_jobs and stage not in self.completed_jobs:
					self.completed_jobs[stage] = self.submitted_jobs[stage]
					del self.submitted_jobs[stage]

			# Decide if need to schedule any new stages
			for group in list(self.queued_jobs.keys()):
				joblist = self.queued_jobs[group]
				dependencies = joblist[0].dependencies

				dependency_failed = True in [dependency in failed_stages for dependency in dependencies]
				dependencies_completed = False not in [dependency in self.completed_stages for dependency in dependencies]

				if dependency_failed:
					# A dependency or chained dependency has failed, move to completed with relevant status
					exit_status = {'hasExited': 'NA',
						'hasSignal': 'NA',
						'terminatedSignal': 'NA',
						'hasCoreDump': 'NA',
						'wasAborted': 'NA',
						'exitStatus': 'NA',
						'resourceUsage': 'NA',
						'completion_status': FAILED_DEPENDENCY}

					for job in joblist:
						job.set_run_state(FINISHED)
						job.set_exit_status(exit_status)

					self.completed_jobs[group] = joblist
					del self.queued_jobs[group]

				elif dependencies_completed:
					# All dependencies are done, schedule stage
					self.submitted_jobs[group] = joblist
					self._submit_arrayjob(joblist, queue)
					del self.queued_jobs[group]


			# Calculate counts for logging
			total_running = self._get_run_status_count(RUNNING)
			total_qw = self._get_run_status_count(PENDING)
			total_pending = len(self.queued_jobs.keys())

			## Get which stages are running (or just show "none")
			stages_running = self._get_stages_with_run_status(RUNNING)

			if not stages_running:
				stages_running = ['none']

			total_failed = self._get_completion_status_count(FAILED) + self._get_completion_status_count(SYSTEM_FAILED) + self._get_completion_status_count(KILLED_BY_USER) + self._get_completion_status_count(COMPLETE_MISSING_OUTPUTS)
			total_complete = self._get_completion_status_count(COMPLETE)

			log_message = '%s jobs running (stages: %s)\t%s jobs qw\t%s jobs pending\t%s jobs completed\t%s jobs failed\r' % (total_running, ','.join(stages_running), total_qw, total_pending, total_complete, total_failed)

			# Only log when status has changed and when requested
			if logging and last_log and (last_log != log_message):
				LOGGER.info(log_message)

			last_log = log_message

			# Check to see if all jobs have completed
			if not self.queued_jobs and not self.submitted_jobs:
				break

			time.sleep(1)

		# Write out the report of logging results
		self.write_report(os.path.join(self.temp_directory, 'job_report.txt'))

		return total_failed == 0

	def _infer_all_dependencies(self):
		"""
		Helper function to take the intitial list of jobs and get the full set of dependencies including chained dependencies.

		Args:
			stages: list of stages to consider

		Returns:
			list of Job: list of jobs that belong to or are dependent on a list of stages.

		"""
		all_stages = list(set([job.name for job in self.joblist]))
		all_dependencies = {}

		# Get the full list of other stages each stage is dependent on
		for stage in all_stages:
			stage_set = set([stage])

			for job in self.joblist:
				for dependency in job.dependencies:
					if dependency in stage_set:
						stage_set.add(job.name)

			for other_stage in list(stage_set):
				if other_stage != stage:
					all_dependencies[other_stage] = list(set(all_dependencies.get(other_stage, []) + [stage]))

		# Now fill in for all jobs
		for job in self.joblist:
			job.dependencies = list(set(job.dependencies + all_dependencies.get(job.name, [])))


	def _check_dependencies(self):
		"""
		Make sure that there are no dependencies to stages that were never added.
		"""

		all_dependencies = []
		for job in self.joblist:
			all_dependencies.extend(job.dependencies)
		all_dependencies = set(all_dependencies)

		all_stages = set([job.name for job in self.skipped_jobs] + [job.name for job in self.joblist])

		difference = list(all_dependencies.difference(all_stages))

		if len(difference) > 0:
			raise ValueError('Invalid dependency detected. %s listed as dependency but no stage shares this name.' % ', '.join(difference))

	def _write_job_array_helper(self):
		"""
		Writes a bash script to temp folder to facilitate array job submission
		"""
		with open(self._get_job_array_helper_path(), 'w') as helper_script:
			helper_script.write(ARRAY_JOB_SCRIPT)
			st = os.stat(self._get_job_array_helper_path())
			os.chmod(self._get_job_array_helper_path(), st.st_mode | stat.S_IEXEC)

	def _prep_temp_dir(self):
		"""
		Make the temporary directory needed for JobManager job execution.
		If exists, clear out old files.
		"""
		#
		if not os.path.exists(self.temp_directory):
			os.makedirs(self.temp_directory)

		# Clean any existing files
		for file in glob(os.path.join(self.temp_directory, '*')):
			os.remove(file)

	def _get_group_dry_run_message(self, group, joblist):
		"""
		Helper function for logging info about each stage in dry runs
		"""
		dependencies = joblist[0].dependencies

		if dependencies:
			dependency_string = ' (%s jobs; dependent on %s)' % (len(joblist), ', '.join(dependencies))
		else:
			dependency_string = ' (%s jobs)' % len(joblist)

		return 'Job array for group %s%s:' % (group, dependency_string)

	def _get_job_dry_run_message(self, job):
		"""
		Helper function to get a log message from a Job object
		"""
		if job.outputs:
			output_string = ' (outputs: %s)' % ', '.join(job.outputs)
		else:
			output_string = ''
		return '\t%s%s' % (job.command, output_string)

	def _get_run_pass_fail(self, job):
		if job.run_state == FINISHED:
			return job.run_state

		try:
			jobstatus = self.session.jobStatus(job.id)

			if  jobstatus != drmaa.JobState.DONE and jobstatus != drmaa.JobState.FAILED:
				if jobstatus == drmaa.JobState.RUNNING:
					return RUNNING
				else:
					return PENDING
			else:
				return FINISHED

		except:
			return FINISHED

	def _get_exit_status(self, job):
		# If already got this, just return stored value
		if job.exit_status:
			return job.exit_status

		try:
			exit_status = self.session.wait(job.id, drmaa.Session.TIMEOUT_WAIT_FOREVER)

			exit_status = {'hasExited': exit_status.hasExited,
				'hasSignal': exit_status.hasSignal,
				'terminatedSignal': exit_status.terminatedSignal,
				'hasCoreDump': exit_status.hasCoreDump,
				'wasAborted': exit_status.wasAborted,
				'exitStatus': exit_codes.get(exit_status.exitStatus, 'unknown exit code: %s' % exit_status.exitStatus),
				'resourceUsage': exit_status.resourceUsage}

			# Give some buffer between when jobs have finished and when check for output files (tries to prevent race)
			completion_time = int(float(exit_status['resourceUsage']['end_time']))
			time_since_completion = int(float((time.time() * 1000) - completion_time))

			if time_since_completion < COMPLETION_OUTPUT_CHECK_DELAY:
				time.sleep((COMPLETION_OUTPUT_CHECK_DELAY - time_since_completion) / 1000)

			# Now check exit status
			if exit_status['exitStatus'] == 0:
				if job.outputs_exist():
					exit_status['completion_status'] = COMPLETE
				else:
					exit_status['completion_status'] = COMPLETE_MISSING_OUTPUTS
			elif exit_status['wasAborted']:
				exit_status['completion_status'] = KILLED_BY_USER
			elif exit_status['terminatedSignal'] or exit_status['hasCoreDump']:
				exit_status['completion_status'] = SYSTEM_FAILED
			else:
				exit_status['completion_status'] = FAILED
		except Exception as e:
			# In some cases where jobs are killed by user, wait will fail, so need to catch
			exit_status = {'hasExited': 'NA',
				'hasSignal': 'NA',
				'terminatedSignal': 'NA',
				'hasCoreDump': 'NA',
				'wasAborted': 'NA',
				'exitStatus': 'NA',
				'resourceUsage': 'NA',
				'completion_status': KILLED_BY_USER}

		job.set_exit_status(exit_status)
		return exit_status

	def _get_stages_with_run_status(self, value):
		"""
		Return a list of stage names for jobs that have a specified run status.

		Args:
			value (str): run status to consider

		Returns:
			int: stages with requested run status
		"""
		stages = []
		for group in self.submitted_jobs:
			for job in self.submitted_jobs[group]:
				if job.run_state == value:
					stages.append(job.name)
		return list(set(stages))

	def _get_stages_with_exit_status(self, value):
		"""
		Return a list of stage names for jobs that have a specified exit status.

		Args:
			value (str): run status to consider

		Returns:
			int: stages with requested run status
		"""

		stages = []
		for group in self.submitted_jobs:
			for job in self.submitted_jobs[group]:
				if job.exit_status and job.exit_status['completion_status'] == value:
					stages.append(job.name)
		return list(set(stages))

	def _get_run_status_count(self, value):
		"""
		Enumerate the number of jobs with a given value for run_status

		Args:
			value (str): value to enumerate

		Returns:
			int: count of jobs with run_status == value

		"""
		count = 0

		for group in self.submitted_jobs:
			for job in self.submitted_jobs[group]:
				if job.run_state == value:
					count += 1

		return count

	def _get_completion_status_count(self, value):
		"""
		Enumerate the number of jobs with a given value for completion status (in exit_status dict)

		Args:
			value (str): value to enumerate

		Returns:
			int: count of jobs with completion_status == value

		"""
		count = 0

		for group in self.completed_jobs:
			for job in self.completed_jobs[group]:
				if job.exit_status and job.exit_status['completion_status'] == value:
					count += 1

		for group in self.submitted_jobs:
			for job in self.submitted_jobs[group]:
 				if job.exit_status and job.exit_status['completion_status'] == value:
 					count += 1

		return count

	def _get_failed_stages(self):
		"""
		Helper function to get a list of stages that have failed entirely.
		"""
		failed_stages = []

		for group in self.submitted_jobs:
			failed_jobs = []

			for job in self.submitted_jobs[group]:
				if job.exit_status and (job.exit_status['completion_status'] == FAILED or job.exit_status['completion_status'] == COMPLETE_MISSING_OUTPUTS or job.exit_status['completion_status'] == FAILED_DEPENDENCY):
					failed_jobs.append(job)

			if len(failed_jobs) == len(self.submitted_jobs[group]):
				failed_stages.append(group)

		return failed_stages

	def _get_finished_stages(self):
		"""
		Helper function to get a list of stages that have failed entirely.
		"""
		finished_stages = []

		for group in self.submitted_jobs:
			finished_jobs = []

			for job in self.submitted_jobs[group]:
				if job.run_state == FINISHED:
					finished_jobs.append(job)

			if len(finished_jobs) == len(self.submitted_jobs[group]):
				finished_stages.append(group)

		return finished_stages


	def write_report(self, filename):
		"""
		Save a report of the last call to run_jobs to specified file. Also saved to temp directory
		by default after call to run_jobs. Useful for understanding if jobs failed and why and viewing
		basic performance statistics.

		Args:
			filename (str): Filename for report.

		Modifies:
			Writes report to file.

		"""

		if not self.completed_jobs and not self.skipped_jobs:
			raise ValueError('No completed jobs to report. Make sure to call run() function prior to writing report.')

		with open(filename, 'w') as report:
			report.write('\t'.join(['jobid', 'stage', 'status', 'was_aborted', 'exit_status', 'memory_request', 'max_vmem_gb', 'duration_hms', 'log_file', 'command']) + '\n')

			# Report on both scheduled and skipped jobs
			report_jobs = copy.deepcopy(self.completed_jobs)
			report_jobs['skipped'] = self.skipped_jobs

			for group in report_jobs:
				jobs = sorted(report_jobs[group], key=lambda x: x.exit_status.get('completion_status', x.name))
				for job in jobs:

					job_id = job.id
					command = str(job.command)
					memory_request = str(job.memory)

					# If job was not scheduled, just put in dummy entry
					if not job_id and not job.exit_status:
						aborted = 'NA'
						completion_status = SKIPPED
						exit_status = 'NA'
						max_vmem_gb = 'NA'
						duration = 'NA'
						log_file = 'NA'
					elif job.exit_status['completion_status'] == FAILED_DEPENDENCY or job.exit_status['completion_status'] == KILLED_BY_USER:
						# Exit status has a bunch of default values in this case just put in fillers
						aborted = 'True'
						completion_status = job.exit_status['completion_status']
						exit_status = 'NA'
						max_vmem_gb = 'NA'
						duration = 'NA'
						log_file = 'NA'
					else:
						job_exit_info = job.exit_status
						aborted = str(job_exit_info['wasAborted'])
						completion_status = str(job_exit_info['completion_status'])
						exit_status = str(job_exit_info['exitStatus'])
						max_vmem_gb = str(float(job_exit_info['resourceUsage']['maxvmem']) / 10e9)
						duration = str(datetime.timedelta(milliseconds=int(float(job_exit_info['resourceUsage']['end_time']) - float(job_exit_info['resourceUsage']['start_time']))))
						log_file = ','.join(glob(os.path.join(self.temp_directory, '*%s' % job.id)))

					# Construct output
					entries = [str(job_id), job.name, completion_status, aborted, exit_status, memory_request, max_vmem_gb, duration, log_file, command]
					report.write('\t'.join(entries) + '\n')

	def _get_job_array_helper_path(self):
		"""
		Gets path to job array helper script.
		"""
		return os.path.join(self.temp_directory, 'job_array_helper.csh')

	def _submit_arrayjob(self, sublist, queue=None):
		"""
		Submits a list of commands as a job array.

		Args:
			sublist (list of dict): list of command dicts to run.

		Returns:
			list of str: list of job IDs

		"""
		if len(sublist) == 0:
			raise ValueError('No commands specified. Must have at least one command.')

		# Sanity check user specified dependencies for this stage
		dependencies = list(set([tuple(job.dependencies) for job in sublist]))

		if len(dependencies) != 1:
			raise ValueError('Multiple dependencies specified for same jobname: %s.' % str(dependencies))

		# Construct native spec for job
		nativeSpecification = '-V -cwd -e %s -o %s -l mfree=%s,h_rt=%s' % (self.temp_directory, self.temp_directory, sublist[0].memory, sublist[0].walltime)
		
		if queue:
			nativeSpecification += ' -q %s' % queue

		# Submit job array of all commands in this stage
		commands = [job.command for job in sublist]

		_,file_name = tempfile.mkstemp(dir=self.temp_directory)
		temp = open(file_name, 'w')
		temp.write('\n'.join(commands) + '\n')
		temp.close()

		jt = self.session.createJobTemplate()
		jt.remoteCommand = '%s %s' % (self._get_job_array_helper_path(), file_name)
		jt.jobName = sublist[0].name
		jt.nativeSpecification = nativeSpecification

		# Submit and set job ids
		jobids = self.session.runBulkJobs(jt, 1, len(commands), 1)

		for job, id in zip(sublist, jobids):
			job.set_id(id)

		# Add job templates
		self.job_templates.append(jt)

		return sublist
