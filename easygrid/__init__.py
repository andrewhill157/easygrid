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
import array_job_helper

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

# Completion status definitions
FAILED = 'FAILED'
COMPLETE = 'COMPLETE'
KILLED_BY_USER = 'KILLED_BY_USER'
FAILED_DEPENDENCY = 'FAILED_DEPENDENCY'
SYSTEM_FAILED = 'SYSTEM_FAILED'
COMPLETE_MISSING_OUTPUTS = 'COMPLETE_MISSING_OUTPUTS'

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
		self.submitted_jobs = {}
		self.complete = False
		self.skipped_jobs = []
		self.failed_stages = []
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
		self.submitted_jobs = {}
		self.complete = False
		self.skipped_jobs = []
		self.failed_stages = []
		self.job_templates = []

	def set_temp_directory(self, path):
		"""
		Setter for the temp directory for pipeline runs.
		"""
		os.path.abspath(temp_directory)

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

	def run(self, monitor=True, logging=True, dry=False):
		"""
		After adding jobs with add_jobs, this function executes them as a pipeline on Grid Engine.

		Modifies:
			Prints status to screen periodically as jobs change status.
			Saves job report to temp directory on completion.

		"""
		# Error if no jobs added
		if not self.joblist and not self.skipped_jobs:
			raise ValueError('No jobs added. Must call add_jobs prior to running jobs.')

		# Make temp directory for job files and reports
		self._prep_temp_dir()

        # Write out Job Array helper script to temp directory
		self._write_job_array_helper()

		# Sort jobs in required order by dependency
		self.joblist = _topological_sort(self.joblist)

		# Submit each group of jobs as an array (or perform a dry run)
		if dry:
			print('DRY RUN: would skip %s jobs that already have outputs present...' % len(self.skipped_jobs))

		for group, joblist in itertools.groupby(self.joblist, key=lambda x: x.name):
			joblist = list(joblist)

			if dry:
				print(self._get_group_dry_run_message(group, joblist))

				for job in joblist:
					print(self._get_job_dry_run_message(job))
			else:
				joblist = self._submit_arrayjob(joblist)
				self.submitted_jobs[group] = joblist

		if not dry and monitor:
			self.monitor(logging=logging)

	def _write_job_array_helper(self):
		"""
		Writes a bash script to temp folder to facilitate array job submission
		"""
		with open(self._get_job_array_helper_path(), 'w') as helper_script:
			helper_script.write(array_job_helper.ARRAY_JOB_SCRIPT)
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
		try:
			jobstatus = self.session.jobStatus(job.id)
		except:
			jobstatus = drmaa.JobState.DONE

		return jobstatus

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

			if exit_status['exitStatus'] == 0:
				if job.outputs_exist():
					exit_status['completion_status'] = COMPLETE
				else:
					exit_status['completion_status'] = COMPLETE_MISSING_OUTPUTS
			elif exit_status['wasAborted'] or exit_status['terminatedSignal'] or exit_status['hasCoreDump']:
				exit_status['completion_status'] = SYSTEM_FAILED
			else:
				exit_status['completion_status'] = FAILED	

		except:
			# In cases where jobs are killed by user, wait will fail, so need to catch
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
				if job.exit_status and job.exit_status['completion_status'] == FAILED:
					failed_jobs.append(job)
			
			if len(failed_jobs) == len(self.submitted_jobs[group]):
				failed_stages.append(group)

		return failed_stages

	def _get_dependents_of_stage(self, stages):
		"""
		Helper function to get a list of jobs that belong to or are dependent on a list of stages.

		Args:
			stages: list of stages to consider

		Returns:
			list of Job: list of jobs that belong to or are dependent on a list of stages.

		"""
		stages = set(stages)
		dependent_jobs = []

		for group in self.submitted_jobs:
			if group in stages:
				for job in self.submitted_jobs[group]:
					if not job.exit_status:
						dependent_jobs.append(job)
			else:
				# Do a first pass to make sure you get all the chained dependencies
				for job in self.submitted_jobs[group]:
					for dependency in job.dependencies:
						if dependency in stages:
							stages.add(group)

		# Do a second pass actually flag all the jobs that need to be killed
		for group in self.submitted_jobs:
			for job in self.submitted_jobs[group]:	
				if True in [dependency in stages for dependency in job.dependencies]:
					dependent_jobs.append(job)

		return dependent_jobs

	def _kill_jobs(self, joblist):
		"""
		Helper function to kill a list of Jobs.
		"""
		for job in joblist:
			try:
				self.session.control(job.id, drmaa.JobControlAction.TERMINATE)
			except:
				print('WARNING: tried to kill job but does not exist (user may have deleted prior to termination): %s' % job.id)

			exit_status = {'hasExited': 'NA',
				'hasSignal': 'NA',
				'terminatedSignal': 'NA',
				'hasCoreDump': 'NA',
				'wasAborted': 'NA',
				'exitStatus': 'NA',
				'resourceUsage': 'NA',
				'completion_status': FAILED_DEPENDENCY}	

			job.set_exit_status(exit_status)			

	def monitor(self, logging=True):
		"""
		Once jobs have been submitted via run_jobs, monitor their progress and log to screen.

		Args:
			logging (bool): True if want to log messages to screen and False otherwise.

		"""

		if not self.submitted_jobs:
			raise ValueError('No jobs have been submitted with run_jobs. Cannot monitor.')

		# Log skipped jobs if there are any
		if len(self.skipped_jobs) > 0:
			LOGGER.info('Skipping %s jobs because specified outputs already present...' % len(self.skipped_jobs))

		last_log = None

		while True:
			for group, joblist in self.submitted_jobs.items():
				for job in joblist:
					jobstatus = self._get_run_pass_fail(job)
					
					if  jobstatus != drmaa.JobState.DONE and jobstatus != drmaa.JobState.FAILED:
						if jobstatus == drmaa.JobState.RUNNING:
							job.set_run_state(RUNNING)
						else:
							job.set_run_state(PENDING)
					else:
						job.set_run_state(FINISHED)
						job.set_exit_status(self._get_exit_status(job))

			total_running = self._get_run_status_count(RUNNING)
			total_pending = self._get_run_status_count(PENDING)
			total_failed = self._get_completion_status_count(FAILED)
			total_system_failed = self._get_completion_status_count(SYSTEM_FAILED) + self._get_completion_status_count(KILLED_BY_USER)
			total_complete = self._get_completion_status_count(COMPLETE)
			log_message = '%s jobs running\t%s jobs pending\t%s jobs completed\t%s jobs failed\t %s system/user failures\r' % (total_running, total_pending, total_complete, total_failed, total_system_failed)

			# Only log when status has changed and when requested
			if logging and (last_log != log_message):
				last_log = log_message				
				LOGGER.info(log_message)

			# Check if any stages have failed entirely
			failed_stages = self._get_failed_stages()
			new_failed_stages = [stage for stage in failed_stages if stage not in self.failed_stages]

			# Kill dependent stages of any entirely failed stages
			if new_failed_stages:
				jobs_to_kill = self._get_dependents_of_stage(new_failed_stages)
				killed_stages = list(set([job.name for job in jobs_to_kill]))
				
				for stage in killed_stages:
					self.failed_stages.extend(new_failed_stages)
					self.failed_stages.extend(killed_stages)

				LOGGER.info('Stages failed: %s; Killing dependent stages: %s' % (', '.join(new_failed_stages), ', '.join(killed_stages)))
				self._kill_jobs(jobs_to_kill)

			time.sleep(1)
			
			# Check to see if all jobs have completed
			if total_running + total_pending == 0:
				break

		# Write out the report of logging results
		self.complete = True
		self.write_report(os.path.join(self.temp_directory, 'job_report.txt'))

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

		if not self.complete:
			raise ValueError('Job status unknown. Must call monitor() function prior to writing job report.')

		with open(filename, 'w') as report:
			report.write('\t'.join(['jobid', 'stage', 'status', 'was_aborted', 'exit_status', 'memory_request', 'max_vmem_gb', 'duration_hms', 'log_file', 'command']) + '\n')
			
			# Report on both scheduled and skipped jobs
			report_jobs = copy.deepcopy(self.submitted_jobs)
			report_jobs['skipped'] = self.skipped_jobs

			for group in report_jobs:
				jobs = sorted(report_jobs[group], key=lambda x: x.exit_status.get('completion_status', x.name))
				for job in jobs:
					
					job_id = job.id
					command = str(job.command)
					memory_request = str(job.memory)

					# If job was not scheduled, just put in dummy entry
					if not job_id:
						aborted = 'NA'
						completion_status = 'SKIPPED'
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
						log_file = ','.join(glob(os.path.join(self.temp_directory, '*%s*' % job.id)))

					# Construct output
					entries = [str(job_id), job.name, completion_status, aborted, exit_status, memory_request, max_vmem_gb, duration, log_file, command]
					report.write('\t'.join(entries) + '\n')

	def _get_job_array_helper_path(self):
		"""
		Gets path to job array helper script.
		"""
		return os.path.join(self.temp_directory, 'job_array_helper.csh')

	def _submit_arrayjob(self, sublist):
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
	
		# Make string for native spec reflecting dependencies
		if dependencies[0]:
			dependency_string = ' -hold_jid %s' % ','.join(dependencies[0])
		else:
			dependency_string = ''

		# Construct native spec for job
		nativeSpecification = '-V -cwd -e %s -o %s -l mfree=%s,h_rt=%s%s' % (self.temp_directory, self.temp_directory, sublist[0].memory, sublist[0].walltime, dependency_string)

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
