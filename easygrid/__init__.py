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
import array_job_helper

# Set up a basic logger
LOGGER = logging.getLogger('something')
myFormatter = logging.Formatter('%(asctime)s: %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(myFormatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.DEBUG)
myFormatter._fmt = "[JOBMANAGER]: " + myFormatter._fmt

# Table to translate common error codes into more useful text
exit_codes = {
	127: 'command not found',
	126: 'command invoked cannot execute',
	2: 'misuse of shell builtins',
	1: 'general error',
	0: 0
}

def topological_sort(joblist):
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

	def __init__(self, command, name, dependencies=[], memory='1G', walltime='10:00:00'):
		self.command = command
		self.memory = memory
		self.walltime = walltime
		self.dependencies = dependencies
		self.name = name
		self.id = id
		self.exit_status = None
		self.run_state = None
	
	def set_id(self, id):
		self.id = id

	def set_exit_status(self, exit_status):
		self.exit_status = exit_status

	def set_run_state(self, state):
		self.run_state = state


class JobManager:
	"""
	Class for constructing and executing simple pipelines of jobs on distributed computing systems
	where different stages of the pipeline are dependent on one another.
	"""
	
	def __init__(self, temp_directory='.jobmanager'):
		"""
		Constructor

		Args:
			temp_directory: A temporary directory for job manager output (temp files, job reports, etc.)

		"""
		self.joblist = []
		self.temp_directory = os.path.abspath(temp_directory)
		self.jobreport = None
		self.session = drmaa.Session()
		self.session.initialize()
		self.submitted_jobs = {}
		self.complete = False

	def __del__(self):
		# Remove temp files
		for file in glob(os.path.join(self.temp_directory, 'tmp*')):
			os.remove(file)

		# Close the DRMAA session
		self.session.exit()

		# Remove bash helper script
		if os.path.exists(self._get_job_array_helper_path()):
			os.remove(self._get_job_array_helper_path())

	def add_job(self, command, name, dependencies=[], memory='1G', walltime='10:00:00'):
		"""
		Adds a command to be run as a job in the job manager.
		Must be called at least once prior to calling run_jobs.

		Args:
			command (str): Command to be run in terminal for this job.
			name (str): a name to identify the stage of the pipeline this job belongs to
			memory (str): memory request for job such as '1G' 
			walltime (str): wall time request such as '100:00:00'
			dependencies (list of str): a list of names for other stages that this job is dependent on.

		"""
		self.joblist.append(Job(command, name, dependencies=dependencies, memory=memory, walltime=walltime))

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
		Make the temporary directory needed for JobManager job executtion.
		If exists, clear out old files.
		"""
		#
		if not os.path.exists(self.temp_directory):
			os.makedirs(self.temp_directory)

		# Clean any existing files
		for file in glob(os.path.join(self.temp_directory, '*')):
			os.remove(file)

	def run_jobs(self, monitor=True, logging=True):
		"""
		After adding jobs with add_jobs, this function executes them as a pipeline on Grid Engine.

		Modifies:
			Prints status to screen periodically as jobs change status.
			Saves job report to temp directory on completion.

		"""
		# Error if no jobs added
		if not self.joblist:
			raise ValueError('No jobs added. Must call add_jobs prior to running jobs.')

		# Make temp directory for job files and reports
		self._prep_temp_dir()

        # Write out Job Array helper script to temp directory
		self._write_job_array_helper()

		# Sort jobs in required order by dependency
		self.joblist = topological_sort(self.joblist)

		# Submit each group of jobs as an array
		for group, joblist in itertools.groupby(self.joblist, key=lambda x: x.name):
			joblist = list(joblist)
			joblist = self._submit_arrayjob(joblist)

			self.submitted_jobs[group] = joblist

		if monitor:
			self.monitor_jobs(logging=logging)


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

		exit_status = self.session.wait(job.id, drmaa.Session.TIMEOUT_WAIT_FOREVER)

		exit_status = {'hasExited': exit_status.hasExited,
		'hasSignal': exit_status.hasSignal,
		'terminatedSignal': exit_status.terminatedSignal,
		'hasCoreDump': exit_status.hasCoreDump,
		'wasAborted': exit_status.wasAborted,
		'exitStatus': exit_codes.get(exit_status.exitStatus, 'unknown exit code: %s' % exit_status.exitStatus),
		'resourceUsage': exit_status.resourceUsage}

		if exit_status['exitStatus'] == 0:
			exit_status['completion_status'] = 'COMPLETE'
		elif exit_status['wasAborted'] or exit_status['terminatedSignal'] or exit_status['hasCoreDump']:
			exit_status['completion_status'] = 'SYSTEM_FAILED'
		else:
			exit_status['completion_status'] = 'FAILED'

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

	def monitor_jobs(self, logging=True):
		"""
		Once jobs have been submitted via run_jobs, monitor their progress and log to screen.

		Args:
			logging (bool): True if want to log messages to screen and False otherwise.

		"""

		if not self.submitted_jobs:
			raise ValueError('No jobs have been submitted with run_jobs. Cannot monitor.')

		last_log = None

		while True:
			for group, joblist in self.submitted_jobs.items():
				for job in joblist:
					jobstatus = self._get_run_pass_fail(job)
					
					if  jobstatus != drmaa.JobState.DONE and jobstatus != drmaa.JobState.FAILED:
						if jobstatus == drmaa.JobState.RUNNING:
							job.set_run_state('RUNNING')
						else:
							job.set_run_state('PENDING')
					else:
						job.set_run_state('FINISHED')
						job.set_exit_status(self._get_exit_status(job))

			total_running = self._get_run_status_count("RUNNING")
			total_pending = self._get_run_status_count("PENDING")
			total_failed = self._get_completion_status_count("FAILED")
			total_system_failed = self._get_completion_status_count("SYSTEM_FAILED")
			total_complete = self._get_completion_status_count("COMPLETE")
			log_message = '%s jobs running\t%s jobs pending\t%s jobs completed\t%s jobs failed\t %s system failed\r' % (total_running, total_pending, total_complete, total_failed, total_system_failed)

			# Only log when status has changed and when requested
			if logging and (last_log != log_message):
				last_log = log_message				
				LOGGER.info(log_message)
			
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
			for group in self.submitted_jobs:
				jobs = sorted(self.submitted_jobs[group], key=lambda x: x.exit_status['completion_status'])
				for job in jobs:
					job_exit_info = job.exit_status
					job_id = job.id
					aborted = str(job_exit_info['wasAborted'])
					completion_status = str(job_exit_info['completion_status'])
					exit_status = str(job_exit_info['exitStatus'])
					max_vmem_gb = str(float(job_exit_info['resourceUsage']['maxvmem']) / 10e9)
					duration = str(datetime.timedelta(milliseconds=int(float(job_exit_info['resourceUsage']['end_time']) - float(job_exit_info['resourceUsage']['start_time']))))
					command = job.command
					log_file = ','.join(glob(os.path.join(self.temp_directory, '*%s*' % job.id)))
					memory_request = job.memory

					entries = [job_id, group, completion_status, aborted, exit_status, memory_request, max_vmem_gb, duration, log_file, command]
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
		
		return sublist