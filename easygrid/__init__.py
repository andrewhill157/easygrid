import drmaa
import os
import logging
import time
import sys
import itertools
import tempfile
import re
import collections
from glob import glob
import .array_job_helper

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
    graph_unsorted = list(set([(job['name'], tuple(job['dependencies'])) for job in joblist]))

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
            raise RuntimeError("A cyclic dependency occurred")

    # Sort job list according to topological ordering
    order = dict([(job[0], i) for i,job in enumerate(graph_sorted)])
    joblist = sorted(joblist, key=lambda x: order[x['name']])
    return joblist

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
		self.temp_directory = temp_directory
		self.jobreport = None

	def add_job(self, command, name='default', memory='1G', walltime='100:00:00', dependencies=[]):
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
		self.joblist.append({'command': command, 'memory': memory, 'walltime': walltime, 'dependencies': dependencies, 'name': name})

	def run_jobs(self):
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
		if not os.path.exists(self.temp_directory):
			os.makedirs(self.temp_directory)

		# Sort jobs in required order by dependency
		self.joblist = topological_sort(self.joblist)

		# Submit each group of jobs as an array
		submitted_jobs = []
		with drmaa.Session() as session:

			status_dict = collections.OrderedDict()

			for group, joblist in itertools.groupby(self.joblist, key=lambda x: x['name']):
				joblist = list(joblist)
				submitted_jobs.append((group, self._submit_arrayjob(joblist, session)))
				status_dict[group] = {}

			# Monitor jobs
			current_jobs = submitted_jobs
			initial_log = True

			while True:

				new_current_jobs = []

				for group, joblist in current_jobs:
					running_count = 0
					pending_count = 0
					completed_jobs = []
					failed_jobs = []
					system_failed = []
					group_current_jobs = []

					for job in joblist:

						try:
							jobstatus = session.jobStatus(job)
						except:
							jobstatus = drmaa.JobState.DONE

						
						if  jobstatus != drmaa.JobState.DONE and jobstatus != drmaa.JobState.FAILED:
							group_current_jobs.append(job)

							if jobstatus == drmaa.JobState.RUNNING:
								running_count += 1
							else:
								pending_count += 1
						else:
							jobid = job
							exit_status = session.wait(jobid, drmaa.Session.TIMEOUT_WAIT_FOREVER)
							
							exit_status = {'id': job,
							'group': group,
							'hasExited': exit_status.hasExited,
							'hasSignal': exit_status.hasSignal,
							'terminatedSignal': exit_status.terminatedSignal,
							'hasCoreDump': exit_status.hasCoreDump,
							'wasAborted': exit_status.wasAborted,
							'exitStatus': exit_codes.get(exit_status.exitStatus, 'unknown exit code'),
							'resourceUsage': exit_status.resourceUsage}

							if exit_status['exitStatus'] == 0:
								completed_jobs.append(exit_status)
							elif exit_status['wasAborted'] or exit_status['terminatedSignal'] or exit_status['hasCoreDump']:
								system_failed.append(exit_status)	
							else:
								failed_jobs.append(exit_status)

					new_current_jobs.append((group, group_current_jobs))

					status_dict[group]['running_count'] = running_count
					status_dict[group]['pending_count'] = pending_count

					group_completed_jobs = status_dict[group].get('completed_jobs', [])
					group_completed_jobs.extend(completed_jobs)
					status_dict[group]['completed_jobs'] = group_completed_jobs

					group_completed_jobs = status_dict[group].get('system_failed', [])
					group_completed_jobs.extend(system_failed)
					status_dict[group]['system_failed'] = group_completed_jobs

					group_completed_jobs = status_dict[group].get('failed_jobs', [])
					group_completed_jobs.extend(failed_jobs)
					status_dict[group]['failed_jobs'] = group_completed_jobs


				if current_jobs != new_current_jobs or initial_log:
					initial_log = False
					current_jobs = new_current_jobs

					total_running = 0
					total_pending = 0
					total_failed = 0
					total_system_failed = 0
					total_complete = 0

					# Generate status string
					for group in status_dict:
						status = status_dict[group]
						total_running += status['running_count']
						total_pending += status['pending_count']
						total_failed += len(status['failed_jobs'])
						total_system_failed += len(status['system_failed'])
						total_complete += len(status['completed_jobs'])

					LOGGER.info('%s jobs running\t%s jobs pending\t%s jobs completed\t%s jobs failed\t %s system failed\r' % (total_running, total_pending, total_complete, total_failed, total_system_failed))
				
				time.sleep(1)	
				
				# Check to see if all jobs have completed
				if sum([len(x[1]) for x in new_current_jobs]) == 0:
					break

		# Remove temp files
		for file in glob(os.path.join(self.temp_directory, 'tmp*')):
			os.remove(file)

		# Stash the report and save to temp dir
		self.jobreport = status_dict
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
		if not self.jobreport:
			raise ValueError('No job report present. Must call run_jobs prior to report generation.')

		with open(filename, 'w') as report:
			for group in self.jobreport:
				for job in self.jobreport[group]['failed_jobs']:
					entries = [job['id'], job['group'], str(job['wasAborted']), str(job['exitStatus']), str(job['resourceUsage']['maxvmem'])]
					report.write('\t'.join(entries) + '\n')

	def clear(self):
		"""
		Clear any current added jobs and job reports.
		"""
		self.joblist = []
		self.jobreport = None

	def _get_job_array_helper_path(self):
		"""
		Gets path to job array helper script.
		"""
		return os.path.join(self.temp_directory, 'job_array_helper.sh')

	def _submit_arrayjob(self, sublist, session):
		"""
		Submits a list of commands as a job array to the specified session.

		Args:
			sublist (list of dict): list of command dicts to run.
			session (drmaa.Session): DRMAA session to submit to

		Returns:
			list of str: list of job IDs

		"""
		if len(sublist) == 0:
			raise ValueError('No commands specified. Must have at least one command.')
			
		# Sanity check user specified dependencies for this stage
		dependencies = list(set([tuple(job['dependencies']) for job in sublist]))

		if len(dependencies) != 1:
			raise ValueError('Multiple dependencies specified for same jobname: %s.' % str(dependencies))
	
		# Write out Job Array helper script to temp directory
		with open(self.get_job_array_helper_path(), 'w') as helper_script:
			helper_script.write(ARRAY_JOB_SCRIPT)

		# Make string for native spec reflecting dependencies
		if dependencies[0]:
			dependency_string = ' -hold_jid %s' % ','.join(dependencies[0])
		else:
			dependency_string = ''

		# Construct native spec for job
		nativeSpecification = '-V -cwd -l mfree=%s,h_rt=%s%s' % (sublist[0]['memory'], sublist[0]['walltime'], dependency_string)

		# Submit job array of all commands in this stage
		commands = [job['command'] for job in sublist]

		_,file_name = tempfile.mkstemp(dir=self.temp_directory)
		temp = open(file_name, 'w')
		temp.write('\n'.join(commands) + '\n')
		temp.close()

		jt = session.createJobTemplate()	
		jt.remoteCommand = '%s %s' % (self._get_job_array_helper_path(), file_name)
		jt.jobName = sublist[0]['name']
		jt.nativeSpecification = nativeSpecification

		jobids = session.runBulkJobs(jt, 1, len(commands), 1)
		
		return jobids			
