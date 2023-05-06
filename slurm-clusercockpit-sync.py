#!/usr/bin/python3
# This script syncs the slurm jobs with the cluster cockpit backend. It uses
# the slurm command line tools to gather the relevant slurm infos and reads
# the corresponding info from cluster cockpit via its api.
#
# After reading the data, it stops all jobs in cluster cockpit which are
# not running any more according to slurm and afterwards it creates all new
# running jobs in cluster cockpit.
#
# -- Michael Schwarz <schwarz@uni-paderborn.de>

import subprocess
import json
import requests
import re
import pyslurm
import logging
import os,time
import psutil

import node_format

class Logger():
    logdir = ""
    logfile = ""

    def __init__(self, logdir):
        self.logdir = logdir
        if not os.path.exists(self.logdir):
            os.mkdir(self.logdir)
        self.logfile = os.path.join(self.logdir,time.strftime("%Y-%m-%d")+".log")

    def generate(self):
        logger = logging.getLogger("cc-slurm-sync")
        logger.setLevel(logging.DEBUG)
        lf = logging.FileHandler(filename=self.logfile,encoding="utf8")
        lf.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")
        lf.setFormatter(formatter)
        logger.addHandler(lf)
        return logger

class CCApi:
    config = {}
    apiurl = ''
    apikey = ''
    headers = {}

    def __init__(self, config, debug=False):
        self.config = config
        self.apiurl = "%s/api/" % config['cc-backend']['host']
        self.apikey = config['cc-backend']['apikey']
        self.headers = { 'accept': 'application/ld+json',
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer %s' % self.config['cc-backend']['apikey']}

    def startJob(self, data):
        url = self.apiurl+"jobs/start_job/"
        r = requests.post(url, headers=self.headers, json=data)
        if r.status_code == 201:
            return r.json()
        else:
            logger.error(data)
            logger.error(r)
            return False

    def stopJob(self, data):
        url = self.apiurl+"jobs/stop_job/"
        r = requests.post(url, headers=self.headers, json=data)
        if r.status_code == 200:
            return r.json()
        else:
            logger.error(data)
            logger.error(r)
            return False

    def getJobs(self, filter_running=True):
        url = self.apiurl+"jobs/"
        if filter_running:
            url = url+"?state=running"
        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            return r.json()
        else:
            return { 'jobs' : []}

class SlurmSync:
    slurmJobData = {}
    slurmDBData = {}
    ccData = {}
    config = {}
    debug = False
    ccapi = None

    def __init__(self, config, debug=False):
        self.config = config
        self.debug = debug

        # validate config
        if "slurm" not in config:
            raise KeyError
        if "cc-backend" not in config:
            raise KeyError
        if "host" not in config['cc-backend']:
            raise KeyError
        if "apikey" not in config['cc-backend']:
            raise KeyError
        if "pidfile" not in config:
            raise KeyError

        self.ccapi = CCApi(self.config, debug)

    def _exec(self, command):
        process = subprocess.Popen(command, stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        output, error = process.communicate()
        if process.returncode is 0:
            return output.decode('utf-8')
        else:
            logger.error(error)
        return ""

    def _readSlurmData(self):
        if self.debug:
            logger.debug("_readSlurmData called")
        slurm_jobs = pyslurm.job().get()
        self.slurmJobData = slurm_jobs

    def _readDBData(self):
        slurm_job_lst = list(self.slurmJobData.keys())
        cc_job_lst = [i['jobId'] for i in self.ccData['jobs']]
        job_lst = slurm_job_lst + cc_job_lst
        addtion_dbdata = pyslurm.slurmdb_jobs().get(jobids=job_lst)
        self.slurmDBData = addtion_dbdata

    def _readCCData(self):
        if self.debug:
            logger.debug("_readCCBackendData called")
        self.ccData = self.ccapi.getJobs()

    def _jobIdInCC(self, jobid):
        for job in self.ccData['jobs']:
            if jobid == job['jobId']:
                return True
        return False

    def _jobRunning(self, jobid):
        job = self.slurmJobData.get(jobid,{})
        if not job:
            return False
        if int(job['job_id']) == int(jobid):
            if job['job_state'] == 'RUNNING':
                return True
        return False

    def _getACCIDsFromGRES(self, gres, nodename):
        ids = self.config['accelerators']

        nodetype = None
        for k, v in ids.items():
            if nodename.startswith(k):
                nodetype = k

        if not nodetype:
            logger.warning("Can't find accelerator definition for node %s" % nodename.strip())
            return []

        # the gres definition might be different on other clusters!
        m = re.match(r"(fpga|gpu):(\w+):(\d)\(IDX:([\d,\-]+)\)", gres)
        if m:
            family = m.group(1)
            type = m.group(2)
            amount = m.group(3)
            indexes = m.group(4)
            acc_id_list = []

            # IDX might be: IDX:0,2-3
            # first split at , then expand the range to individual items
            if len(indexes) > 1:
                idx_list = indexes.split(',')
                idx = []
                for i in idx_list:
                    if len(i) == 1:
                        idx.append(i)
                    else:
                        start = i.split('-')[0]
                        end = i.split('-')[1]
                        idx = idx + list(range(int(start), int(end)+1))
                indexes = idx

            for i in indexes:
                acc_id_list.append(ids[nodetype][str(i)])

            return acc_id_list

        return []

    def _ccStartJob(self, job):
        job_id = job['job_id']
        logger.info("Crate job %s, user %s, name %s" % (job_id, self.slurmDBData[job_id]['user'], job['name']))
        nodelist = self._convertNodelist(job['nodes'])
        # Exclusive job?
        if job['shared'] == "none":
            exclusive = 1
        # exclusive to user
        elif job['shared'] == "user":
            exclusive = 2
        # exclusive to mcs
        elif job['shared'] == "mcs":
            exclusive = 3
        # default is shared node
        else:
            exclusive = 0

        # read job script and environment

        hashdir = "hash.%s" % str(job_id)[-1]
        jobscript_command = 'scontrol write batch_script %s -' % job_id
        try:
            jobscript = self._exec(jobscript_command)
        except:
            jobscript = 'NO JOBSCRIPT'

        environment = ''
        # FIXME sometimes produces utf-8 conversion errors
        environment_filename = "%s/%s/job.%s/environment" % (self.config['slurm']['state_save_location'], hashdir, job_id)
        try:
            with open(environment_filename, 'r', encoding='utf-8') as f:
                environment = f.read()
        except FileNotFoundError:
            environment = 'NO ENV'
        except UnicodeDecodeError:
            environment = 'UNICODE_DECODE_ERROR'

        # truncate environment to 50.000 chars. Otherwise it might not fit into the
        # Database field together with the job script etc.
        environment = environment[:50000]


        # get additional info from slurm and add environment
        add_info = {
            "JobId":job_id,
            "JobName":job["name"],
            "UserId":job["user_id"],
            "GroupId":job["group_id"],
            "Priority":job["priority"],
            "Nice":job["nice"],
            "Account":job["account"],
            "QOS":job["qos"],
            "JobState":job["job_state"],
            "Reason":job["state_reason"],
            "Dependency":job["dependency"],
            "Requeue":job["requeue"],
            "Restarts":job["restart_cnt"],
            "BatchFlag":job["batch_flag"],
            "Reboot":job["reboot"],
            "ExitCode":job["exit_code"],
            "RunTime":job["run_time_str"],
            "TimeLimit":job["time_limit_str"],
            "TimeMin":job["time_min"],
            "AccrueTime":job["accrue_time"],
            "SuspendTime":job["suspend_time"],
            "LastSchedEval":job["last_sched_eval"],
            "Partition":job["partition"],
            "AllocNode:Sid":str(job["alloc_node"])+str(job["alloc_sid"]),
            "ReqNodeList":job["req_nodes"],
            "ExcNodeList":job["exc_nodes"],
            "NodeList":job["nodes"],
            "BatchHost":job["batch_host"],
            "NumNodes":job["num_nodes"],
            "NumCPUs":job["num_cpus"],
            "NumTasks":job["num_tasks"],
            "CPUs/Task":job["cpus_per_task"],
            "TRES":job["tres_req_str"],
            "MinCPUsNode":job["pn_min_cpus"],
            "MinMemoryCPU":job["pn_min_memory"],
            "MinTmpDiskNode":job["pn_min_tmp_disk"],
            "Features":job["features"],
            "Contiguous":job["contiguous"],
            "Licenses":job["licenses"],
            "Network":job["network"],
            "Command":job["command"],
            "WorkDir":job["work_dir"],
            "StdErr":job["std_err"],
            "StdIn":job["std_in"],
            "StdOut":job["std_out"],
            "Power":job["power_flags"]
        }

        slurminfo = str(add_info)
        slurminfo = slurminfo + "ENV:\n====\n" + environment

        # change cluster name manually, if it belongs to cluster2
        if job['partition'] in self.config["cluster_info"]["cluster2"]:
            cluster = "cluster2"
        else:
            cluster = self.slurmDBData[job_id]['cluster']

        # build payload
        data = {'jobId' : job_id,
            'user' : self.slurmDBData[job_id]['user'],
            'cluster' : cluster,
            'numNodes' : job['num_nodes'],
            'numHwthreads' : job['num_cpus'],
            'startTime': job['start_time'],
            'walltime': int(job['time_limit']) * 60,
            'project': job['account'],
            'partition': job['partition'],
            'exclusive': exclusive,
            'resources': [],
            'metadata': {
                'jobName' : job['name'],
                'jobScript' : jobscript,
                'slurmInfo' : slurminfo
                }
            }

        # is this part of an array job?
        if job['array_job_id'] and job['array_job_id'] > 0:
            data.update({"arrayJobId" : job['array_job_id']})

        i = 0
        num_acc = 0
        for node in nodelist:
            # begin dict
            resources = {'hostname' : node.strip()}

            # if a job uses a node exclusive, there are some assigned cpus (used by this job)
            # and some unassigned cpus. In this case, the assigned_cpus are those which have
            # to be monitored, otherwise use the unassigned cpus.
            cores = job['cpus_alloc_layout'][node.strip()]
            resources.update({"hwthreads": cores})

            # Get allocated GPUs if some are requested
            if 'gres_detail' in job and len(job['gres_detail']) > 0:

                gres = job['gres_detail'][i]
                acc_ids = self._getACCIDsFromGRES(gres, node)
                if len(acc_ids) > 0:
                    num_acc = num_acc + len(acc_ids)
                    resources.update({"accelerators" : acc_ids})


            data['resources'].append(resources)
            i = i + 1

        # if the number of accelerators has changed in the meantime, upate this field
        data.update({"numAcc" : num_acc})

        if self.debug:
            logger.debug(data)
        self.ccapi.startJob(data)

    def _ccStopJob(self, jobid):
        logger.info("Stop job %s" % jobid)

        # get search for the jobdata stored in CC
        ccjob = {}
        for j in self.ccData['jobs']:
            if j['jobId'] == jobid:
                ccjob = j

        # check if job is still in squeue data
        job = self.slurmJobData.get(jobid,{})
        if job:
            if job['job_id'] == jobid:
                jobstate = job['job_state'].lower()
                endtime = job['end_time']
                if jobstate == 'requeued':
                    logger.info("Requeued job")
                    jobstate = 'failed'

                    if int(ccjob['startTime']) >= int(job['end_time']):
                        logger.info("squeue correction")
                    # For some reason (needs to get investigated), failed jobs sometimes report
                    # an earlier end time in squee than the starting time in CC. If this is the
                    # case, take the starting time from CC and add ten seconds to the starting
                    # time as new end time. Otherwise CC refuses to end the job.
                        endtime = int(ccjob['startTime']) + 1

        else:
            jobAcctData = self.slurmDBData[jobid]
            jobstate = jobAcctData['state_str'].lower()
            endtime = jobAcctData['end']

            if jobstate == "node_fail":
                jobstate = "failed"
            if jobstate == "requeued":
                logger.info("Requeued job")
                jobstate = "failed"

                if int(ccjob['startTime']) >= int(jobAcctData['time']['end']):
                    logger.info("sacct correction")
                    # For some reason (needs to get investigated), failed jobs sometimes report
                    # an earlier end time in squee than the starting time in CC. If this is the
                    # case, take the starting time from CC and add ten seconds to the starting
                    # time as new end time. Otherwise CC refuses to end the job.
                    endtime = int(ccjob['startTime']) + 1

        data = {
            'jobId' : jobid,
            'cluster' : ccjob['cluster'],
            'startTime' : ccjob['startTime'],
            'stopTime' : endtime,
            'jobState' : jobstate
        }

        self.ccapi.stopJob(data)

    def _convertNodelist(self, nodelist):
        logger.info("nodelist in _convertNodelist: %s" % nodelist)
        # Use slurm to convert a nodelist with ranges into a comma separated list of unique nodes
        if re.search(self.config['node_regex'], nodelist):
            retval = node_format.change_format([nodelist])
            return retval
        else:
            return []


    def sync(self, limit=200, jobid=None, direction='both'):
        if self.debug:
            logger.debug("sync called")
            logger.debug("jobid %s" % jobid)
        self._readSlurmData()
        self._readCCData()
        self._readDBData()

        # check the project state, if the old project si running, exit
        pid_file = self.config["pidfile"]
        if os.path.exists(pid_file):
            with open(pid_file,"r") as f1:
                pid = f1.readline().strip()
                if not pid:
                    pid_exist_state = False
                else:
                    old_pid = int(pid)
                    if psutil.pid_exists(old_pid):
                        pid_exist_state = True
                    else:
                        pid_exist_state = False
                if pid_exist_state:
                    exit()
        now_pid = os.getpid()
        with open(pid_file,"w") as f2:
            f2.write(str(now_pid))

        # Abort after a defined count of sync actions. The intend is, to restart this script after the
        # limit is reached. Otherwise, if many many jobs get stopped, the script might miss some new jobs.
        sync_count = 0

        # iterate over cc jobs and stop them if they have already ended
        if direction in ['both', 'stop']:
            for job in self.ccData['jobs']:
                if jobid:
                    if int(job['jobId']) == int(jobid) and not self._jobRunning(job['jobId']):
                        self._ccStopJob(job['jobId'])
                        sync_count = sync_count + 1
                else:
                    if not self._jobRunning(job['jobId']):
                        self._ccStopJob(job['jobId'])
                        sync_count = sync_count + 1
                if sync_count >= limit:
                    logger.info("sync limit (%s) reached" % limit)
                    break

        sync_count = 0
        # iterate over running jobs and add them to cc if they are still missing there
        if direction in ['both', 'start']:
            for job_id,job in self.slurmJobData.items():
                # Skip this job if the user does not want the metadata of this job to be submitted to ClusterCockpit
                # The text field admin_comment is used for this. We assume that this field contains a comma seperated
                # list of flags.
                if 'admin_comment' in job.keys():
                    if "disable_cc_submission" in str(job['admin_comment']).split(','):
                        logger.info("Job %s: disable_cc_sumbission is set. Continue with next job" % job['job_id'])
                        continue
                # consider only running jobs
                if job['job_state'] == "RUNNING":
                    if jobid:
                        if int(job['job_id']) == int(jobid) and not self._jobIdInCC(job['job_id']):
                            self._ccStartJob(job)
                            sync_count = sync_count + 1
                    else:
                        if not self._jobIdInCC(job['job_id']):
                            self._ccStartJob(job)
                            sync_count = sync_count + 1
                if sync_count >= limit:
                    logger.info("sync limit (%s) reached" % limit)
                    break
        os.remove(pid_file)

if __name__ == "__main__":
    import argparse
    about = """This script syncs the slurm jobs with the cluster cockpit backend. It uses
        the slurm command line tools to gather the relevant slurm infos and reads
        the corresponding info from cluster cockpit via its api.

        After reading the data, it stops all jobs in cluster cockpit which are
        not running any more according to slurm and afterwards it creates all new
        running jobs in cluster cockpit.
        """
    parser = argparse.ArgumentParser(description=about)
    parser.add_argument("-c", "--config", help="Read config file. Default: config.json", default="config.json")
    parser.add_argument("-d", "--debug", help="Enable debug output", action="store_true")
    parser.add_argument("-j", "--jobid", help="Sync this jobid")
    parser.add_argument("-l", "--limit", help="Stop after n sync actions in each direction. Default: 200", default="200", type=int)
    parser.add_argument("-g", "--logdir", help="The directory path of log file.", default="logs")
    parser.add_argument("--direction", help="Only sync in this direction", default="both", choices=['both', 'start', 'stop'])
    args = parser.parse_args()

    logger = Logger(args.logdir).generate()
    # open config file
    if args.debug:
        logger.debug("load config file: %s" % args.config)
    with open(args.config, 'r', encoding='utf-8') as f:
        config = json.load(f)
    if args.debug:
        logger.debug("config file contents:")
        logger.debug(config)
    s = SlurmSync(config, args.debug)
    s.sync(args.limit, args.jobid, args.direction)
