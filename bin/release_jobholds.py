#!/usr/bin/python

import sys


from vsc.jobs.moab.internal import MoabCommand
from vsc.jobs.moab.showq import Showq
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.cache import FileCache
from vsc.utils.fancylogger import getLogger, logToScreen, setLogLevelInfo
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import SimpleNagios
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

# Constants
NAGIOS_HEADER = 'release_jobholds'
NAGIOS_CHECK_FILENAME = '/var/cache/icinga/%s.nagios.json.gz' % NAGIOS_HEADER
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

RELEASEJOB_CACHE_FILE = '/var/cache/%s.json.gz' % NAGIOS_HEADER
RELEASEJOB_LOCK_FILE = '/var/run/%s.lock' % NAGIOS_HEADER

RELEASEJOB_LIMITS = {
    # jobs in hold per user (maximum of all users)
    'peruser_warning': 10,
    'peruser_critical': 20,
    # total number of jobs in hold
    'total_warning': 50,
    'total_critical': 100,
    # per job release attempts (maximum of all jobs)
    'release_warning': 50,
    'release_critical': 70,
}

RELEASEJOB_SUPPORTED_HOLDTYPES = ('BatchHold',)

_log = getLogger(__name__, fname=False)
logToScreen(True)
setLogLevelInfo()

def process_hold(clusters, dry_run=False):
    """Process a filtered queueinfo dict"""
    releasejob_cache = FileCache(RELEASEJOB_CACHE_FILE)

    # get the showq data
    for hosts, data in clusters.items():
        data['path'] = data['spath']  # showq path
    showq = Showq(clusters, cache_pickle=True)
    (queue_information, reported_hosts, failed_hosts) = showq.get_moab_command_information()

    # release the jobs, prepare the command
    m = MoabCommand(cache_pickle=False, dry_run=dry_run)
    for hosts, data in clusters.items():
        data['path'] = data['mpath']  # mjobctl path
    m.clusters = clusters

    # read the previous data
    ts_data = releasejob_cache.load('queue_information')
    if ts_data is None:
        oldts = 0
        old_queue_information = {}
    else:
        (oldts, old_queue_information) = ts_data

    stats = {
        'peruser': 0,
        'total': 0,
        'release': 0,
    }

    release_jobids = []

    for user, clusterdata in queue_information.items():
        oldclusterdata = old_queue_information.setdefault(user, {})
        totaluser = 0
        for cluster, data in clusterdata.items():
            olddata = oldclusterdata.setdefault(cluster, {})
            # DRMJID is supposed to be unique
            # get all oldjobids in one dict
            oldjobs = dict([(j['DRMJID'], j['_release']) for jt in olddata.values() for j in  jt])
            for jobtype, jobs in data.items():
                removeids = []
                for idx, job in enumerate(jobs):
                    jid = job['DRMJID']

                    if jobtype in RELEASEJOB_SUPPORTED_HOLDTYPES:
                        totaluser += 1
                        release = max(oldjobs.get(jid, 0), 0) + 1
                        job['_release'] = release
                        stats['release'] = max(stats['release'], release)
                        release_jobids.append(jid)
                        # release the job
                        cmd = [m.clusters[cluster]['path'], '-u', jid]
                        _log.info("Releasing job %s cluster %s for the %s-th time." % (jid, cluster, release))
                        if dry_run:
                            _log.info("Dry run %s" % cmd)
                        else:
                            m._run_moab_command(cmd, cluster, [])
                    else:
                        # keep historical data, eg a previously released job could be idle now
                        # but keep the counter in case it gets held again
                        try:
                            release = oldjobs[jid]
                            job['_release'] = release
                        except KeyError:
                            # not previously in hold, remove it
                            removeids.append(idx)

                # remove the jobs (in reverse order)
                for remove_idx in removeids[::-1]:
                    jobs.pop(remove_idx)

                # cleanup
                if len(jobs) == 0:
                    data.pop(jobtype)
            # cleanup
            if len(data) == 0:
                clusterdata.pop(cluster)
        # cleanup
        if len(clusterdata) == 0:
            queue_information.pop(user)

        # update stats
        stats['peruser'] = max(stats['peruser'], totaluser)
        stats['total'] += totaluser

    _log.info("Release statistics: total jobs in hold %(total)s; max in hold per user %(peruser)s; max releases per job %(release)s" % stats)

    # update and close
    releasejob_cache.update('queue_information', queue_information, 0)
    releasejob_cache.close()

    return release_jobids, stats

def get_queue_information(clusters):
    """Get the queue information from the cluster(s). Remove unsupported jobtypes"""

    # santize by removing all unsupported jobtypes
    for user, clusterdata in queue_information.items():
        for cluster, data in clusterdata.items():
            for jobtype in data.keys():
                if not jobtype in RELEASEJOB_SUPPORTED_HOLDTYPES:
                    data.pop(jobtype)
            if not data:
                clusterdata.pop(cluster)
        if not clusterdata:
            queue_information.pop(user)

    return queue_information

def main():
    """Main function"""
    options = {
        'nagios_check_filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios_check_interval_threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'location': ('the location for storing the pickle file: gengar, muk', str, 'store', 'gengar'),
        'ha': ('high-availability master IP address', None, 'store', None),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    }

    opts = simple_option(options)

    nag = SimpleNagios(_cache=NAGIOS_CHECK_FILENAME)

    if opts.options.ha and not proceed_on_ha_service(opts.options.ha):
        _log.info("Not running on the target host in the HA setup. Stopping.")
        nag.ok("Not running on the HA master.")
    else:
        # parse config file
        clusters = {}
        for host in opts.options.hosts:
            master = opts.configfile_parser.get(host, "master")
            showq_path = opts.configfile_parser.get(host, "showq_path")
            mjobctl_path = opts.configfile_parser.get(host, "mjobctl_path")
            clusters[host] = {
                'master': master,
                'spath': showq_path,
                'mpath': mjobctl_path,
            }

        # process the new and previous data
        released_jobids, stats = process_hold(clusters, dry_run=opts.options.dry_run)

        # nagios state
        stats.update(RELEASEJOB_LIMITS)
        stats['message'] = "released %s jobs in hold" % len(released_jobids)
        nag._eval_and_exit(**stats)

    _log.info("Cached nagios state: %s %s" % (nag._final_state[0][1], nag._final_state[1]))

if __name__ == '__main__':
    main()
