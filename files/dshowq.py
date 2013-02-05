#!/usr/bin/python
##
#
# Copyright 2009-2012 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
The dshowq scripts collects showq information from all Tier-2 clusters and distributes it
in the user's home directory to allow faster lookup.

@author Stijn De Weirdt
@author Andy Georges

It should run on a regular bass to avoid information to become (too) outdated.
"""
# --------------------------------------------------------------------
import cPickle
import grp
import os
import pwd
import sys
import time
from optparse import OptionParser
from subprocess import Popen, PIPE

# --------------------------------------------------------------------
# FIXME: we should move this to use the new fancylogger directly from vsc.utils
import vsc.fancylogger as fancylogger
import vsc.utils.fs_store as store
from lockfile import LockFailed, NotLocked, NotMyLock
from vsc.exceptions import UserStorageError, FileStoreError, FileMoveError
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapGroup, VscLdapUser
from vsc.ldap.filters import InstituteFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_WARNING, NAGIOS_EXIT_CRITICAL
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile, LockFileReadError


#Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/dshowq.nagios.pickle'
NAGIOS_HEADER = 'dshowq'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes
# HostsReported HostsUnavailable UserCount UserNoStorePossible
NAGIOS_REPORT_VALUES_TEMPLATE = "HR=%d, HU=%d, UC=%d, NS=%d"

DSHOWQ_LOCK_FILE = '/var/run/dshowq_tpid.lock'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(False)
fancylogger.setLogLevelInfo()

realshowq = '/usr/bin/showq'
voprefix = 'gvo'

VSC_INSTALL_USER_ID = 'vsc40003'

## all default VOs
defaultvo = 'gvo00012'
novos = ('gvo00012', 'gvo00016', 'gvo00017', 'gvo00018')


def getinfo(res, host, dry_run=False):
    """
    Parse the showq information for the given host .

    This function calls the actual showq command on the target host and parses the resulting
    XML into a python dictionary. A timestamp is added, representing the time at which the data
    was retrieved.

    @type res: dictionary
    @type host: string

    @param res: data we already retrieved for the other hosts
    @param host: the host we target in this call

    @returns res: updated dictionary with the showq information.
    """

    out = get_showq_output(host, dry_run)

    if not out:
        # Failure, do nothing
        logger.error("ERROR: Failed to get output from real showq.")
        return

    res = parseshowqxml(res, host, out)
    res['timeinfo'] = time.time()

    return res


def parseshowqxml(res, host, txt):
    """
    Parse showq --xml output

    @type res: dictionary
    @type host: string

    @param res: current dictionary woth the parsed outut for other hosts
    @param host: the name of the cluster we target

    @returns res: updated dictionary with the showq information for this host.

    <job AWDuration="3931" Account="gvo00000" Class="short" DRMJID="123456788.master.gengar.gent.vsc"
    EEDuration="1278479828" Group="vsc40000" JobID="123456788" JobName="job.sh" MasterHost="node129"
    PAL="gengar" ReqAWDuration="7200" ReqProcs="8" RsvStartTime="1278480000" RunPriority="663"
    StartPriority="663" StartTime="127848000" StatPSDed="31467.120000" StatPSUtl="3404.405600"
    State="Running" SubmissionTime="1278470000" SuspendDuration="0" User="vsc40000">
    <job Account="gvo00000" BlockReason="IdlePolicy" Class="short" DRMJID="1231456789.master.gengar.gent.vsc"
    Description="job 123456789 violates idle HARD MAXIPROC limit of 800 for user vsc40000  (Req: 8  InUse: 800)"
    EEDuration="1278486173" Group="vsc40023" JobID="1859934" JobName="job.sh" ReqAWDuration="7200" ReqProcs="8"
    StartPriority="660" StartTime="0" State="Idle" SubmissionTime="1278480000" SuspendDuration="0" User="vsc40000"></job>
    """
    mand = ['ReqProcs', 'SubmissionTime', 'JobID', 'DRMJID', 'Class']
    running = ['MasterHost']
    idle = []
    blocked = ['BlockReason', 'Description']

    import xml.dom.minidom
    doc = xml.dom.minidom.parseString(txt)

    for j in doc.getElementsByTagName("job"):
        job = {}
        user = j.getAttribute('User')
        state = j.getAttribute('State')
        if not user in res:
            res[user] = {}
        if not host in res[user]:
            res[user][host] = {}
        if not state in res[user][host]:
            res[user][host][state] = []

        for n in mand:
            job[n] = j.getAttribute(n)
            if not job[n]:
                logger.error("Failed to find mandatory name %s in %s" % (n, j.toxml()))
                job.pop(n)
        if state in ('Running'):
            for n in running:
                job[n] = j.getAttribute(n)
                if not job[n]:
                    logger.error("Failed to find running name %s in %s" % (n, j.toxml()))
                    job.pop(n)
        else:
            if j.hasAttribute('BlockReason'):
                if state == 'Idle':
                    ## redefine state
                    state = 'IdleBlocked'
                    if not res[user][host].has_key(state):
                        res[user][host][state] = []
                for n in blocked:
                    job[n] = j.getAttribute(n)
                    if not job[n]:
                        logger.error("Failed to find blocked name %s in %s" % (n, j.toxml()))
                        job.pop(n)
            else:
                for n in idle:
                    job[n] = j.getAttribute(n)
                    if not job[n]:
                        logger.error("Failed to find idle name %s in %s" % (n, j.toxml()))
                        job.pop(n)

        res[user][host][state].append(job)

    return res


def run_and_collect(command):
    """Execute the command and get the resulting stdout and stderr.

    @type command: string

    @param command: the command to execute on the shell.

    @returns: a tuple(output, error) or None when the command fails
    """
    p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, close_fds=True)
    out = ''
    err = ''
    while True:
        try:
            o, e = p.communicate()
            out += o
            err += e
        except:
            break

    if p.returncode == 0:
        return (out, err)
    else:
        return None


def store_pickle_cluster_file(host, output, dry_run=False):
    """Store the result of the showq command in the relevant pickle file.

    @type output: string

    @param output: showq output information
    """
    try:
        if not dry_run:
            store.store_pickle_data_at_user('root', '.showq.pickle.cluster_%s' % (host), output)
        else:
            logger.info("Dry run: skipping actually storing pickle files for cluster data")
    except (UserStorageError, FileStoreError, FileMoveError), err:
        # these should NOT occur, we're root, accessing our own home directory
        logger.critical("Cannot store the out file %s at %s" % ('.showq.pickle.cluster_%s', '/root'))


def load_pickle_cluster_file(host):
    """Load the data from the pickled files.

    @type host: string

    @param host: cluster for which we load data

    @returns: representation of the showq output.
    """
    home = pwd.getpwnam('root')[5]

    if not os.path.isdir(home):
        logger.error("Homedir %s of root not found" % (home))
        return None

    source = "%s/.showq.pickle.cluster_%s" % (home, host)

    try:
        f = open(source)
        out = cPickle.load(f)
        f.close()
        return out
    except Exception, err:
        logger.error("Failed to load pickle from file %s: %s" % (source, err))
        return None


def get_showq_output(host, dry_run=False):

    host_masters = {
        'gengar': 'master2',
        'gastly': 'master3',
        'haunter': 'master5',
        'gulpin': 'master9',
        'dugtrio': 'master11',
        'raichu': 'master13',
        }

    if host in host_masters:
        exe = "%s --xml --host=%s.%s.gent.vsc" % (realshowq, host_masters[host], host)
    elif host:
        exe = "%s --xml" % realshowq
    else:
        logger.error("Unknown host specified: %s" % host)
        sys.exit(0)

    result = run_and_collect(exe)

    if result:
        logger.info("Subprocess %s ran OK, storing resulting data in pickle files" % (exe))
        # create backup of out, in case future showq commands fail
        (out, err) = result
        store_pickle_cluster_file(host, out, dry_run)
        return out
    else:
        logger.error("Subprocess %s failed, trying to restore resulting data from previous pickle files" % (exe))
        return load_pickle_cluster_file(host)


def collectgroups(indiv):
    """
    List of individual users, return list of lists of users in VO (or individuals)
    """
    # list of VOs
    posvos = [x for x in grp.getgrall() if x[0].startswith(voprefix)]
    defvo = [x for x in posvos if x[0] == defaultvo][0][3]
    found = []
    groups = []
    for us in indiv:
        if us in found:
            continue
        group = [x for x in posvos if (not x[0] in novos) and (us in x[3])]
        if len(group) > 0:
            found += group[0][3]
            groups.append(group[0][3])
        else:
            # If not in VO or default vo, ignore
            if us in defvo:
                found.append(us)
                groups.append([us])

    return groups


def getName(members, uid):
    member = filter(lambda x: x['uid'] == uid, members)
    if member:
        return member[0]['gecos']
    else:
        return "(name not found)"


def collectgroupsLDAP(active_users):
    """
    List of individual users, return list of lists of users in VO (or individuals)
    """
    #setdebugloglevel(False)
    vsc_config = VscConfiguration()
    u = LdapQuery(vsc_config)

    ## all sites filter
    ldap_filter = InstituteFilter('antwerpen') | InstituteFilter('brussel') | InstituteFilter('gent') | InstituteFilter('leuven')

    user_maps_per_vo = {}

    # FIXME: workaround until such time as we have decent VO trees in the LDAP, as is expected by the LDAP libs
    vos = [g for g in VscLdapGroup.lookup(ldap_filter) if g.group_id.startswith('gvo')]
    members = dict([(u.user_id, u) for u in VscLdapUser.lookup(ldap_filter)])
    user_to_vo_map = dict([(u, vo) for vo in vos for u in vo.memberUid])

    found = set()
    for user in active_users:
        if user in found:
            continue

        # find vo of this user
        vo = user_to_vo_map.get(user, None)
        if vo:
            if vo.group_id == defaultvo:
                logger.debug("user %s belongs to the default vo %s" % (user, vo.group_id))
                found.add(user)
                name = members[user].gecos
                user_maps_per_vo[user] = {user: name}
            else:
                user_map = dict([(uid, members[uid].gecos) for uid in vo.memberUid])
                for uid in user_map:
                    found.add(uid)
                user_maps_per_vo[vo.group_id] = user_map
                logger.debug("added userMap for the vo %s" % (vo.group_id))
        # ignore users not in any VO (including default VO)

    return user_maps_per_vo


def filter_info_for_group(users, queue_information):
    """
    @type users: list of strings
    @type queue_information: dictionary

    @param users: VSC user IDs
    @param queue_information: the showq information for all users

    @returns: dictionary with the queue information for each of the given users.
    """

    filtered_queue_information = dict([(user_id, queue_information[user_id]) for user_id in users if user_id in queue_information])

    if len(filtered_queue_information) == 0:
        return

    filtered_queue_information['timeinfo'] = queue_information['timeinfo']
    return filtered_queue_information


def main():
    # Collect all info
    opt_parser = OptionParser()
    opt_parser.add_option('-n', '--nagios', dest='nagios', default=False, action='store_true',
                          help='print out nagios information')
    opt_parser.add_option("", "--dry-run", dest="dry_run", default=False, action="store_true",
                          help="Do not make any updates whatsoever.")
    opt_parser.add_option("", "--debug", dest="debug", default=False, action="store_true",
                          help="Put logging at debug level")

    (opts, args) = opt_parser.parse_args(sys.argv)

    if opts.debug:
        fancylogger.setLogLevelDebug()

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)
    if opts.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    lockfile = TimestampedPidLockfile(DSHOWQ_LOCK_FILE)
    try:
        lockfile.acquire()
    except LockFailed, err:
        logger.critical('Unable to obtain lock: lock failed')
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_CRITICAL, "CRITICAL - script failed taking lock %s" % (DSHOWQ_LOCK_FILE))
        sys.exit(1)
    except LockFileReadError, err:
        logger.critical("Unable to obtain lock: could not read previous lock file %s" % (DSHOWQ_LOCK_FILE))
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_CRITICAL, "CRITICAL - script failed reading lockfile %s" % (DSHOWQ_LOCK_FILE))
        sys.exit(1)

    failed_hosts = []
    reported_hosts = []

    tf = "%Y-%m-%d %H:%M:%S"

    logger.info("dshowq.py start time: %s" % time.strftime(tf, time.localtime(time.time())))

    queue_information = {}

    hosts = ["gengar", "gastly", "haunter", "gulpin", "dugtrio", "raichu"]
    for host in hosts:

        previous_queue_information = queue_information
        queue_information = getinfo(queue_information, host, opts.dry_run)
        if not queue_information:
            logger.error("Couldn't collect info for host %s" % (host))
            failed_hosts.append(host)
            queue_information = previous_queue_information
            continue
        else:
            reported_hosts.append(host)
            #lockfile.release()
            #sys.exit(1)

    # Collect all user/VO maps of active users
    # - for all active users, get their VOs
    # - for those groups, get all users
    # - make list of VOs and of individual users (ie default VO)
    activeusers = queue_information.keys()
    groups = collectgroupsLDAP(activeusers)

    # force mounting the home directories for the ghent users
    # FIXME: this works for the current setup, might be an issue if we change things.
    #        see ticket #987
    vsc_install_user_home = None
    try:
        vsc_install_user_home = pwd.getpwnam(VSC_INSTALL_USER_ID)[5]
        cmd = "sudo -u %s stat %s" % (VSC_INSTALL_USER_ID, vsc_install_user_home)
        os.system(cmd)
    except Exception, err:
        logger.critical("Cannot stat the VSC install user (%s) home at %s. Bailing." % (VSC_INSTALL_USER_ID, vsc_install_user_home))
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("cannot access home for user: %s" % (vsc_install_user_home),
                                           hosts=len(reported_hosts),
                                           hosts_critical=len(failed_hosts),
                                           stored=0,
                                           stored_critical=0))
        sys.exit(1)

    nagios_user_count = 0
    nagios_no_store = 0
    for group in groups.values():
        # Filter and pickle results
        # - per VO
        # - per user
        filtered_queue_information = filter_info_for_group(group, queue_information)
        logger.debug("filtered queueu information for group %s: %s" % (group, filtered_queue_information))

        if filtered_queue_information:
            for us in group:
                try:
                    if not opts.dry_run:
                        store.store_pickle_data_at_user(us, '.showq.pickle', (filtered_queue_information, group))
                        nagios_user_count += 1
                    else:
                        logger.info("Dry run: skipping storing pickle files at user (%s, %s) home." % (us, group))
                except (UserStorageError, FileStoreError, FileMoveError), err:
                    logger.error('Could not store pickle file for user %s' % (us))
                    nagios_no_store += 1
                    pass  # just keep going, trying to store the rest of the data

    logger.info("dshowq.py end time: %s" % time.strftime(tf, time.localtime(time.time())))

    try:
        lockfile.release()
    except NotLocked, err:
        logger.critical('Lock release failed: was not locked.')
        nagios_reporter.cache(NAGIOS_EXIT_WARNING,
                              NagiosResult("lock release failed (not locked)",
                                           hosts=len(reported_hosts),
                                           hosts_critical=len(failed_hosts),
                                           stored=nagios_user_count,
                                           stored_critical=nagios_no_store))
        sys.exit(1)
    except NotMyLock, err:
        logger.error('Lock release failed: not my lock')
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_WARNING, "WARNING - lock release fail (not my lock) | %s" % (NAGIOS_REPORT_VALUES_TEMPLATE % (failed_hosts, reported_hosts, nagios_user_count, nagios_no_store)))
        sys.exit(1)

    nagios_reporter.cache(NAGIOS_EXIT_OK,
                          NagiosResult("dshowq run successful",
                                       hosts=len(reported_hosts),
                                       hosts_critical=len(failed_hosts),
                                       stored=nagios_user_count,
                                       stored_critical=nagios_no_store))

    sys.exit(0)


if __name__ == '__main__':
    main()
