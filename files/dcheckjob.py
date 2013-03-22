#!/usr/bin/python
##
#
# Copyright 2013-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
dcheckjob.py requests all idle (blocked) jobs from Moab and stores the result in a JSON structure in each
users pickle directory.

@author Andy Georges
"""

import vsc.utils.fs_store as store
import vsc.utils.generaloption
from lockfile import LockFailed, NotLocked, NotMyLock
from vsc import fancylogger
from vsc.administration.user import MukUser
from vsc.jobs.moab.checkjob import checkjob, checkjobInfo
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapGroup, VscLdapUser
from vsc.ldap.filters import InstituteFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils.fs_store import UserStorageError, FileStoreError, FileMoveError
from vsc.utils.generaloption import simple_option
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_WARNING, NAGIOS_EXIT_CRITICAL
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile, LockFileReadError

#Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/dcheckjob.nagios.pickle'
NAGIOS_HEADER = 'dcheckjob'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

DCHECKJOB_LOCK_FILE = '/var/run/dcheckjob_tpid.lock'


#FIXME: this is almost completely common with dshowq pickle cache storage
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


# FIXME: This is almost completely the same as dshowq
def get_checkjob_information(opts):
    """Accumulate the checkjob information for the users on the given hosts."""

    queue_information = checkjobInfo()
    failed_hosts = []
    reported_hosts = []

    # Obtain the information from all specified hosts
    for host in opts.options.hosts:

        master = opts.configfile_parser.get(host, "master")
        checkjob_path = opts.configfile_parser.get(host, "checkjob_path")

        host_queue_information = checkjob(checkjob_path, host, ["--host=%s" % (master)], xml=True, process=True)

        if not host_queue_information:
            failed_hosts.append(host)
            logger.error("Couldn't collect info for host %s" % (host))
            logger.info("Trying to load cached pickle file for host %s" % (host))

            host_queue_information = load_pickle_cluster_file(host)
        else:
            store_pickle_cluster_file(host, host_queue_information)

        if not host_queue_information:
            logger.error("Couldn't load info for host %s" % (host))
        else:
            queue_information.update(host_queue_information)
            reported_hosts.append(host)

    return (queue_information, reported_hosts, failed_hosts)


def main():
    # Collect all info

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios': ('print out nagios information', None, 'store_true', False, 'n'),
        'nagios_check_filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios_check_interval_threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'checkjob_path': ('the path to the real shpw executable',  None, 'store', ''),
        'location': ('the location for storing the pickle file: home, scratch', str, 'store', 'home'),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    }

    opts = simple_option(options)

    if opts.options.debug:
        fancylogger.setLogLevelDebug()

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opt.options.nagios_check_interval_threshold)
    if opts.options.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    lockfile = TimestampedPidLockfile(CHECKJOB_LOCK_FILE)
    lock_or_bork(lockfile, nagios_reporter)

    tf = "%Y-%m-%d %H:%M:%S"

    logger.info("checkjob.py start time: %s" % time.strftime(tf, time.localtime(time.time())))

    (queue_information, reported_hosts, failed_hosts) = get_checkjob_information(opts)
    timeinfo = time.time()

    active_users = queue_information.keys()

    logger.debug("Active users: %s" % (active_users))
    logger.debug("Queue information: %s" % (queue_information))

    # We need to determine which users should get an updated pickle. This depends on
    # - the active user set
    # - the information we want to provide on the cluster(set) where this script runs
    # At the same time, we need to determine the job information each user gets to see
    (target_users, target_queue_information, user_map) = determine_target_information(active_users,
                                                                                      queue_information)

    logger.debug("Target users: %s" % (target_users))

    nagios_user_count = 0
    nagios_no_store = 0

    LdapQuery(VscConfiguration())

    for user in target_users:
        if not opts.options.dry_run:
            try:
                (path, store) = get_pickle_path(opts.options.location, user)
                user_queue_information = target_queue_information[user]
                user_queue_information['timeinfo'] = timeinfo
                store(user, path, (user_queue_information, user_map[user]))
                nagios_user_count += 1
            except (UserStorageError, FileStoreError, FileMoveError), err:
                logger.error("Could not store pickle file for user %s" % (user))
                nagios_no_store += 1
        else:
            logger.info("Dry run, not actually storing data for user %s at path %s" % (user, get_pickle_path(opts.options.location, user)[0]))
            logger.debug("Dry run, queue information for user %s is %s" % (user, target_queue_information[user]))

    logger.info("dcheckjob.py end time: %s" % time.strftime(tf, time.localtime(time.time())))

    #FIXME: this still looks fugly
    bork_result = NagiosResult("lock release failed",
                               hosts=len(reported_hosts),
                               hosts_critical=len(failed_hosts),
                               stored=nagios_user_count,
                               stored_critical=nagios_no_store)
    release_or_bork(lockfile, nagios_reporter, bork_result)

    nagios_reporter.cache(NAGIOS_EXIT_OK,
                          NagiosResult("run successful",
                                       hosts=len(reported_hosts),
                                       hosts_critical=len(failed_hosts),
                                       stored=nagios_user_count,
                                       stored_critical=nagios_no_store))

    sys.exit(0)


if __name__ == '__main__':
    main()





