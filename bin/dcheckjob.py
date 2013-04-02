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
dcheckjob.py requests all idle (blocked) jobs from Moab and stores the result in a CheckjobInfo structure in each
users pickle directory.

@author Andy Georges
"""
import os
import sys
import time

from vsc.utils import fancylogger
from vsc.administration.user import MukUser, cluster_user_pickle_location_map, cluster_user_pickle_store_map
from vsc.jobs.moab.checkjob import Checkjob, CheckjobInfo
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.utils import LdapQuery
from vsc.utils.fs_store import UserStorageError, FileStoreError, FileMoveError
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

#Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/dcheckjob.nagios.pickle'
NAGIOS_HEADER = 'dcheckjob'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 30 * 60  # 30 minutes

DCHECKJOB_LOCK_FILE = '/var/run/dcheckjob_tpid.lock'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


# FIXME: common
def get_pickle_path(location, user_id):
    """Determine the path (directory) where the pickle file qith the queue information should be stored.

    @type location: string
    @type user_id: string

    @param location: indication of the user accesible storage spot to use, e.g., home or scratch
    @param user_id: VSC user ID

    @returns: tuple of (string representing the directory where the pickle file should be stored,
                        the relevant storing function in vsc.utils.fs_store).
    """
    return (os.path.join(cluster_user_pickle_location_map[location](user_id).pickle_path(), ".checkjob.pickle"), cluster_user_pickle_store_map[location])


def main():
    # Collect all info

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios': ('print out nagios information', None, 'store_true', False, 'n'),
        'nagios_check_filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios_check_interval_threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'hosts': ('the hosts/clusters that should be contacted for job information', None, 'extend', []),
        'location': ('the location for storing the pickle file: home, scratch', str, 'store', 'home'),
        'ha': ('high-availability master IP address', None, 'store', None),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    }

    opts = simple_option(options)

    if opts.options.debug:
        fancylogger.setLogLevelDebug()

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)
    if opts.options.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    if not proceed_on_ha_service(opts.options.ha):
        logger.warning("Not running on the target host in the HA setup. Stopping.")
        nagios_reporter(NAGIOS_EXIT_WARNING,
                        NagiosResult("Not running on the HA master."))
        sys.exit(NAGIOS_EXIT_WARNING)

    lockfile = TimestampedPidLockfile(DCHECKJOB_LOCK_FILE)
    lock_or_bork(lockfile, nagios_reporter)

    logger.info("dcheckjob started a run")

    LdapQuery(VscConfiguration())

    clusters = {}
    for host in opts.options.hosts:
        master = opts.configfile_parser.get(host, "master")
        checkjob_path = opts.configfile_parser.get(host, "checkjob_path")
        clusters[host] = {
            'master': master,
            'path': checkjob_path
        }

    checkjob = Checkjob(clusters, cache_pickle=True, dry_run=True)

    (job_information, reported_hosts, failed_hosts) = checkjob.get_moab_command_information()
    timeinfo = time.time()

    active_users = job_information.keys()

    logger.debug("Active users: %s" % (active_users))
    logger.debug("Checkjob information: %s" % (job_information))

    nagios_user_count = 0
    nagios_no_store = 0

    for user in active_users:
        if not opts.options.dry_run:
            try:
                (path, store) = get_pickle_path(opts.options.location, user)
                user_queue_information = CheckjobInfo({user: job_information[user]})
                store(user, path, (timeinfo, user_queue_information))
                nagios_user_count += 1
            except (UserStorageError, FileStoreError, FileMoveError), _:
                logger.error("Could not store pickle file for user %s" % (user))
                nagios_no_store += 1
        else:
            logger.info("Dry run, not actually storing data for user %s at path %s" % (user, get_pickle_path(opts.options.location, user)[0]))
            logger.debug("Dry run, queue information for user %s is %s" % (user, job_information[user]))

    logger.info("dcheckjob run end")

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
