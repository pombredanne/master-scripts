#!/usr/bin/env python
##
#
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://hpc.ugent.be).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
Check the running jobs and the job queues for jobs that belong to
users who are no longer active (grace or inactive)

If the state field in the LDAP equals
    - grace: remove jobs from the queue
    - inactive: remove running jobs and jobs from the queue

Script can be run with the following options:
    - --dry-run: just check, take no action and report on what would be done
    - --debug: set logging level to DEBUG instead of INFO

This script is running on the masters, which are at Python 2.6.x.
"""

import socket
import sys
import time

from PBSQuery import PBSQuery

from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapUser
from vsc.ldap.filters import LdapFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.generaloption import simple_option
from vsc.utils.mail import VscMail
from vsc.utils.nagios import NagiosResult, NagiosReporter, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_OK


fancylogger.logToFile('/var/log/pbs_check_inactive_user_jobs.log')
fancylogger.setLogLevelDebug()

logger = fancylogger.getLogger(name='pbs_check_inactive_user_jobs')

NAGIOS_CHECK_FILENAME = '/var/log/pickles/pbs_check_inactive_user_jobs.nagios.pickle'
NAGIOS_HEADER = 'pbs_check_inactive_user_jobs'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

PBS_CHECK_LOG_FILE = '/var/log/pbs_check_inactive_user_jobs.log'


def get_user_with_status(status):
    """Get the users from the HPC LDAP that match the given status.

    @type ldap: vsc.ldap.utils.LdapQuery instance
    @type status: string represeting a valid status in the HPC LDAP

    @returns: list of VscLdapUser nametuples of matching users.
    """
    logger.info("Retrieving users from the HPC LDAP with status=%s." % (status))

    ldap_filter = LdapFilter("status=%s" % (status))
    users = VscLdapUser.lookup(ldap_filter)

    logger.info("Found %d users in the %s state." % (len(users), status))
    logger.debug("The following users are in the %s state: %s" % (status, [u.user_id for u in users]))

    return users


def remove_queued_jobs(jobs, grace_users, inactive_users, dry_run=True):
    """Determine the queued jobs for users in grace or inactive states.

    These jobs are removed if dry_run is False.

    FIXME: I think that jobs may still slip through the mazes. If a job can start
           sooner than a person becomes inactive, a gracing user might still make
           a succesfull submission that gets started.
    @type jobs: dictionary of all jobs known to PBS, indexed by PBS job name
    @type grace_users: list of VscLdapUser of users in grace
    @type inactive_users: list of VscLdapUser of users who are inactive

    @returns: list of jobs that have been removed
    """
    uids = [u.user_id for u in grace_users]
    uids.extend([u.user_id for u in inactive_users])

    jobs_to_remove = []
    for (job_name, job) in jobs.items():
        user_id = jobs[job_name]['euser'][0]
        if user_id in uids:
            jobs_to_remove.append((job_name, job))

    logger.info("Found {queued_count} queued jobs belonging to gracing or inactive users".format(queued_count=len(jobs_to_remove)))
    logger.debug("These are the jobs names: {job_names}".format(job_names=[n for (n, _) in jobs_to_remove]))

    if not dry_run:
        pass

    return jobs_to_remove


def remove_running_jobs(jobs, inactive_users, dry_run=True):
    """Determine the jobs that are currently running that should be removed due to owners being in grace or inactive state.

    FIXME: At this point there is no actual removal.

    @returns: list of jobs that have been removed.
    """
    return []


def print_report(queued_jobs, running_jobs):
    """Print a report detailing the jobs that have been removed from the queue or have been killed.

    @type queued_jobs: list of queued job tuples (name, PBS job entry)
    @type running_jobs: list of running job tuples (name, PBS job entry)
    """
    print 'pbs_check_active_user_jobs report'
    print '---------------------------------\n\n'

    print 'Queued jobs that will be removed'
    print '--------------------------------'
    print "\n".join(["User {user_name} queued job at {queue_time} with name {job_name}".format(user_name=job['euser'][0],
                                                                                               queue_time=job['qtime'][0],
                                                                                               job_name=job_name)
                     for (job_name, job) in queued_jobs])

    print '\n'
    print 'Running jobs that will be killed'
    print '--------------------------------'
    print "\n".join(["User {user_name} has a started job at {start_time} with name {job_name}".format(user_name=job['euser'][0],
                                                                                                      start_time=job['start_time'][0],
                                                                                                      job_name=job_name)
                     for (job_name, job) in running_jobs])


def mail_report(t, queued_jobs, running_jobs):
    """Mail report to hpc-admin@lists.ugent.be.

    @type t: string representing the time when the job list was fetched
    @type queued_jobs: list of queued job tuples (name, PBS job entry)
    @type running_jobs: list of running job tuples (name, PBS job entry)
    """

    message_queued_jobs = '\n'.join(['Queued jobs belonging to gracing or inactive users', 50 * '-'] +
                                    ["{user_name} - {job_name} queued at {queue_time}".format(user_name=job['euser'][0],
                                                                                              queue_time=job['qtime'][0],
                                                                                              job_name=job_name)
                                     for (job_name, job) in queued_jobs])

    message_running_jobs = '\n'.join(['Running jobs belonging to inactive users', 40 * '-'] +
                                     ["{user_name} - {job_name} running on {nodes}".format(user_name=job['euser'][0],
                                                                                           job_name=job_name,
                                                                                           nodes=str(job['exec_host']))
                                      for (job_name, job) in running_jobs])

    mail_to = 'hpc-admin@lists.ugent.be'
    mail = VscMail()

    message = """Dear admins,

These are the jobs on belonging to users who have entered their grace period or have become inactive, as indicated by the
LDAP replica on {master} at {time}.

{message_queued_jobs}

{message_running_jobs}

Kind regards,
Your friendly pbs job checking script
""".format(master=socket.gethostname(), time=time.ctime(), message_queued_jobs=message_queued_jobs, message_running_jobs=message_running_jobs)

    try:
        logger.info("Sending report mail to %s" % (mail_to))
        mail.sendTextMail(mail_to=mail_to,
                          mail_from='HPC-user-admin@ugent.be',
                          reply_to='hpc-admin@lists.ugent.be',
                          subject='PBS check for jobs belonging to gracing or inactive users',
                          message=message)
    except Exception, err:
        logger.error("Failed in sending mail to %s (%s)." % (mail_to, err))


def main(args):
    """Main script."""

    options = {
        'nagios': ('print out nagion information', None, 'store_true', False, 'n'),
        'nagios_check_filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios_check_interval_threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'mail-report': ('mail a report to the hpc-admin list with job list for gracing or inactive users',
                        None, 'store_true', False),
        'ha': ('high-availability master IP address', None, 'store', None),
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
    }
    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)

    if options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    if not proceed_on_ha_service(opts.options.ha):
        logger.warning("Not running on the target host in the HA setup. Stopping.")
        nagios_reporter(NAGIOS_EXIT_WARNING,
                        NagiosResult("Not running on the HA master."))
        sys.exit(NAGIOS_EXIT_WARNING)

    try:
        vsc_config = VscConfiguration()
        LdapQuery(vsc_config)

        grace_users = get_user_with_status('grace')
        inactive_users = get_user_with_status('inactive')

        pbs_query = PBSQuery()

        t = time.ctime()
        jobs = pbs_query.getjobs()  # we just get them all

        removed_queued = remove_queued_jobs(jobs, grace_users, inactive_users, options.dry_run)
        removed_running = remove_running_jobs(jobs, inactive_users, options.dry_run)

        if options.mail and not options.dry_run:
            if len(removed_queued) > 0 or len(removed_running) > 0:
                mail_report(t, removed_queued, removed_running)
    except Exception, err:
        logger.error("Something went wrong: {err}".format(err=err))
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("Script failed, check log file ({logfile})".format(logfile=PBS_CHECK_LOG_FILE)))
        sys.exit(NAGIOS_EXIT_CRITICAL)

    if len(removed_queued) > 0 or len(removed_running) > 0:
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("grace or inactive user jobs queud",
                                           queued=len(removed_queued),
                                           running=len(remove_running_jobs)))
    else:
        nagios_reporter.cache(NAGIOS_EXIT_OK,
                              NagiosResult("no queued or running jobs for grace or inactive users",
                                           queued=0,
                                           running=0))


if __name__ == '__main__':
    main(sys.argv[1:])
