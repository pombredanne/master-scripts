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

# --------------------------------------------------------------------
import logging
import sys
import time
from collections import namedtuple
from optparse import OptionParser

# --------------------------------------------------------------------
from PBSQuery import PBSQuery

# --------------------------------------------------------------------
import vsc.fancylogger as fancylogger
from vsc.ldap.utils import LdapQuery
from vsc.utils.mail import VscMail


fancylogger.logToFile('/var/log/hpc_sync_ldap_collector.log')
fancylogger.setLogLevel(logging.DEBUG)

logger = fancylogger.getLogger(name='sync_inactive_users')


LDAPUser = namedtuple('LDAPUser', ['uid', 'status', 'gecos'])


def get_status_users(ldap, status):
    """Get the users from the HPC LDAP that match the given status.

    @type ldap: vsc.ldap.utils.LdapQuery instance
    @type status: string represeting a valid status in the HPC LDAP

    @returns: list of LDAPUser nametuples of matching users.
    """
    logger.info("Retrieving users from the HPC LDAP with status=%s." % (status))

    users = ldap.user_filter_search(filter="status=%s" % status,
                                    attributes=['cn', 'status', 'gecos'])

    logger.info("Found %d users in the %s state." % (len(users)))
    logger.debug("The following users are in the %s state: %s" % (status, users))

    return map(LDAPUser._make, users)


def get_grace_users(ldap):
    """Obtain the users that have entered their grace period.

    @type ldap: vsc.ldap.utils.LdapQuery instance

    @returns: list of LDAPUser elements of users who match the grace status
    """
    return get_status_users(ldap, 'grace')


def get_inactive_users(ldap):
    """Obtain the users that have been set to inactive.

    @type ldap: vsc.ldap.utils.LdapQuery instance

    @returns: list of LDAPUser elements of inactive users
    """
    return get_status_users(ldap, 'inactive')


def remove_queued_jobs(jobs, grace_users, inactive_users, dry_run=True):
    """Determine the queued jobs for users in grace or inactive states.

    These jobs are removed if dry_run is False.

    FIXME: I think that jobs may still slip through the mazes. If a job can start
           sooner than a person becomes inactive, a gracing user might still make
           a succesfull submission that gets started.
    @type jobs: dictionary of all jobs known to PBS, indexed by PBS job name
    @type grace_users: list of LDAPUser namedtuples of users in grace
    @type inactive_users: list of LDAPUser namedtuples of users who are inactive

    @returns: list of jobs that have been removed
    """
    uids = [u.uid for u in grace_users]
    uids.extend([u.uid for u in inactive_users])

    jobs_to_remove = []
    for (job_name, job) in jobs.items():
        user_id = jobs[job_name]['euser'][0]
        if user_id in uids:
            jobs_to_remove.append((job_name, job))

    if not dry_run:
        pass

    return jobs_to_remove


def remove_running_jobs(inactive_users, dry_run=True):
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
    print "pbs_check_active_user_jobs report"
    print "---------------------------------\n\n"

    print "Queued jobs that will be removed"
    print "--------------------------------"
    print "\n".join(["User {user_name} queued job at {queue_time} with name {job_name}".format(user_name=job['euser'][0],
                                                                                               queue_time=job['qtime'][0],
                                                                                               job_name=job_name)
                     for (job_name, job) in queued_jobs])

    print "\n"
    print "Running jobs that will be killed"
    print "--------------------------------"
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

    message_queued_jobs = "\n".join(["Queued jobs belonging to gracing or inactive users", 50 * "-"] +
                                    ["{user_name} - {job_name} queued at {queue_time}".format(user_name=job['euser'][0],
                                                                                              queue_time=job['qtime'][0],
                                                                                              job_name=job_name)
                                     for (job_name, job) in queued_jobs])

    message_running_jobs = "\n".join(["Running jobs belonging to inactive users", 4 * "-"] +
                                     ["{user_name} - {job_name} running on {nodes}".format(user_name=job['euser'][0],
                                                                                           job_name=job_name,
                                                                                           nodes=str(job['exec_host']))
                                      for (job_name, job) in running_jobs])

    mail_to = "hpc-admin@lists.ugent.be"
    mail = VscMail()

    message = """Dear admins,

These are the jobs on belonging to users who have entered their grace period or have become inactive, as indicated by the
LDAP replica on {master} at {time}.

{message_queued_jobs}

{message_running_jobs}

Kind regards,
Your friendly pbs job checking script
""".format(time=time.ctime(), message_queued_jobs=message_queued_jobs, message_running_jobs=message_running_jobs)

    try:
        logger.info("Sending report mail to %s" % (mail_to))
        mail.sendTextMail(mail_to=mail_to,
                          mail_from="HPC-user-admin@ugent.be",
                          reply_to="hpc-admin@lists.ugent.be",
                          subject="PBS check for jobs belonging to gracing or inactive users",
                          message=message)
    except Exception, err:
        logger.error("Failed in sending mail to %s (%s)." % (mail_to, err))


def main(args):
    """Main script."""

    parser = OptionParser()
    parser.add_option("-d", "--dry-run", dest="dry_run", default=False, action="store_true",
                      help="Do NOT perform any database actions, simply output what would be done")
    parser.add_option("", "--debug", dest="debug", default=False, action="store_true",
                      help="Enable debug output to log.")
    parser.add_option("-m", "--mail-report", dest="mail", default=False, action="store_true",
                      help="Send mail to the hpc-admin list with job list from gracing or inactive users")

    (options, args) = parser.parse_args(args)

    if options.debug:
        fancylogger.setLogLevel(logging.DEBUG)
    else:
        fancylogger.setLogLevel(logging.INFO)

    ldap = LdapQuery()

    grace_users = get_grace_users(ldap)
    inactive_users = get_inactive_users(ldap)

    pbs_query = PBSQuery()

    t = time.ctime()
    jobs = pbs_query.get_jobs()  # we just get them all

    removed_queued = remove_queued_jobs(jobs, grace_users, inactive_users, options.dry_run)
    removed_running = remove_running_jobs(jobs, inactive_users, options.dry_run)

    if options.mail and not options.dry_run:
        # For now, we always mail.
        mail_report(t, removed_queued, removed_running)


if __name__ == '__main__':
    main(sys.argv[1:])
