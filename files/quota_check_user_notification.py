#!/usr/bin/env python
##
#
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""Script to check for quota transgressions and notify the offending users.

- relies on mmrepquota to get a quick estimate of user quota
- checks all known GPFS mounted file systems

Created Mar 8, 2012

@author Andy Georges
"""

# author: Andy Georges

import logging
import os
import pwd
import re
import sys

## FIXME: deprecated in >= 2.7
from optparse import OptionParser
from lockfile import LockFailed

import vsc.fancylogger as fancylogger

from vsc.exceptions import VscError
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile, LockFileReadError

from vsc.gpfs.quota.mmfs_utils import MMRepQuota
from vsc.gpfs.quota.entities import User, VO
from vsc.gpfs.quota.fs_store import UserFsQuotaStorage, VoFsQuotaStorage
from vsc.gpfs.quota.report import MailReporter, NagiosReporter
from vsc.gpfs.utils.exceptions import CriticalException

## Constants
NAGIOS_CHECK_FILENAME = '/var/log/quota/gpfs_quota_checker.nagios.pickle'
NAGIOS_HEADER = 'quota_check'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 30 * 60  ## 30 minutes

QUOTA_CHECK_LOG_FILE = '/var/log/quota/gpfs_quota_checker.log'
QUOTA_CHECK_REMINDER_CACHE_FILENAME = '/var/log/quota/gpfs_quota_checker.report.reminderCache.pickle'
QUOTA_CHECK_LOCK_FILE = '/var/run/gpfs_quota_checker_tpid.lock'

VSC_INSTALL_USER_NAME = 'vsc40003'

#debug = True
debug = False

# logger setup
fancylogger.logToFile(QUOTA_CHECK_LOG_FILE)
fancylogger.logToScreen(False)
fancylogger.setLogLevel(logging.INFO)
logger = fancylogger.getLogger('gpfs_quota_checker')
logger.setLevel(logging.INFO)


opt_parser = OptionParser()
opt_parser.add_option('-n', '--nagios', dest='nagios', default=False, action='store_true', help='print out nagios information')


def __nub(list):
    """Returns the unique items of a list.

    Code is taken from
    http://stackoverflow.com/questions/480214/how-do-you-remove-duplicates-from-a-list-in-python-whilst-preserving-order

    @type list: a list :-)

    @returns: a new list with each element from `list` appearing only once (cfr. Michelle Dubois).
    """
    seen = set()
    seen_add = seen.add
    return [ x for x in list if x not in seen and not seen_add(x)]


def get_gpfs_mount_points():
    '''Find out which devices are mounted under GPFS.'''
    source = '/proc/mounts'
    reg_mount = re.compile(r"^(?P<dev>\S+)\s+(?P<mntpt>\S+)\s+gpfs")
    f = file(source, 'r')
    ms = []
    for fs in f.readlines():
        r = reg_mount.search(fs)
        if r:
            (dev, _) = r.groups()
            ms.append(dev)
    ms = __nub(ms)
    if ms == []:
        logger.critical('no devices found that are mounted under GPFS')
        raise CriticalException("no devices found that are mounted under GPFS when checking %s" % (source))
    ## The following needs to be hardcoded
    if '/dev/home' not in ms:
        ms.append('/dev/home')
    logger.info('Found GPFS mounted entries: %s' % (ms))
    return ms


def get_mmrepquota_maps(devices, user_id_map):
    """Run the mmrepquota command and parse all data into user and VO maps.

    @type devices: [ String ]

    Returns (user dictionary, vo dictionary).
    """
    user_map = {}
    vo_map = {}

    for device in devices:

        mmfs = MMRepQuota(device)
        mmfs_output_lines_user = mmfs.execute_user()
        mmfs_output_lines_vo = mmfs.execute_fileset()

        uM = mmfs.parse_user_quota_lines(mmfs_output_lines_user, timestamp=True)
        fM = mmfs.parse_vo_quota_lines(mmfs_output_lines_vo, timestamp=True)

        if uM is None:
            logger.critical("could not obtain quota information for users for device %s" % (device))
            #raise CriticalException("could not gather user data from mmrepquota for device %s" % (device))
        if fM is None:
            logger.critical("could not obtain quota information for VOs for device %s" % (device))
            #raise CriticalException("could not gather vo data from mmrepquota for device %s" % (device))

        for (uId, ((used, soft, hard, doubt, expired), ts)) in uM.items():
            ## we get back the user IDs, not user names, since the GPFS tools
            ## circumvent LDAP's ncd caching mechanism.
            ## the backend expects user names
            ## getpwuid should be using the ncd cache for the LDAP info,
            ## so this should not hurt the system much
            user_info = user_id_map and user_id_map[uId] or pwd.getpwuid(uId) ## backup
            user_name = user_info[0]
            user = user_map.get(user_name, User(user_name))
            user.update_quota(device, used, soft, hard, doubt, expired, ts)
            user_map[user_name] = user

        for (vId, ((used, soft, hard, doubt, expired), ts)) in fM.items():
            ## here, we have the VO names, as per the GPFS configuration
            vo = vo_map.get(vId, VO(vId))
            vo.update_quota(device, used, soft, hard, doubt, expired, ts)
            vo_map[vId] = vo

    return (user_map, vo_map)


def nagios_analyse_data(ex_users, ex_vos, user_count, vo_count):
    '''Analyse the data blobs we gathered and build a summary for nagios.

    @type ex_users: [ quota.entities.User ]
    @type ex_vos: [ quota.entities.VO ]
    @type user_count: int
    @type vo_count: int

    Returns a tuple with two elements:
        - the exit code to be provided when the script runs as a nagios check
        - the message to be printed when the script runs as a nagios check
    '''
    ex_u = len(ex_users)
    ex_v = len(ex_vos)
    if ex_u == 0 and ex_v == 0:
        return (NagiosReporter.NAGIOS_EXIT_OK, "OK | ex_u=0 ex_v=0 pU=0 pV=0")
    else:
        pU = float(ex_u) / user_count
        pV = float(ex_v) / vo_count
        return (NagiosReporter.NAGIOS_EXIT_WARNING, "WARNING quota exceeded | ex_u=%d ex_v=%d pU=%f pV=%f" % (ex_u, ex_v, pU, pV))


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = pwd.getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d


def main(argv):

    (opts, args) = opt_parser.parse_args(argv)

    logger.info('started GPFS quota check run.')

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)

    if opts.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    lockfile = TimestampedPidLockfile(QUOTA_CHECK_LOCK_FILE)
    try:
        lockfile.acquire()
    except (LockFileReadError, LockFailed), err:
        logger.critical('Cannot obtain lock, bailing %s' % (err))
        nagios_reporter.cache(2, "CRITICAL quota check script failed to obtain lock")
        lockfile.release()
        sys.exit(2)

    try:
        mount_points = get_gpfs_mount_points()
        user_id_map = map_uids_to_names()
        (mm_rep_quota_map_users, mm_rep_quota_map_vos) = get_mmrepquota_maps(mount_points, user_id_map)

        if not mm_rep_quota_map_users or not mm_rep_quota_map_vos:
            raise CriticalException('no usable data was found in the mmrepquota output')

        ## figure out which users are crossing their softlimits
        ex_users = filter(lambda u: u.exceeds(), mm_rep_quota_map_users.values())
        logger.info("found %s users who are exceeding their quota: %s" % len(ex_users, [u.vsc_id for u in ex_users]))

        ## figure out which VO's are exceeding their softlimits
        ## currently, we're not using this, VO's should have plenty of space
        ex_vos = filter(lambda v: v.exceeds(), mm_rep_quota_map_vos.values())
        logger.info("found %s VOs who are exceeding their quota: %s" % len(ex_vos, [v.vo_id for v in ex_vos]))

        # force mounting the home directories for the ghent users
        # FIXME: this works for the current setup, might be an issue if we change things.
        #        see ticket #987
        vsc_install_user_home = None
        try:
            vsc_install_user_home = pwd.getpwnam(VSC_INSTALL_USER_NAME)[5]
            cmd = "sudo -u %s stat %s" % (VSC_INSTALL_USER_NAME, vsc_install_user_home)
            os.system(cmd)
        except Exception, err:
            raise CriticalException('Cannot stat the VSC install user (%s) home at (%s).' % (VSC_INSTALL_USER_NAME, vsc_install_user_home))

        # FIXME: cache the storage quota information (test for exceeding users)
        u_storage = UserFsQuotaStorage()
        for user in mm_rep_quota_map_users.values():
            try:
                u_storage.store_quota(user)
            except VscError, err:
                logger.error("Could not store data for user %s" % (user))
                pass  ## we're just moving on, trying the rest of the users. The error will have been logged anyway.

        v_storage = VoFsQuotaStorage()
        for vo in mm_rep_quota_map_vos.values():
            try:
                v_storage.store_quota(vo)
            except VscError, err:
                logger.error("Could not store vo data for user %s" % (user))
                pass  ## we're just moving on, trying the rest of the VOs. The error will have been logged anyway.

        # Report to the users who are exceeding their quota
        reporter = MailReporter(QUOTA_CHECK_REMINDER_CACHE_FILENAME)
        for user in ex_users:
            reporter.report_user(user)
        reporter.close()

    except CriticalException, err:
        logger.critical("critical exception caught: %s" % (err.message))
        nagios_reporter.cache(2, "CRITICAL script failed - %s" % (err.message))
        lockfile.release()
        sys.exit(1)

    (nagios_exit_code, nagios_message) = nagios_analyse_data(ex_users
                                                            , ex_vos
                                                            , user_count=len(mm_rep_quota_map_users.values())
                                                            , vo_count=len(mm_rep_quota_map_vos.values()))
    nagios_reporter.cache(nagios_exit_code, nagios_message)
    lockfile.release()

if __name__ == '__main__':
    main(sys.argv)

