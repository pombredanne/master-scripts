#!/usr/bin/python

"""
Collect showq info
- filter
- distribute pickle
"""
import cPickle
import grp
import logging
import os
import pwd
import sys
import time

import vsc.utils.fs_store as store

from lockfile import LockFailed, NotLocked, NotMyLock
from vsc.utils.nagios import NagiosReporter
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile
from vsc.exceptions import UserStorageError, FileStoreError, FileMoveError

import vsc.fancylogger as fancylogger

logger = fancylogger.getLogger(__name__)
fancylogger.setLogLevel(logging.INFO)
## need the full utils, not the simple ones
try:
    from vsc.ldap import utils
except Exception, err:
    logger.critical("Can't init ldap utils: %s" % err)
    sys.exit(1)

from optparse import OptionParser

#Constants
NAGIOS_CHECK_FILENAME = '/var/log/pickles/dshowq.nagios.pickle'
NAGIOS_HEADER = 'dshowq'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  ## 15 minutes
# HostsReported HostsUnavailable UserCount UserNoStorePossible
NAGIOS_REPORT_VALUES_TEMPLATE = "HR=%d, HU=%d, UC=%d, NS=%d"


DSHOWQ_LOCK_FILE = '/var/run/dshowq_tpid.lock'

opt_parser = OptionParser()
opt_parser.add_option('-n', '--nagios', dest='nagios', default=False, action='store_true', help='print out nagios information')

realshowq = '/usr/bin/showq'
voprefix = 'gvo'

VSC_INSTALL_USER_ID = 'vsc40003'

## all default VOs
defaultvo = 'gvo00012'
novos = ('gvo00012', 'gvo00016', 'gvo00017', 'gvo00018')

def getinfo(res, host):
    """
    Execute showq -v
    - parse into fields
    - add timestamp
    """

    out = getout(host)
    if not out:
        # Failure, do nothing
        logger.error("ERROR: Failed to get output from real showq.")
        return
    res = parseshowqxml(res, host, out)
    # don't check, empty when there are no jobs (which is ok)
    #if not res:
    #    print "ERROR: Failed to parse XML obtained from showq."
        # Failure, do nothing
    #    return

    ## add timestamp to res
    res['timeinfo'] = time.time()

    return res


def parseshowqxml(res, host, txt):
    """
    Parse showq --xml output

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
        if not res.has_key(user):
            res[user] = {}
        if not res[user].has_key(host):
            res[user][host] = {}
        if not res[user][host].has_key(state):
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


def getout(host):
    if host in ["gengar", "gastly", "haunter", "gulpin", "dugtrio"]:
        if host == "gengar":
            exe = "%s --xml --host=master2.gengar.gent.vsc" % (realshowq)
        if host == "gastly":
            exe = "%s --xml --host=master3.gastly.gent.vsc" % (realshowq)
        if host == "haunter":
            exe = "%s --xml --host=master5.haunter.gent.vsc" % (realshowq)
        if host == "gulpin":
            exe="%s --xml --host=master9.gulpin.gent.vsc"%(realshowq)
            # maui workaround:
            # exe = "ssh master9.gulpin.gent.vsc /root/showq_to_xml.sh"
        if host == "dugtrio":
            exe="%s --xml --host=master11.dugtrio.gent.vsc"%(realshowq)
            # maui workaround:
            # exe = "ssh master11.dugtrio.gent.vsc /root/showq_to_xml.sh"
    else:
        if not host:
            exe = "%s --xml" % realshowq
        else:
            logger.error("Unknown host specified: %s" % host)
            sys.exit(0)
    from subprocess import Popen, PIPE
    p = Popen(exe, shell=True, stdout=PIPE, stderr=PIPE, close_fds=True)
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
        logger.info("Subprocess %s ran OK, storing resulting data in pickle files" % (exe))
        # create backup of out, in case future showq commands fail
        try:
            store.store_pickle_data_at_user('root', '.showq.pickle.cluster_%s' % host, out)
        except (UserStorageError, FileStoreError, FileMoveError), err:
            # these should NOT occur, we're root, accessing our own home directory
            logger.critical("Cannot store the out file %s at %s" % ('.showq.pickle.cluster_%s', '/root'))
        return out
    else:
        logger.error("Subprocess %s failed, trying to restore resulting data from previous pickle files: %s" % (exe, err))
        # try restoring last known out
        home = pwd.getpwnam('root')[5]
        if not os.path.isdir(home):
            logger.error("Homedir %s owner %s not found" % (home, owner))
            return

        dest = "%s/.showq.pickle.cluster_%s" % (home, host)
        try:
            f = open(dest)
            out = cPickle.load(f)
            f.close()
            return out
        except Exception , err:
            logger.error("Failed to load pickle from file %s: %s" % (dest, err))
            return

def collectgroups(indiv):
    """
    List of individual users, return list of lists of users in VO (or individuals)
    """
    ## list of VOs
    posvos = [ x for x in grp.getgrall() if x[0].startswith(voprefix)]
    defvo = [ x for x in posvos if x[0] == defaultvo ][0][3]
    found = []
    groups = []
    for us in indiv:
        if us in found: continue
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

def collectgroupsLDAP(indiv):
    """
    List of individual users, return list of lists of users in VO (or individuals)
    Uses LDAP directly
    """
    #setdebugloglevel(False)
    u = utils.LdapQuery()

    ## all sites filter
    ldapf = "(|(institute=antwerpen) (institute=brussel) (institute=gent) (institute=leuven))"

    userMapsPerVo = {}
    vos = u.vo_filter_search(filter=ldapf, attributes=['cn', 'description', 'institute', 'memberUid'])
    members = u.user_filter_search(filter=ldapf, attributes=['institute', 'uid', 'gecos', 'cn'])
    found = []
    for us in indiv:
        if us in found: continue

        # find vo of this user
        vo = filter(lambda x: us in x.get('memberUid',[]), vos)
        if len(vo) == 1:
            # check if for default VO
            if vo[0]['cn'] == defaultvo:
                found.append(us)
                name = getName(members, us)
                userMapsPerVo[us] = {us:name}
            else:
                userMap = {}
                for uid in vo[0]['memberUid']:
                    found.append(uid)
                    name = getName(members, uid)
                    userMap[uid] = name
                userMapsPerVo[vo[0]['cn']] = userMap
        # ignore users not in any VO (including default VO)

    return userMapsPerVo

def groupinfo(users, res):
    """
    For list of users, return filtered data
    """
    newres = {}
    for us in users:
        if res.has_key(us):
            newres[us] = res[us]

    if len(newres) == 0:
        return

    newres['timeinfo'] = res['timeinfo']
    return newres

def groupinfoLDAP(users, res):
    """
    For list of users, return filtered data
    """
    newres = {}
    for us in users.keys():
        if res.has_key(us):
            newres[us] = res[us]

    if len(newres) == 0:
        return

    newres['timeinfo'] = res['timeinfo']
    return newres


if __name__ == '__main__':
    # Collect all info

    (opts, args) = opt_parser.parse_args(sys.argv)
    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)
    if opts.nagios:
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

    res = {}

    hosts = ["gengar", "gastly", "haunter", "gulpin", "dugtrio"]
    for host in hosts:

        oldres = res
        res = getinfo(res, host)
        if not res:
            logger.error("Couldn't collect info for host %s" % (host))
            failed_hosts.append(host)
            res = oldres
            continue
        else:
            reported_hosts.append(host)
            #lockfile.release()
            #sys.exit(1)

    # Collect all user/VO maps of active users
    # - for all active users, get their VOs
    # - for those groups, get all users
    # - make list of VOs and of individual users (ie default VO)
    activeusers = res.keys()
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
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_CRITICAL, "CRITICAL - cannot install home for user: %s" % (vsc_install_user_home))
        sys.exit(1)

    nagios_user_count = 0
    nagios_no_store = 0
    for group in groups.values():
        # Filter and pickle results
        # - per VO
        # - per user
        newres = groupinfoLDAP(group, res)

        if newres:
            for us in group:
                try:
                    store.store_pickle_data_at_user(us, '.showq.pickle', (newres, group))
                    nagios_user_count += 1
                except (UserStorageError, FileStoreError, FileMoveError), err:
                    logger.error('Could not store pickle file for user %s' % (us))
                    nagios_no_store += 1
                    pass # just keep going, trying to store the rest of the data

    logger.info("dshowq.py end time: %s" % time.strftime(tf, time.localtime(time.time())))

    try:
        lockfile.release()
    except NotLocked, err:
        logger.critical('Lock release failed: was not locked.')
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_WARNING, "WARNING - lock release fail (not locked) | %s" % (NAGIOS_REPORT_VALUES_TEMPLATE % (failed_hosts, reported_hosts, nagios_user_count, nagios_no_store)))
        sys.exit(1)
    except NotMyLock, err:
        logger.error('Lock release failed: not my lock')
        nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_WARNING, "WARNING - lock release fail (not my lock) | %s" % (NAGIOS_REPORT_VALUES_TEMPLATE % (failed_hosts, reported_hosts, nagios_user_count, nagios_no_store)))
        sys.exit(1)

    nagios_reporter.cache(NagiosReporter.NAGIOS_EXIT_OK, "OK | %s" % (NAGIOS_REPORT_VALUES_TEMPLATE % (len(failed_hosts), len(reported_hosts), nagios_user_count, nagios_no_store)))
    sys.exit(0)

