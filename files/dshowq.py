#!/usr/bin/python

"""
Collect showq info
- filter 
- distribute pickle
"""
import sys, os, re, grp, pwd, time, cPickle
realshowq = '/opt/moab/bin/showq'

voprefix = 'gvo'

from lockfile import LockFailed, NotLocked, NotMyLock
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

import vsc.fancylogger as fancylogger

## need the full utils, not the simple ones
try:
    from vsc.ldap import utils
#from vsc.log import setdebugloglevel
except Exception, err:
    logger.critical("Can't init utils: %s" % err)
    sys.exit(1)

realshowq = '/opt/moab/bin/showq'

voprefix = 'gvo'

logger = fancylogger.getLogger(__name__)

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

def writebuffer(owner, data, extra=''):
    """
    cpickle data to file. 
    -- enforce strict permisisons 
    """
    try:
        home = pwd.getpwnam(owner)[5]
    except Exception, err:
        #print "User %s inactive (?): %s"%(owner,err)
        logger.warning("User %s inactive (?): %s" % (owner, err))
        return
    if not os.path.isdir(home):
        logger.warning("Homedir %s owner %s not found" % (home, owner))
        return

    fn = ".showq.pickle"
    dest = os.path.join("%s" % home, "%s%s" % (fn, extra))
    tmpdir = "/tmp"
    if owner == 'root':
       tmpdir = "/root" # we need to use /root here for os.rename to work
    desttmp = os.path.join(tmpdir, "%s.tmp" % fn)
    if not os.path.exists(desttmp):
        try:
            f = open(desttmp, 'w')
            f.write('')
            f.close()
        except Exception, err:
            logger.error("Failed to write to temporary destination %s: %s" % (desttmp, err))
            return

    try:
        f = open(desttmp, 'w')
        cPickle.dump(data, f)
        f.close()
    except Exception, err:
        logger.error("Failed to to pickle %s: %s" % (desttmp, err))
        return

    """
    Move tmp do real dest
    """
    try:
	if owner == 'root':
		os.rename(desttmp, dest) # rename file
		import stat
        	os.chmod(dest, stat.S_IRUSR) # read-only
	else:
		# copy file to desired location, as another user (necessary because of NFS root squash)

            os.chown(desttmp, pwd.getpwnam(owner)[2], pwd.getpwnam(owner)[3]) # restrict access
            cmd = "sudo -u %s chmod 700 %s" % (owner, dest) # make sure destination is writable if it's there
            os.system(cmd)
            cmd = "sudo -u %s cp %s %s" % (owner, desttmp, dest) # copy new file
            os.system(cmd)
            cmd = "sudo -u %s chmod 400 %s" % (owner, dest) # change to read-only
            os.system(cmd)
            os.remove(desttmp) # get rid of tmp file
    except Exception, err:
        logger.error("Failed to move tmp file %s to real dest %s: %s" % (desttmp, dest, err))
        return

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
            exe = "%s --xml --host=master.gengar.gent.vsc" % (realshowq)
        if host == "gastly":
            exe = "%s --xml --host=master3.gastly.gent.vsc" % (realshowq)
        if host == "haunter":
            exe = "%s --xml --host=master5.haunter.gent.vsc" % (realshowq)
        if host == "gulpin":
            #exe="%s --xml --host=master9.gulpin.gent.vsc"%(realshowq)
            exe = "ssh master9.gulpin.gent.vsc /root/showq_to_xml.sh"
        if host == "dugtrio":
            #exe="%s --xml --host=master11.dugtrio.gent.vsc"%(realshowq)
            exe = "ssh master11.dugtrio.gent.vsc /root/showq_to_xml.sh"
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
        # create backup of out, in case future showq commands fail
        writebuffer('root', out, '.cluster_%s' % host)
        return out
    else:
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
            """
            If not in VO or default vo, ignore
            """
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
    u = utils.FullLdapTools()

    ## all sites filter
    ldapf = "(|(institute=antwerpen) (institute=brussel) (institute=gent) (institute=leuven))"

    userMapsPerVo = {}
    vos = u.vo_search(filter=ldapf, attrs=['cn', 'description', 'institute', 'memberUid'])
    members = u.user_search(filter=ldapf, attrs=['institute', 'uid', 'gecos', 'cn'])
    found = []
    for us in indiv:
        if us in found: continue

        # find vo of this user
        vo = filter(lambda x: us in x['memberUid'], vos)
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
    """
    Collect all info
    """

    lockfile = TimestampedPidLockfile('/var/run/dshowq_tpid.lock')
    try:
        lockfile.acquire()
    except LockFailed, err:
        logger.critical('Unable to obtain lock: lock failed')
        sys.exit(1)
    except LockFileReadError, err:
        logger.critical("Unable to obtain lock: could not read previous lock file /var/run/dshowq_tpid.lock")
        sys.exit(1)


    tf = "%Y-%m-%d %H:%M:%S"

    logger.info("dshowq.py start time: %s" % time.strftime(tf, time.localtime(time.time())))

    res = {}

    hosts = ["gengar", "gastly", "haunter", "gulpin", "dugtrio"]
    for host in hosts:

        oldres = res
        res = getinfo(res, host)
        if not res:
            logger.error("Couldn't collect info")
            lockfile.release()
            sys.exit(0)

    """
    Collect all user/VO maps of active users
    - for all active users, get their VOs
    - for those groups, get all users
    - make list of VOs and of individual users (ie default VO)
    """
    activeusers = res.keys()
    groups = collectgroupsLDAP(activeusers)

    for group in groups.values():
        """
        Filter and pickle results
        - per VO
        - per user
        """
        newres = groupinfoLDAP(group, res)

        if newres:
            for us in group:
                writebuffer(us, (newres, group))

    logger.info("dshowq.py end time: %s" % time.strftime(tf, time.localtime(time.time())))

    try:
        lockfile.release()
    except NotLocked, err:
        logger.critical('Lock release failed: was not locked.')
    except NotMyLock, err:
        logger.error('Lock release failed: not my lock')
