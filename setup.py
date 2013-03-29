#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2009-2013 Ghent University
#
# This file is part of vsc-config,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""Basic setup.py for master scripts"""

from distutils.core import setup

import vsc.install.shared_setup
from vsc.install.shared_setup import ag, sdw, wdp, kh


PACKAGE = {
    'name': 'master_scripts',
    'version': '1.3',
    'author': [ag, kh, sdw, wdp],
    'description': 'UGent HPC scripts that should be deployed on the masters',
    'license': 'LGPL',
    'packages': ['vsc', 'vsc.utils'],
    'scripts': ['bin/pbs_check_inactive_user-jobs.py', 'bin/dshowq.py', 'bin/quota_check_user_notification.py'],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
