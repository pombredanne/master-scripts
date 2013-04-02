#!/usr/bin/env python
# -*- coding: latin-1 -*-
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
"""Basic setup.py for building the hpcugent Icinga checks"""

import sys
import os
from distutils.core import setup
import glob

setup(name="master_scripts",
      version="1.2",
      description="UGent HPC scripts that should be deployed on the masters",
      long_description="""Scripts that run on one or more masters
 - GPFS quota checking and caching
 - Queue information caching for the users
 - PBS queue monitoring for inactive users
""",
      license="LGPL",
      author="HPC UGent",
      author_email="hpc-admin@lists.ugent.be",
      scripts=glob.glob(os.path.join("files", "*")),
      url="http://www.ugent.be/hpc")
