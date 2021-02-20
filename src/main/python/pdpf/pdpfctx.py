#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PdpfApp entry class
This module provides the main Pdpf singleton `pdpfApp`
"""
from datetime import datetime
import os
import sys
import re
import json
import pkgutil

import pdpf
from pdpf.error import PdpfRuntimeError

from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame


class PdpfCtx(object):
    """The Python representation of Pdpf.

    Its singleton instance is created later in the containing module
    and is named pdpfApp
    """

    # Singleton instance of PdpfApp
    _instance = None

    # default rel path for python sources from appDir
    SRC_PROJECT_PATH = "src/main/python"
    # default location for py UDL's in smv projects
    SRC_LIB_PATH = "library"

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            raise PdpfRuntimeError("An instance of SmvApp has not been created")
        else:
            return cls._instance

    @classmethod
    def createInstance(cls, config, _sparkSession, py_module_hotload=True):
        """Create singleton instance. Also returns the instance.
        """
        cls._instance = cls(config, _sparkSession, py_module_hotload)
        return cls._instance

    @classmethod
    def setInstance(cls, app):
        """Set the singleton instance.

            config need to have at least 'projectName' and 'projectDir'
        """
        cls._instance = app

    def __init__(self, config, _sparkSession, py_module_hotload=True):
        self.sparkSession = _sparkSession
        self.pdpfHome = os.environ.get("PDPF_HOME")
        if (self.pdpfHome is None):
            raise PdpfRuntimeError("PDPF_HOME env variable not set!")

        self.pdpfConf = config
        self.projectName = config.get('projectName')
        self.projectDir = config.get('projectDir')

        if (self.projectDir is None or self.projectName is None):
            raise PdpfRuntimeError("projectName and projectDir need to be specified in config param")

        self.sparkSession = _sparkSession

        if (self.sparkSession is not None):
            sc = self.sparkSession.sparkContext
            sc.setLogLevel("ERROR")

            # Set application name from config
            sc._conf.setAppName(self.pdpfConf.get('projectName', 'pdpfProject'))

            self.sc = sc
            self.sqlContext = self.sparkSession._wrapped

        self.py_module_hotload = py_module_hotload

        # shortcut is meant for internal use only
        # self.dsm = DataSetMgr(self._jvm, self.py_smvconf)

        # computed df cache, keyed by m.versioned_fqn
        # self.data_cache = {}

        # AFTER app is available but BEFORE stages,
        # use the dynamically configured app dir to set the source path, library path
        self.prependDefaultDirs()

        # self.repoFactory = DataSetRepoFactory(self)
        # self.dsm.register(self.repoFactory)

        # provider cache, keyed by providers' fqn
        # self.provider_cache = {}
        # self.refresh_provider_cache()

    def pdpfVersion(self):
        versionFile = self.pdpfHome + "/.pdpf_version"
        with open(versionFile, "r") as fp:
            line = fp.readline()
        return line.strip()

    def prependDefaultDirs(self):
        """ Ensure that mods in src/main/python and library/ are discoverable.
            If we add more default dirs, we'll make this a set
        """
        self._prepend_source(self.SRC_LIB_PATH)
        self._prepend_source(self.SRC_PROJECT_PATH)

    def removeDefaultDirs(self):
        """ The cleanup version of prependDefaultDirs
        """
        self._remove_source(self.SRC_PROJECT_PATH)
        self._remove_source(self.SRC_LIB_PATH)

    def _abs_path_for_project_path(self, src_path):
        # Load dynamic app dir from scala
        return os.path.abspath(os.path.join(self.projectDir, src_path))

    def _prepend_source(self, src_path):
        abs_path = self._abs_path_for_project_path(src_path)
        # Source must be added to front of path to make sure it is found first
        sys.path.insert(1, abs_path)
        pdpf.logger.debug("Prepended {} to sys.path".format(abs_path))

    def _remove_source(self, src_path):
        try:
            abs_path = self._abs_path_for_project_path(src_path)
            sys.path.remove(abs_path)
            pdpf.logger.debug("Removed {} from sys.path".format(abs_path))
        except ValueError:
            # ValueError will be raised if the project path was not previously
            # added to the sys.path
            pass

