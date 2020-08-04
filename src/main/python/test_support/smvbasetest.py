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

import unittest

import os, shutil, sys
from test_runner import TestConfig

class SmvBaseTest(unittest.TestCase):
    # DataDir value is deprecated. Use tmpDataDir instead
    DataDir = "./target/data"
    PytestDir = "./target/pytest"
    TestSrcDir = "./src/test/python"

    @classmethod
    def setUpClass(cls):
        # Import needs to happen during EVERY setup to ensure that we are
        # using the most recently reloaded SmvApp
        #from smv.smvapp import SmvApp

        cls.sparkSession = TestConfig.sparkSession()
        cls.sparkSession.sparkContext.setLogLevel("ERROR")

        #args = TestConfig.smv_args() + cls.smvAppInitArgs() + ['--data-dir', cls.tmpDataDir()]
        # The test's SmvApp must be set as the singleton for correct results of some tests
        # The original SmvApp (if any) will be restored when the test is torn down
        #cls.smvApp = SmvApp.createInstance(args, cls.sparkSession)

        sys.path.append(cls.resourceTestDir())

        #cls.mkTmpTestDir()

    @classmethod
    def resourceTestDir(cls):
        """Directory where resources (like modules to run) for this test are expected."""
        return cls.TestSrcDir + "/" + cls.__module__
