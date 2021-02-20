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
from .test_runner import TestConfig

class PdpfBaseTest(unittest.TestCase):
    """Base Test Suite

        Each test suite creates its own PDPF context from a shared Spark Session for each
        test run (test_runner.py)
    """
    PytestDir = "./target/pytest"
    TestSrcDir = "./src/test/python"

    @classmethod
    def setUpClass(cls):
        import pdpf
        pdpf.logger.setLevel('DEBUG')

        from pdpf.pdpfctx import PdpfCtx

        cls.sparkSession = TestConfig.sparkSession()
        cls.sparkSession.sparkContext.setLogLevel("ERROR")

        # pdpf config is simply a dictionary
        conf = {
            'projectDir': cls.PytestDir,
            'projectName': 'pdpf-test'
        }

        cls.pdpfCtx = PdpfCtx.createInstance(conf, cls.sparkSession)

        sys.path.append(cls.resourceTestDir())

        cls.mkTmpTestDir()

    @classmethod
    def tearDownClass(cls):
        sys.path.remove(cls.resourceTestDir())

    @classmethod
    def resourceTestDir(cls):
        """Directory where resources (like modules to run) for this test are expected."""
        return cls.TestSrcDir + "/" + cls.__module__

    @classmethod
    def tmpTestDir(cls):
        """Temporary directory for each test to put the files it creates. Automatically cleaned up."""
        return cls.PytestDir + "/" + cls.__name__

    @classmethod
    def tmpDataDir(cls):
        """Temporary directory for each test to put the data it creates. Automatically cleaned up."""
        return cls.tmpTestDir() + "/data"

    @classmethod
    def mkTmpTestDir(cls):
        shutil.rmtree(cls.tmpTestDir(), ignore_errors=True)
        os.makedirs(cls.tmpTestDir())

