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

from test_support.pdpfbasetest import PdpfBaseTest
import pdpf
from pdpf.hdfshandler import HdfsHandler
import os
import sys

class HdfsTest(PdpfBaseTest):
    hdfs = None

    def setUp(self):
        self.hdfs = HdfsHandler(self.pdpfCtx)

    def _path(self, filename):
        return "{}/{}".format(self.tmpTestDir(), filename)

    def test_exist(self):
        path = self._path("test_exist.tmp")
        with open(path, 'w') as f:
            f.write('xxxx')
        assert(self.hdfs.exists(path))

    def test_create(self):
        path = self._path("test_create.tmp")
        self.hdfs.createFileAtomic(path)
        assert(self.hdfs.exists(path))

    def test_delete(self):
        path = self._path("test_delete.tmp")
        self.hdfs.createFileAtomic(path)
        assert(os.path.exists(path))
        self.hdfs.deleteFile(path)
        assert(not os.path.exists(path))

