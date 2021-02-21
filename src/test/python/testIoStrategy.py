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
from pdpf.pdpfmodulerunner import PdpfModuleRunner
from pdpf.iostrategy import TextOnHdfsIoStrategy, PicklablePersistenceStrategy,\
    ParquetPersistenceStrategy
import os
import sys

class IoStrategyTest(PdpfBaseTest):
    def _path(self, filename):
        return "{}/{}".format(self.tmpTestDir(), filename)

    def test_textIo(self):
        path = self._path("test_text.tmp")
        ioS = TextOnHdfsIoStrategy(self.pdpfCtx, path)
        teststr = "===write test string==="
        ioS.write(teststr)
        res = ioS.read()
        self.assertEqual(teststr, res)

    def test_pickle(self):
        path = self._path("test_pickle.tmp")
        ioS = PicklablePersistenceStrategy(self.pdpfCtx, path)
        teststr = "===write test string==="
        ioS.write({
            'vals': [1, 2],
            'str': teststr
        })
        res = ioS.read()
        self.assertEqual(teststr, res['str'])
        self.assertEqual(2, res['vals'][1])

    def test_parquet(self):
        path = self._path("test_parquet.tmp")
        ioS = ParquetPersistenceStrategy(self.pdpfCtx, path)
        teststr = "===write test string==="
        df = self.simpleStrDf(teststr)
        ioS.write(df)
        resDF = ioS.read()
        self.assertSimpleStrDf(resDF, teststr)

    def test_sparkdf_persist(self):
        from project2.modules import M1
        m = M1(self.pdpfCtx)
        m._resolve()

        # below should be replaced by "df" function in Base
        def run_debug(info):
            log = pdpf.logger
            log.debug("Runinfo {}".format(info))
        [mdata] = PdpfModuleRunner([m], self.pdpfCtx, run_debug).run()
        self.assertSimpleStrDf(mdata, 'test_str')
