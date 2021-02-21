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
from pdpf.iostrategy import TextOnHdfsIoStrategy, PicklablePersistenceStrategy,\
    ParquetPersistenceStrategy
import os
import sys
from pyspark.sql.types import StructField, StructType, StringType

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
        schema = StructType([ StructField('str', StringType(), False) ])
        df = self.pdpfCtx.sparkSession.createDataFrame(
            [{ 'str': teststr }],
            schema
        )
        ioS.write(df)
        resDF = ioS.read()
        res = resDF.collect()[0][0]
        self.assertEqual(res, teststr)
