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

class HdfsHandler(object):
    """Provide HDFS access through Spark Java gateway
    """
    def __init__(self, pdpfCtx):
        self.pdpfCtx = pdpfCtx
        self.jvm = self.pdpfCtx.sc._gateway.jvm
        self.hadoopConf = self.pdpfCtx.sc._jsc.hadoopConfiguration()
        self.Path = self.jvm.org.apache.hadoop.fs.Path

    def _getFileSystem(self, path):
        uri = self.jvm.java.net.URI.create(path)
        return self.jvm.org.apache.hadoop.fs.FileSystem.get(uri, self.hadoopConf)

    def exists(self, path):
        fs = self._getFileSystem(path)
        return fs.exists(self.Path(path))

    def createFileAtomic(self, path):
        return self._getFileSystem(path).create(self.Path(path), False).close()

    def deleteFile(self, path):
        return self._getFileSystem(path).delete(self.Path(path), True)

    def readFromFile(self, path):
        stream = self._getFileSystem(path).open(self.Path(path))
        writer = self.jvm.java.io.StringWriter()
        self.jvm.org.apache.commons.io.IOUtils.copy(stream, writer, "UTF-8")
        return writer.toString()
