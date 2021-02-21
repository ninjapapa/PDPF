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
import abc
import binascii

from pdpf.utils import ABC

# If using Python 2, prefer cPickle because it is faster
# If using Python 3, there is no cPickle (cPickle is now the implementation of pickle)
# see https://docs.python.org/3.1/whatsnew/3.0.html#library-changes
try:
    import cPickle as pickle_lib
except ImportError:
    import pickle as pickle_lib

class IoStrategy(ABC):
    """Base class for all module I/O, including read, write and persistence"""
    @abc.abstractmethod
    def read(self):
        """Read data from persisted"""

    @abc.abstractmethod
    def write(self, raw_data):
        """Write data to persist file/db"""

class PersistenceStrategy(IoStrategy):
    """Base class for IO strategy which used for persisting data"""
    @abc.abstractmethod
    def isPersisted(self):
        """Whether the data got successfully persisted before"""

    @abc.abstractmethod
    def remove(self):
        """Remove persisted file(s)"""

class NonOpPersistenceStrategy(PersistenceStrategy):
    """Never persist, isPersisted always returns false"""
    def read(self):
        pass

    def write(self, raw_data):
        pass

    def isPersisted(self):
        return False

    def remove(self):
        pass

class FileOnHdfsPersistenceStrategy(PersistenceStrategy):
    """Abstract class for persisting data to Hdfs file system
        handling general tasks as file name creation, locking when write, etc.

        Args:
            smvApp(SmvApp):
            versioned_fqn(str): data/module's FQN/Name with hash_of_hash
            postfix(str): persisted file's postfix
            file_path(str): parameters "versioned_fqn" and "postfix" are used to create
                a data file path. However if "file_path" is provided, all the other 3
                parameters are ignored
    """
    def __init__(self, pdpfCtx, file_path):
        self.pdpfCtx = pdpfCtx
        self.hdfs = pdpfCtx.hdfs
        self._file_path = file_path

    @abc.abstractmethod
    def _read(self):
        """The raw io read action"""

    def read(self):
        # May add lock or other logic here in future
        return self._read()

    @abc.abstractmethod
    def _write(self, raw_data):
        """The raw io write action"""

    def write(self, dataframe):
        # May add lock or other logic here in future
        self._write(dataframe)

    def isPersisted(self):
        return self.hdfs.exists(self._file_path)

    def remove(self):
        self.hdfs.deleteFile(self._file_path)


class PicklablePersistenceStrategy(FileOnHdfsPersistenceStrategy):
    def __init__(self, pdpfCtx, file_path):
        super(PicklablePersistenceStrategy, self).__init__(pdpfCtx, file_path)

    def _read(self):
        # reverses result of applying _write. see _write for explanation.
        hex_encoded_pickle_as_str = self.hdfs.readFromFile(self._file_path)
        pickled_res_as_str = binascii.unhexlify(hex_encoded_pickle_as_str)
        return pickle_lib.loads(pickled_res_as_str)

    def _write(self, rawdata):
        pickled_res = pickle_lib.dumps(rawdata, -1)
        # pickle may contain problematic characters like newlines, so we
        # encode the pickle it as a hex string
        hex_encoded_pickle = binascii.hexlify(pickled_res)
        # encoding will be a bytestring object if in Python 3, so need to convert it to string
        # str.decode converts string to utf8 in python 2 and bytes to str in Python 3
        hex_encoded_pickle_as_str = hex_encoded_pickle.decode()
        self.hdfs.writeToFile(hex_encoded_pickle_as_str, self._file_path)


class ParquetPersistenceStrategy(FileOnHdfsPersistenceStrategy):
    """Persist strategy for using Spark native parquet

        Args:
            smvApp(SmvApp):
            versioned_fqn(str): data/module's FQN/Name with hash_of_hash
            file_path(str): parameter "versioned_fqn" is used to create
                a data file path. However if "file_path" is provided, all the other 2
                parameters are ignored
    """
    def __init__(self, pdpfCtx, file_path):
        super(ParquetPersistenceStrategy, self).__init__(pdpfCtx, file_path)

    @property
    def _semaphore_path(self):
        return "{}.semaphore".format(self._file_path)

    def _read(self):
        return self.pdpfCtx.sparkSession.read.parquet(self._file_path)

    def _write(self, rawdata):
        # default to overwrite to be consistent with csv persist
        self.hdfs.deleteFile(self._file_path)

        # rawdata is a Spark DF
        rawdata.write.parquet(self._file_path)
        self.hdfs.createFileAtomic(self._semaphore_path)

    def remove(self):
        self.hdfs.deleteFile(self._file_path)
        self.hdfs.deleteFile(self._semaphore_path)

    def isPersisted(self):
        return self.hdfs.exists(self._semaphore_path)


class TextOnHdfsIoStrategy(IoStrategy):
    """Simple read/write a small text file on Hdfs"""
    def __init__(self, pdpfCtx, path):
        self.hdfs = pdpfCtx.hdfs
        self._path = path

    def read(self):
        return self.hdfs.readFromFile(self._path)

    def write(self, rawdata):
        self.hdfs.writeToFile(rawdata, self._path)
