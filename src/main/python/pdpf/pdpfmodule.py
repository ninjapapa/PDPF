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
import inspect
import traceback
from datetime import datetime

import pdpf
from pdpf.utils import ABC, is_string, pdpfhash, stripComments, lazy_property
from pdpf.iostrategy import NonOpPersistenceStrategy, ParquetPersistenceStrategy
from pdpf.error import PdpfRuntimeError

from pyspark.sql import DataFrame

def _sourceHash(module):
    src = inspect.getsource(module)
    src_no_comm = stripComments(src)
    # DO NOT use the compiled byte code for the hash computation as
    # it doesn't change when constant values are changed.  For example,
    # "a = 5" and "a = 6" compile to same byte code.
    # co_code = compile(src, inspect.getsourcefile(cls), 'exec').co_code
    return pdpfhash(src_no_comm)


class PdpfGenericModule(ABC):
    """Abstract base class for all SMV modules, including dataset and task modules
    """

    # Python's issubclass() check does not work well with dynamically
    # loaded modules.  In addition, there are some issues with the
    # check, when the `abc` module is used as a metaclass, that we
    # don't yet quite understand.  So for a workaround we add the
    # typcheck in the Smv hierarchies themselves.
    IsPdpfGenericModule = True


    def __init__(self, pdpfCtx):
        self.pdpfCtx = pdpfCtx

        # Set when instant created and resolved
        self.timestamp = None
        self.resolvedRequiresDS = []

        # keep a reference to the result data
        self.data = None

    @classmethod
    def fqn(cls):
        """Returns the fully qualified name
        """
        return cls.__module__ + "." + cls.__name__

    #########################################################################
    # User interface methods
    #
    # - description: Optional, default class docstr
    # - requiresDS: Required
    # - version: Optional, default "0" --- Deprecated!
    #########################################################################

    def description(self):
        return self.__doc__

    @abc.abstractmethod
    def requiresDS(self):
        """User-specified list of dependencies

            Override this method to specify the SmvGenericModule needed as inputs.

            Returns:
                (list(SmvGenericModule)): a list of dependencies
        """
        pass

    @abc.abstractmethod
    def run(self, i):
        """User-specified definition of the operations of this SmvModule

            Override this method to define the output of this module, given a map
            'i' from input SmvGenericModule to resulting DataFrame. 'i' will have a
            mapping for each SmvGenericModule listed in requiresDS. E.g.

            def requiresDS(self):
                return [MyDependency]

            def run(self, i):
                return i[MyDependency].select("importantColumn")

            Args:
                (RunParams): mapping from input SmvGenericModule to DataFrame

            Returns:
                (DataFrame): output of this SmvModule
        """

    def version(self):
        """Version number
            Deprecated!

            Returns:
                (str): version number of this SmvGenericModule
        """
        return "0"


    #########################################################################
    # Methods for sub-classes to implement and/or override
    #
    # - persistStrategy: Required
    # - _dependencies: Optional, default self.requiresDS()
    # - instanceValHash: Optional, default 0
    # - persistStrategy: Optional, default NonOpPersistenceStrategy
    # - _assure_output_type: Optional
    # - doRun: Required
    #########################################################################

    # @abc.abstractmethod
    # def persistStrategy(self):
    #     """Return an SmvIoStrategy for data persisting"""

    def _dependencies(self):
        """Can be overridden when a module has dependency other than requiresDS
        """
        return self.requiresDS()

    def instanceValHash(self):
        """Hash computed based on instance values of the dataset, such as the timestamp of
            an input file

            Returns:
                (int)
        """
        return 0

    def persistStrategy(self):
        """Return an SmvIoStrategy for data persisting"""
        return NonOpPersistenceStrategy()

    # Sub-class implementation of doRun may use RunParams
    class RunParams(object):
        """Map from SmvGenericModule to resulting DataFrame

            We need to simulate a dict from ds to df where the same object can be
            keyed by different datasets with the same fqn. For example, in the
            module

            class X(SmvModule):
                def requiresDS(self): return [Foo]
                def run(self, i): return i[Foo]

            the i argument of the run method should map Foo to
            the correct DataFrame.

            Args:
                (dict): a map from fqn to DataFrame
        """

        def __init__(self, fqn2df):
            self.fqn2df = fqn2df

        def __getitem__(self, ds):
            """Called by the '[]' operator
            """
            if not hasattr(ds, 'fqn'):
                raise TypeError('Argument to RunParams must be an SmvGenericModule')
            else:
                return self.fqn2df[ds.fqn()]

    def _assure_output_type(self, data):
        """Check whether the output of run method is expected by the concrete module
            Raise an exception if check failed
        """
        pass

    def doRun(self, known):
        """Compute this dataset, and return the dataframe"""
        i = self.RunParams(known)
        res = self.run(i)
        self._assure_output_type(res)
        return res

    #########################################################################
    # Internal method for hash of hash calculation
    #########################################################################
    def _sourceCodeHash(self):
        """Hash computed based on the source code of the dataset's class
        """
        res = 0

        cls = self.__class__
        # get hash of module's source code text
        try:
            sourceHash = _sourceHash(cls)
        except Exception as err:  # `inspect` will raise error for classes defined in the REPL
            # Instead of handle the case that module defined in REPL, just raise Exception here
            traceback.print_exc()
            message = "{0}({1!r})".format(type(err).__name__, err.args)
            raise Exception(
                message + "\n" + "SmvGenericModule " +
                self.fqn() + " defined in shell can't be persisted"
            )

        pdpf.logger.debug("{} sourceHash: {}".format(self.fqn(), sourceHash))
        res += sourceHash

        # incorporate source code hash of module's parent classes
        for m in inspect.getmro(cls):
            try:
                # TODO: it probably shouldn't matter if the upstream class is an SmvGenericModule - it could be a mixin
                # whose behavior matters but which doesn't inherit from SmvGenericModule
                if m.IsPdpfGenericModule and m != cls and not m.fqn().startswith("pdpf."):
                    res += m(self.pdpfCtx)._sourceCodeHash()
            except:
                pass

        return res

    def _dataset_hash(self):
        """current module's hash value, depend on code and potentially
            linked data (such as for SmvCsvFile)
        """
        log = pdpf.logger
        _instanceValHash = self.instanceValHash()
        log.debug("{}.instanceValHash = {}".format(self.fqn(), _instanceValHash))

        _sourceCodeHash = self._sourceCodeHash()
        log.debug("{}.sourceCodeHash = ${}".format(self.fqn(), _sourceCodeHash))

        res = _instanceValHash + _sourceCodeHash

        # ensure python's numeric type can fit in a java.lang.Integer
        return res & 0x7fffffff

    def _resolve(self):
        """dummy resolve
        """
        self.resolvedRequiresDS = [M(self.pdpfCtx) for M in self.requiresDS()]
        return self

    @lazy_property
    def _hash_of_hash(self):
        """hash depends on current module's _dataset_hash, and all ancestors.
            this calculation could be expensive, so made it a lazy property
        """
        # TODO: implement using visitor too
        log = pdpf.logger
        _dataset_hash = self._dataset_hash()
        log.debug("{}.dataset_hash = {}".format(self.fqn(), _dataset_hash))

        res = _dataset_hash
        for m in self.resolvedRequiresDS:
            res += m._hash_of_hash
        log.debug("{}.hash_of_hash = {}".format(self.fqn(), res))
        return res

    def _ver_hex(self):
        return "{0:08x}".format(self._hash_of_hash)

    @lazy_property
    def versioned_fqn(self):
        """module fqn with the hash of hash. It is the signature of a specific
            version of the module
        """
        return "{}_{}".format(self.fqn(), self._ver_hex())

    #########################################################################
    # Internal method for run module
    #########################################################################
    def _populate_data(self, fqn2df):
        """Entry point for the module runner
            create module data
            fqn2df will be appended
        """
        res = self._computeData(fqn2df)
        fqn2df.update({self.fqn(): res})
        return None

    def _do_action_on_df(self, func, df, desc):
        log = pdpf.logger
        log.info("STARTING {} on {}".format(desc, self.fqn()))

        before  = datetime.now()

        res = func(df)

        after   = datetime.now()
        duration = (after - before)
        secondsElapsed = duration.total_seconds()

        log.info("COMPLETED {}: {}".format(desc, self.fqn()))
        log.info("RunTime: {}".format(duration))

        return (res, secondsElapsed)


    def _computeData(self, fqn2df):
        """When DF is not in cache, do the real calculation here
        """
        pdpf.logger.debug("compute: {}".format(self.fqn()))
        _strategy = self.persistStrategy()
        if (not _strategy.isPersisted()):
            df = self.doRun(fqn2df)
            # Acquire lock on persist to ensure write is atomic
            # with self._smvLock():
            (res, persistingTimeElapsed) = self._do_action_on_df(
                _strategy.write, df, "RUN & PERSIST OUTPUT")
            # Need to populate self.data, since postAction need it
            if (_strategy.isPersisted()):
                # NonOp write will do nothing, so still not persisted
                self.data = _strategy.read()
            else:
                self.data = df
        else:
            pdpf.logger.debug("{} had a persisted file".format(self.fqn()))
            self.data = _strategy.read()

        return self.data

    def _is_persisted(self):
        return self.persistStrategy().isPersisted()

class PdpfSparkDfMod(PdpfGenericModule):
    """Base class for Spark DFs
    """
    # Override this to add the task to a Spark job group
    def _do_action_on_df(self, func, df, desc):
        name = self.fqn()
        self.pdpfCtx.sc.setJobGroup(groupId=name, description=desc)
        (res, secondsElapsed) = super(PdpfSparkDfMod, self)._do_action_on_df(func, df, desc)

        # Python api does not have clearJobGroup
        # set groupId and description to None is equivalent
        self.pdpfCtx.sc.setJobGroup(groupId=None, description=None)
        return (res, secondsElapsed)

    def _persist_path(self):
        name = self.versioned_fqn
        return "{}/{}.parquet".format(self.pdpfCtx.tmpDataDir, name)

    def persistStrategy(self):
        return ParquetPersistenceStrategy(self.pdpfCtx, self._persist_path())

    def _assure_output_type(self, run_output):
        if (not isinstance(run_output, DataFrame)):
            raise PdpfRuntimeError(
                'The output data from this module should be a Spark DataFrame, but {} is given.'.format(type(run_output))
            )

