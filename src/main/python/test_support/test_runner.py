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
import sys
from unittest import *
from pyspark.sql import SparkSession

class SmvTestRunner(object):
    """Runs SMV tests

        DOES NOT reload code. If code needs to be reloaded before running tests
        (as in smv-pyshell), this must happen before the run method is called.
    """
    def __init__(self, test_path):
        # The path where the test modules with be found
        self.test_path = test_path

    def run(self, test_names):
        """Run the tests with the given names

            Args:
                test_names (arr(str)): Tests to run. If empty, all tests will be run.
        """
        loader = TestLoader()
        sys.path.append(self.test_path)

        if len(test_names) == 0:
            suite = loader.discover(self.test_path)
        else:
            suite = loader.loadTestsFromNames(test_names)


        result = TextTestRunner(verbosity=2).run(suite)
        sys.path.remove(self.test_path)
        print("result is ", result)
        return len(result.errors) + len(result.failures)


class TestConfig(object):

    @classmethod
    def sparkSession(cls):
        if not hasattr(cls, "spark"):
            #hivedir = "file://{0}/{1}/smv_hive_test".format(tempfile.gettempdir(), getpass.getuser())
            spark_builder = (SparkSession.builder
                .master("local[*]")
                .appName("PDPF Test")
                # force this conf to avoid error of `Hive support is required to CREATE Hive TABLE`
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.driver.memory", "1G")
                .config("spark.executor.memory", "1G")
                .config("spark.sql.test", "")
                .config("spark.sql.hive.metastore.barrierPrefixes",
                        "org.apache.spark.sql.hive.execution.PairSerDe")
                #.config("spark.sql.warehouse.dir", hivedir)\
                .config("spark.ui.enabled", "false")
            )

            cls.spark = spark_builder.getOrCreate()

        return cls.spark

    # test names specified via command line
    @classmethod
    def test_names(cls):
        if not hasattr(cls, '_test_names'):
            cls.parse_args()
        return cls._test_names

    @classmethod
    def test_path(cls):
        if not hasattr(cls, '_test_path'):
            cls.parse_args()
        return cls._test_path

    # Parse argv to split up the the smv args and the test names
    @classmethod
    def parse_args(cls):
        args = sys.argv[1:]
        test_names = []
        test_path = "./src/test/python"
        while(len(args) > 0):
            next_arg = args.pop(0)
            if(next_arg == "-t"):
                test_names.append( args.pop(0) )
            elif(next_arg == "-d"):
                test_path = args.pop(0)

        cls._test_names = test_names
        cls._test_path = test_path
