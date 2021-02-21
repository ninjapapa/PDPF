from pdpf import *
from pyspark.sql.types import StructField, StructType, StringType

class M1(PdpfSparkDfMod):
    def requiresDS(self):
        return []

    def run(self, i):
        schema = StructType([ StructField('str', StringType(), False) ])
        df = self.pdpfCtx.sparkSession.createDataFrame(
            [{ 'str': 'test_str' }],
            schema
        )
        return df
