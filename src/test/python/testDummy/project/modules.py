from pdpf import *

class M1(PdpfGenericModule):
    def requiresDS(self):
        return []

    def version(self):
        return "test_v1"
