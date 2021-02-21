from pdpf import *

class M1(PdpfGenericModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return 1

    def version(self):
        return "test_v1"


class M2(PdpfGenericModule):
    def requiresDS(self):
        return [M1]

    def run(self, i):
        res = i[M1] + 2
        return res
