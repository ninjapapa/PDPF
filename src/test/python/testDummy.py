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
from pdpf.pdpfmodulerunner import PdpfModuleRunner
import pdpf
import os
import sys

class DummyTest(PdpfBaseTest):
    def test_dummy(self):
        fqn = "project.modules.M1"
        ctx = self.pdpfCtx
        print("Context version: {}".format(ctx.pdpfVersion()))
        from project.modules import M1
        m = M1(ctx)
        print("Module Versioned FQN: {}".format(m.versioned_fqn))
        self.assertEqual(m.fqn(), fqn)

    def test_requiresDS(self):
        from project.modules import M2
        m = M2(self.pdpfCtx)
        m._resolve()
        print("Module Versioned FQN: {}".format(m.versioned_fqn))

    def test_moduleRunner(self):
        # following should be replaced by DS resolver
        from project.modules import M2
        m = M2(self.pdpfCtx)
        m._resolve()

        # below should be replaced by "df" function in Base
        def run_debug(info):
            log = pdpf.logger
            log.debug("Runinfo {}".format(info))
        [mdata] = PdpfModuleRunner([m], self.pdpfCtx, run_debug).run()

        self.assertEqual(3, mdata)

