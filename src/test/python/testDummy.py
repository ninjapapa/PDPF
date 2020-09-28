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
import os

class DummyTest(PdpfBaseTest):
    def test_dummy(self):
        fqn = "stage1.modules.B"
        ctx = self.pdpfCtx
        print(ctx.pdpfVersion())
        self.assertEqual("stage1.modules.B", fqn)
