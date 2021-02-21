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
import pdpf
from pdpf.modulesvisitor import ModulesVisitor

class PdpfModuleRunner(object):
    """Represent the run-transaction. Provides the single entry point to run
        a group of modules
    """
    def __init__(self, modules, pdpfCtx, runMonitorCallback=None):
        self.roots = modules
        self.pdpfCtx = pdpfCtx
        self.log = pdpf.logger
        self.visitor = ModulesVisitor(modules)
        self.runMonitorCallback = runMonitorCallback

    def run(self):
        # a set of modules which need to run post_action, keep tracking
        # to make sure post_action run one and only one time for each TX
        # the set will be updated by _create_df, _create_meta and _force_post
        # and eventually be emptied out
        # See docs/dev/SmvGenericModule/SmvModuleRunner.md for details
        mods_to_run_post_action = set(self.visitor.modules_needed_for_run)

        # a map from fqn to already run DF, since the `run` interface of
        # SmvModule takes a map of class => df, the map here have to be
        # keyed by class method instead of `versioned_fqn`, which is only
        # in the resolved instance
        known = {}

        # Do the real module calculation, when there are persistence, run
        # the post_actions and ancestor ephemeral modules post actions
        self._create_df(known)

        dfs = [m.data for m in self.roots]
        return dfs

    def _create_df(self, known):
        # run module and create df. when persisting, post_action
        # will run on current module and all upstream modules
        def runner(m, state):
            (fqn2df) = state
            fqn = m.fqn()
            # tell monitor m is running
            if (self.runMonitorCallback is not None):
                self.runMonitorCallback({
                    'fqn': fqn,
                    'status': 'started',
                    'applicationId': self.pdpfCtx.sc.applicationId
                })
            # Run module
            m._populate_data(fqn2df)
            # tell monitor m is done
            if (self.runMonitorCallback is not None):
                self.runMonitorCallback({
                    'fqn': fqn,
                    'status': 'completed',
                    'applicationId': self.pdpfCtx.sc.applicationId
                })

        self.visitor.dfs_visit(runner, (known), need_to_run_only=True)
