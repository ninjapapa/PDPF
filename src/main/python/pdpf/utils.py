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
import sys
import re

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

def is_string(obj):
    """Check whether object is a string type with Python 2 and Python 3 compatibility
    """
    # See http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
    try:
        return isinstance(obj, basestring)
    except:
        return isinstance(obj, str)

def pdpfhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate sourceCodeHash.
    """
    import binascii

    # Python 2* has "str" type the same as bytes, while Python 3 has
    # to covert "str" to bytes through "str".encode("utf-8")
    if sys.version_info >= (3, 0):
        byte_str = text.encode("utf-8")
    else:
        byte_str = text

    return binascii.crc32(byte_str)

def stripComments(code):
    import re
    return re.sub(r'(?m)^ *(#.*\n?|[ \t]*\n)', '', code)

def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

