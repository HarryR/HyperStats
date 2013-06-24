"""
Copyright (c) 2013, G Roberts
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the project nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__all__ = ['make_facet_id', 'flatten_facet', 'all_permutations', 'Daemon',
           'QueueDaemon', 'unixtime', 'to_utf8_str', 'split_facet']

from base64 import b64encode
from time import time as unixtime
import hashlib, marshal, signal, json, logging

LOG = logging.getLogger(__name__)

class DataObject(object):
    """
    http://www.saltycrane.com/blog/2012/08/python-data-object-motivated-desire-mutable-namedtuple-default-values/
    
    An object to hold data. Motivated by a desire for a mutable namedtuple with
    default values. To use, subclass, and define __slots__.

    The default default value is None. To set a default value other than None,
    set the `default_value` class variable.

    Example:
        class Jello(DataObject):
            default_value = 'no data'
            __slots__ = (
                'request_date',
                'source_id',
                'year',
                'group_id',
                'color',
                # ...
            )
    """
    __slots__ = ()
    default_value = None

    def __init__(self, *args, **kwargs):
        # Set default values
        for att in self.__slots__:
            setattr(self, att, self.default_value)

        # Set attributes passed in as arguments
        for k, v in zip(self.__slots__, args):
            setattr(self, k, v)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def asdict(self):
        return dict(
            (att, getattr(self, att)) for att in self.__slots__)
n
    def astuple(self):
        return tuple(getattr(self, att) for att in self.__slots__)

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join('{}={}'.format(
                    att, repr(getattr(self, att))) for att in self.__slots__))

def to_utf8_str(obj):
    """
    Converts argument into a utf-8 byte string
    """
    if isinstance(obj, basestring):
        if isinstance(obj, unicode):
            return obj.encode('utf-8', 'ignore')
        # Otherwise.. must be a str() which is already bytes
        return obj
    elif isinstance(obj, int):
        return "%d" % (obj,)
    elif isinstance(obj, float):
        return "%f" % (obj)
    raise TypeError, "Cannot convert type '%s' to utf-8 string" % (type(obj),)

def split_facet(facet):
    """
    Returns a dictionary of the following elements:

     - id: Hashed facet ID
     - parent_id: ID of the parent facet
     - child: Full value of the last item from the facet
    """
    assert type(facet) in [list, set]    
    facet = flatten_facet(facet)
    facet_id = make_facet_id(facet)
    facet_parent = facet[:-1]
    if len(facet_parent) == len(facet):
        facet_child = ''
    else:
        facet_child = to_utf8_str(facet[-1])
    facet_parent_id = make_facet_id(facet_parent)
    return {
        'id': facet_id,
        'parent_id': facet_parent_id,
        'child': facet_child
    }

def make_facet_id(facet):
    """
    Unique ID for the facet
    """    
    if type(facet) not in [list, tuple]:
        facet = [facet]
    hasher = hashlib.new('sha1')
    for value in facet:
        hasher.update(to_utf8_str(value))
    return b64encode(hasher.digest()[:9])

def flatten_facet(facets_to_flatten):
    """
    Flattens a list of lists into a single list.

        [['derp', 123], ['merp', 456]]

    becomes

        ['derp', 123, 'merp', 456]
    """
    out = []
    for facet in facets_to_flatten:
        out += facet
    return out

def power_set(inputs, minlength=1):
    count = len(inputs)
    members = int(pow(2, count))
    # Ewww... we're formatting the number into binary to work out which
    # entries to output
    bstr = '{' + "0:0{}b".format(count) + '}'
    ret = []
    for i in range(0, members):
        b = bstr.format(i)
        out = []
        for j in range(0, count):
            if b[j] == '1':
                out.append(inputs[j])
        if len(out) >= minlength:
            ret.append(out)
    return ret

def permute(dims, i=0, chain=None):
    if chain is None:
        chain = []
    if i >= len(dims):
        return [chain]
    chains = []
    for l in dims[i]:
        chains += permute(dims, i + 1, chain + [l])
    return chains

def all_permutations(inputs):
    """
    All permutations of the input dictionary.

        {'derp': [123, 456], 'merp': 987}

    Will create output like:

        [['derp', 123, 456],
         ['derp', 123],
         ['merp', 987],
         ['merp', 987, 'derp', 123],
         ['merp', 987, 'derp', 123, 456]]

    Values can be floats, ints and strings.
    Or they can be lists of ints, floats and strings.
    """
    sets = {}
    if type(inputs) == dict:
        inputs = inputs.items()
    for key, levels in inputs:
        combos = []
        for level in levels:
            combos.append(level)
            if key not in sets:
                sets[key] = []
            sets[key].append([key] + combos)
    all_points = power_set(sets.values())
    ret = []
    for lol in all_points:
        ret += permute(lol)
    return ret


class Daemon(object):
    """
    This daemon takes records from the `queue` list in Redis and inserts into
    HyperDex.
    """
    def __init__(self):
        self._stop = False
        self._status = {
            'last': unixtime(),
            'interval': 2
        }
        self._stats = {}
        signal.signal(signal.SIGINT, self._signal_handler)

    def is_stopping(self):
        """
        Break the run() loop at the next possible opportunity
        """
        return self._stop

    def _signal_handler(self, _sig, _frame=None):
        """
        Stop daemon gracefully
        """
        print "SIGINT caught, stopping gracefully"
        self._stop = True

    def incr_stats(self, name, value=1):
        """
        Increment the named counter
        """
        self._stats[name] = self._stats.get(name, 0) + value

    def show_status(self):
        """
        Display a summary line
        """
        status = self._status
        stats = self._stats
        now = unixtime()
        if (now - status['last']) > status['interval']:
            status['last'] = now
            print 'now:', ' | '.join(['%s:%d' % (key, value) for key, value in stats.items()])
            self._stats = {key: 0 for key in stats.keys()}

class QueueDaemon(Daemon):
    """
    Waits for entries on a Redis queue and hands the recors to the process()
    funtion. The records must be a 'marshal' encoded dictionary with both 'id'
    and 'ttl' keys.
    """
    def __init__(self, rdb):
        """
        :param rdb: StrictRedis instance
        """
        assert rdb is not None
        super(QueueDaemon, self).__init__()
        self._rdb = rdb

    @property
    def redis(self):
        return self._rdb

    def process(self, record):
        """
        Process a single record

        :param record: Dictionary
        """
        raise NotImplementedError

    def _handle(self, data):
        """
        Grunt work, wrapper for the 'process' method.

        Handles re-queueing of items which couldn't be processed.
        """
        self.incr_stats('popped')
        try:
            record = marshal.loads(data)
        except ValueError:
            record = None
        if record is None:
            self.incr_stats('invalid')
            return
        
        is_processed = False
        try:
            is_processed = self.process(record)
        except Exception:
            LOG.error("Failed to process", exc_info=True)

        # Failed processing for some reason
        if not is_processed:
            # Put the CDR back in queue for processing if process fails                
            record['ttl'] = record.get('ttl', 0) + 1
            if record['ttl'] > 3:
                # But only 3-4 times... then it's 'fucked'
                # XXX: how do we handle 'fucked' items?
                self.redis.rpush('queue_fucked', json.dumps(record))
                self.incr_stats('fucked')
            else:
                self.redis.rpush('queue', json.dumps(record))                
                self.incr_stats('retry')
            self.incr_stats('redis.ops.rpush')
            self.incr_stats('redis.ops')
        else:
            # TODO: insert the 'cost' of processing this record
            self.redis.rpush(record['id'], unixtime())
            self.redis.expire(record['id'], 2)
            self.incr_stats('processed')
            self.incr_stats('redis.ops.rpush')
            self.incr_stats('redis.ops.expire')
            self.incr_stats('redis.ops', 2)

    def run(self, queue_name):
        """
        Listen for messages on the 'cdrpickup' channel and process them.

        Loops forever.
        """      
        while not self.is_stopping():
            # TODO: blpoprpush onto a 'working' list
            #       then move to a 'done' list
            #       must be uber reliable!
            msg = self.redis.blpop([queue_name], timeout=1)
            self.incr_stats('redis.ops.blpop')
            self.incr_stats('redis.ops')
            if msg is not None and len(msg) == 2:                
                self._handle(msg[1])
            self.show_status()
        print "stopped"