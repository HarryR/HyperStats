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

__all__ = ['main']

from hyperstats.connection import redis_connect, hyperdex_connect
from hyperstats.common import to_utf8_str, unixtime, split_facet
import logging, json, bottle, marshal

LOG = logging.getLogger(__name__)
RDB = redis_connect()
HDEX = hyperdex_connect()

class ValidationError(Exception):
    """
    Data the user provided isn't sane.
    """
    pass


def sanitized_facets(input_facets):
    """
    Sanitize all the facets.

    It must be a dictionary of scalar values, or a dictionary of lists of scalar
    values.

        e.g. {'derp': 123, 'merp': [123, "hello"]}

    """
    if type(input_facets) != dict:
        raise ValidationError('"facets" must be dictionary')

    facets = []
    for facet_name, facet_values in input_facets.items():
        if type(facet_name) not in [str, unicode]:
            raise ValidationError("Facet names must be strings")

        facet_name = to_utf8_str(facet_name)
        if type(facet_values) in [str, unicode, int, float]:
            facet_values = [to_utf8_str(facet_values)]
        if type(facet_values) not in [list, set]:
            raise ValidationError("Facet values for '%s' must be a string, int or float"
                                  " - or a list containing only strings, ints and floats" % (facet_name,))
        for i in range(0, len(facet_values)):
            value = facet_values[i]
            if type(value) not in [int, str, unicode, float]:
                raise ValidationError("Facet '%s[%d]' must be a string, int or float" % (facet_name, i))                
        facets.append((facet_name, [to_utf8_str(tmp_value) for tmp_value in facet_values]))

    return sorted(facets, key=lambda x: x[0])


def sanitized_values(input_values):
    """
    Sanitize all values, it must be a dictionary of values
    """
    if type(input_values) != dict:
        raise ValidationError('"values" must be dictionary')

    values = []
    for value_name, value in input_values.items():
        if type(value_name) not in [str, unicode]:
            raise ValidationError("Value names must be strings")        
        if type(value) != int:
            raise ValidationError("Value '%s' must be an integer" % (value_name,))
        values.append((to_utf8_str(value_name), int(value)))

    return sorted(values, key=lambda x: x[0])


def make_record(data):
    """
    Make a record to be inserted into a bucket
    """
    if data is None:
        raise ValidationError('No data received')

    if type(data) != dict or 'facets' not in data or 'values' not in data:
        raise ValidationError('Data must be dictionary with "id", "facets" and "values" keys')    

    if 'id' not in data or type(data['id']) not in [int, str, unicode]:
        raise ValidationError('The "id" must be a string or int')

    record_id = to_utf8_str(data['id'])
    facets = sanitized_facets(data['facets'])
    values = sanitized_values(data['values'])    

    return {
        'id': record_id,
        'facets': facets,
        'values': values,
    }

def handle_error(httperror):
    response = bottle.response
    response.set_header('content-type', 'application/json')
    return json.dumps({'ok': False,
                       'status': httperror.status,
                       'msg': httperror.output})

@bottle.error(code=500)
def handle_error500(httperror):
    return handle_error(httperror)

@bottle.error(code=400)
def handle_error400(httperror):
    return handle_error(httperror)

@bottle.route('/<bucket:re:[a-z]+>/find-values', method=['POST'], name='find_values')
def find_values(bucket):
    """
    Retrieves a list of the facet values underneath a parent facet.

    It accepts a JSON dictionry of the parent facet names and a limit on how 
    many sub-facets to return.

        {
            'tablet-days': {'facet': {'device': ['tablet', 'apple'],
                                      'time': ['2013', '1', '15']},
                            'limit': 50}
        }

    On success it will return the a standard API response including the
    'results' key which contains a 

        {
            'ok': true,
            'status': 200,
            'results': {
                'tablet-days': {'1': {'duration': 38289323,
                                      'datapoints': 4919}},
                               {'2': {'duration': 382829,
                                      'datapoints': 1234}}
            }
        }
    """
    start_time = unixtime()
    request = bottle.request
    query = request.json

    if query is None or type(query) != dict:
        bottle.abort(400, 'Must POST application/json dictionary')

    # Validate the searches
    searches = {}
    for name, search in query.items():
        if 'facet' not in search:
            bottle.abort(400, '"facet" key required for "%s"' % (name,))
        if 'limit' not in search:
            bottle.abort(400, '"limit" key required for "%s"' % (name,))
        try:
            limit = int(search.get('limit'))
        except ValueError:
            bottle.abort(400, 'Invalid "limit" for "%s"' % (name,))

        withvalues = bool(search.get('withvalues'))

        startkey = None
        if 'startkey' in search:
            try:
                startkey = to_utf8_str(search['startkey'])
            except TypeError:
                bottle.abort(400, 'Start key for "%s" is invalid type' % (name,))

        try:
            facet = sanitized_facets(search['facet'])
        except ValidationError, oops:
            LOG.info("'%s' contained invalid facet", name, exc_info=True)
            bottle.abort(400, "%s: %s" % (name, oops.message))
        searches[name] = {
            'facet': split_facet(facet),
            'limit': limit,
            'startkey': start,
            'withvalues': withvalues
        }

    # Perform searches    
    all_results = {}
    for name, search in searches.items():
        predicate = {
            'facet_parent_id': search['facet']['parent_id'],            
        }
        limit = search['limit']
        startkey = search['startkey']
        if startkey is not None:
            predicate['facet'] = hyperclient.GreaterEqual(startkey)
            # TODO: add NotEqual for the 'start' value too
            limit += 1
        withvalues = search['withvalues']
        results = {} if withvalues else []
        searchiter = HDEX.sorted_search(bucket, predicate, 'facet', limit, 'min')
        for result in searchiter:
            facet = result['facet']
            if facet == startkey:
                continue
            if withvalues:
                results[facet] = result['values']
            else:
                results.append(facet)
        all_results[name] = results

    end_time = unixtime()

    return {
        'ok': True,
        'status': 200,
        'results': results,
        'time': end_time - start_time
    }


@bottle.route('/<bucket:re:[a-z]+>/get-values', method=['POST'], name='get_values')
def get_values(bucket):
    """
    Retrieves the values stored in a single facet.

    It accepts a JSON dictionary of query names to facets, e.g.:

        {
            'tablet-dayone': {'device': ['tablet', 'apple'],
                              'time': ['2013', '1', '15']}
            'tablet-daytwo': {'device': ['tablet', 'apple'],
                              'time': ['2013', '1', '16']}
        }

    On success it will respond with the an API standard response including the
    'results' key, a dictionary of the values queried for, e.g.:

        {
            'ok': true,
            'status': 200,
            'results': {
                'tablet-dayone': {
                    'duration': 1212421,
                    'datapoints': 238282
                },
                'tablet-daytwo': {
                    'duration': 3292382,
                    'datapoints': 38283
                }
            }
        }

    This allows effecient retrieve of any number of facet values from  the DB.
    """
    start_time = unixtime()
    request = bottle.request
    query = request.json

    if query is None or type(query) != dict:
        bottle.abort(400, 'Must POST application/json dictionary')

    facet_keys = {}
    results = {}

    # Prepare facets for query
    for key, facet in query.items():
        try:
            facet = sanitized_facets(query['facets'])
        except ValidationError, oops:
            LOG.info("Query for '%s' contained invalid facet", key, exc_info=True)
            bottle.abort(400, "%s: %s" % (key, oops.message))
        facet_keys[key] = split_facet(facet)    

    # Retrieve values from databases
    try:
        for key, facet_key in facet_keys.items():
            data = HDEX.get(bucket, facet_key['id'])
            if data is None:
                results[key] = None
            results[key] = data['values']
    except Exception:
        LOG.error('Failed to retrieve facets', exc_info=True)
        bottle.abort(500, 'Could not retrieve facets')

    end_time = unixtime()

    return {
        'ok': True,
        'status': 200,
        'results': results,
        'time': end_time - start_time
    }

@bottle.route('/<bucket:re:[a-z]+>', method=['POST', 'PUT'], name='sink')
def sink(bucket):
    """
    Allows clients to submit records via HTTP
    """
    start_time = unixtime()
    request = bottle.request
    data = request.json

    try:
        record = make_record(data)
    except ValidationError, oops:
        LOG.info('Input data failed sanitization checks', exc_info=True)
        bottle.abort(400, oops.message)
    except Exception:
        LOG.error('Failed to create record', exc_info=True)
        bottle.abort(500, 'Could not pre-process data')

    try:
        RDB.rpush('aggqueue', marshal.dumps(record))        
    except Exception:
        LOG.error("Failed to insert data", exc_info=True)
        bottle.abort(500, 'Server Error, data not inserted')

    end_time = unixtime()

    return {
        'ok': True,
        'status': 200,
        'id': record['id'],
        'time': end_time - start_time
    }

def main(args):    
    bottle.run(host='localhost', port=8080, server='gevent')

if __name__ == "__main__":
    main()