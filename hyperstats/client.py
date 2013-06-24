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

__all__ = ['ClientException', 'Client']

import requests, json

class ClientException(Exception):
    pass

class Client(object):
    """
    The Client allows you to send data to the HyperStats system in the 
    most effecient way possible.
    """
    def __init__(self, api_url):
        assert isinstance(api_url, basestring)
        self._api_url = api_url.rstrip('/ ')
        self._session = requests.Session()

    def send(self, bucket, guid, facets, values):
        """
        :param bucket: Name of data bucket to insert into
        :param guid: Unique ID for this record
        :param facets: Dictionary of facets
        :param values: Dictionary of values
        """
        assert isinstance(bucket, basestring)
        assert isinstance(guid, basestring)
        assert type(facets) == dict
        assert type(values) == dict
        payload = {
            'id': guid,
            'facets': facets,
            'values': values
        }
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json'}
        url = '%s/%s' % (self._api_url, bucket)
        response = self._session.post(url, data=json.dumps(payload),
                                           headers=headers)

        data = response.json()
        if data.get('ok', False):
            return data

        raise ClientException([data.get('status', 1),
                               data.get('msg', 'Unknown Error')])