#!/usr/bin/python
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

__all__ = []

from random import randint, choice
from os import urandom
from base64 import b32encode
from hyperstats.client import Client

def random_id():
    return b32encode(urandom(5))

MAGAZINES = ['Mayfair', 'Playboy', 'Hustler']

def main():
    client = Client('http://localhost:8080/')
    bucket = 'stats'

    while True:
        year = randint(2006, 2012)
        month = randint(1, 12)
        day = randint(1, 28)
        hour = randint(1, 24)        
        facets = dict(time=[year, month, day, hour],
                      device=[choice(['tablet', 'phone']), choice(['apple', 'samsung'])],
                      content=[randint(10, 15), choice(MAGAZINES), 'Issue %d' % randint(1, 20)])
        values = dict(datapoints=1,
                      view_duration=randint(1, 500),
                      revenue=randint(1, 10))
        client.send(bucket, random_id(), facets, values)

if __name__ == "__main__":
    main()