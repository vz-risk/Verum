#!/usr/bin/env python
# encoding: utf-8
"""
cymru_api.py
"""
#  From: https://gist.github.com/zakird/11196064

import sys
import os
import socket
import unittest

class CymruIPtoASNResult(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __str__(self):
        return "<CymruIPtoASNResult (%s)>" % self.ip_address

    __repr__ = __str__

class CymruIPtoASNService(object):
    URL = "whois.cymru.com"

    """Whois   Netcat          Action
            begin           enable bulk input mode          (netcat only)
            end             exit the whois/netcat client    (netcat only)
    -p      prefix          include matching prefix
    -q      noprefix        disable matching prefix (default)
    -c      countrycode     include matching country code
    -d      nocountrycode   disable country codes (default)
    -n      asname          include asnames (default)
    -o      noasname        disable asnames
    -r      registry        display matching registry
    -s      noregistry      disable registry display (default)
    -a      allocdate       enable allocation date
    -b      noallocdate     disable allocation date (default)
    -t      truncate        truncate asnames (default)
    -u      notruncate      do not truncate asnames
    -v      verbose         enable all flags (-c -r -p -a -u -a)
    -e      header          enable column headings (default)
    -f      noheader        disable column headings 
    -w      asnumber        include asnumber column (default)
    -x      noasnumber      disable asnumber column (will not work for IP mappings)
    -h      help            this help message"""

    def __init__(self):
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket.connect((self.URL, 43))

    def _gen_query(self, queries):
        lines = []
        lines.append("begin")
        lines.append("verbose")
        lines.extend(queries)
        lines.append("end\r\n")
        return "\n".join(lines)

    def _send_query(self, query):
        self.__socket.sendall(query)
        self.__socket.shutdown(socket.SHUT_WR)
        response = ''
        while True:
            r = self.__socket.recv(16)
            if r and r != '':
                response = ''.join((response, r))
            else:
                break
        return response

    LABELS = (
        'as_number',
        'ip_address',
        'bgp_prefix',
        'country',
        'registry',
        'allocated_at',
        'as_name'
    )

    def _parse_response(self, response):
        for line in response.split("\n"):
            if line.startswith("Bulk mode;") or line == '':
                continue
            else:
                clean = map(lambda v: v.rstrip().lstrip(), line.split('|'))
                yield CymruIPtoASNResult(**dict(zip(self.LABELS, clean)))

    def query(self, queries):
        query = self._gen_query(queries)
        response = self._send_query(query)
        results = self._parse_response(response)
        for r in results:
            yield r

    def query_one(self, query):
        return list(self.query([query,]))[0]

class CymruIptoASNServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = CymruIPtoASNService()

    def testOne(self):
        # expect the following:
        # ['3676', '128.255.1.1', '128.255.0.0/16', 'US', 'arin', '1987-06-05',
        # 'UIOWA-AS - University of Iowa']
        r = self.service.query_one("128.255.1.1")
        self.assertEquals(r.as_number, '3676')
        self.assertEquals(r.ip_address, "128.255.1.1")
        self.assertEquals(r.country, "US")
        self.assertEquals(r.registry, "arin")
        self.assertEquals(r.as_name, "UIOWA-AS - University of Iowa")

    def testMultiple(self):
        rs = list(self.service.query(["128.255.1.1", "141.212.1.1"]))
        self.assertEquals(rs[0].as_number, '3676')
        self.assertEquals(rs[1].as_number, '36375')

    def testFailure(self):
        pass

if __name__ == '__main__':
    unittest.main()