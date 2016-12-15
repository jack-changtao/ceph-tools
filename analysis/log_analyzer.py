#!/usr/bin/python

import gzip
import os
import os.path
import re
import sys
import datetime
from datetime import datetime

tracker_regex = re.compile('.*op tracker -- seq: ([0-9]+), time: (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d), event: (.*), op: (.*)\((client\S* )')
# 0, duration event,op
all_stat={}

def wrapgz(gfilename):
    def retval():
        gfile = gzip.open(gfilename, 'rb')
        gfile.__exit__ = lambda: gfile.close()
        return gfile
    return (gfilename, retval)

def wrap(filename):
    def retval():
        nfile = open(filename, 'rb')
        return nfile
    return (filename, retval)

def get_logs(path):
    output = {}
    output['config'] = lambda: open(os.path.join(path, 'config.yaml'), 'r')
    output['osd'] = {}
    output['client'] = {}
    for path, dirs, files in os.walk(os.path.abspath(path)):
        for filename in files:
            match = re.match('ceph-osd.([0-9]+).log.gz', filename)
            if match:
                fn = os.path.join(path, filename)
                output['osd'][int(match.group(1))] = wrapgz(
                    os.path.join(path, filename))
            match = re.match('ceph-osd.([0-9]+).log', filename)
            if match and not int(match.group(1)) in output['osd']:
                fn = os.path.join(path, filename)
                output['osd'][int(match.group(1))] = wrap(
                    os.path.join(path, filename))
            match = re.match('client.([0-9]+).log.gz', filename)
            if match:
                fn = os.path.join(path, filename)
                output['client'][int(match.group(1))] = wrapgz(
                    os.path.join(path, filename))
            match = re.match('client.([0-9]+).log', filename)
            if match and not int(match.group(1)) in output['client']:
                fn = os.path.join(path, filename)
                output['client'][int(match.group(1))] = wrap(
                    os.path.join(path, filename))
    return output

def parse_tracker_line(line):
    retval = {}
    match = tracker_regex.match(line)
    if match:
        retval['seq'] = int(match.group(1))
        retval['time'] = datetime.strptime(
            match.group(2), '%Y-%m-%d %H:%M:%S.%f'
            )
        retval['event'] = match.group(3)
        retval['op'] = match.group(4)
        retval['reqid'] = match.group(5)
        return retval
    return None

class Request:
    def __init__(self):
        self.parsed = []
        self.events = []
        self.last_event = None
        self.first_event = None
        self._primary = -1
        self.osds = []
        
    def is_skip_events(self, event):
	#skip_events = set(['waiting for rw locks','send apply ack','op_applied','done'])
        skip_events = set(['send apply ack','op_applied','done'])
        if event in skip_events:
	   return True
        return False
    def add_event(self, parsed):
	if self.is_skip_events(parsed['event']) == True:
	   return 
        if self.parsed == []:
            self.last_event = parsed['time']
            self.first_event = parsed['time']
         
        self.parsed.append(parsed)
        self.events.append((parsed['time'], parsed['event'], parsed['osd'], parsed['op']))
        self.events.sort()
        if self.last_event < parsed['time']:
            self.last_event = parsed['time']
        if self.first_event > parsed['time']:
            self.first_event = parsed['time']
        if parsed['event'] == 'op_applied':
            self._primary = parsed['osd']
        if parsed['osd'] not in self.osds:
            self.osds.append(parsed['osd'])
            self.osds.sort()

    def duration(self):
        return (self.last_event - self.first_event).total_seconds() * 1000

    def __repr__(self):
        return str(self.events) + " " + \
               str(self.duration()) + " " + self.parsed[0]['reqid']

    def pretty_print(self):
        outstr = "reqid: %s, duration: %sms, events:%d "%(
            self.parsed[0]['reqid'],str(self.duration() ),len(self.events))
        outstr += "\n=====================\n"
        count = 0
        for (time, event, osd, op) in self.events:
            if(count==0):
                last_time = time
            duration = (time - last_time).total_seconds() * 1000
            outstr += "duration(%sms)\t%s\t(osd.%s):\t%s,\t%s\n"%(str(duration),str(time), str(osd), event, op)
            last_time = time
            count= count + 1
        outstr += "=====================\n"
        return outstr
    def add_stat(self):
        count = 0
        for (time, event, osd, op) in self.events:
            if(count==0):
                last_time = time
            duration = (time - last_time).total_seconds() * 1000
            #print event
            if all_stat.has_key(count) == False :
                all_stat[count] = [0,'event','op']
            d = all_stat[count][0]
            d += duration
            all_stat[count] = [d,event,op]
            last_time = time
            count = count + 1
    def get_events_num(self):
        return len(self.events)
    def primary(self):
        return self._primary

    def replicas(self):
        return self.osds


def get_request(logs):
    requests = {}
    for i, (fn, func) in logs['osd'].iteritems():
      with func() as f:
        for line in f.readlines():
            parsed = parse_tracker_line(line)
            if not parsed or parsed['reqid'] == 'unknown.0.0:0':
                continue
            parsed['osd'] = i
            if parsed['reqid'] not in requests:
                requests[parsed['reqid']] = Request()
            requests[parsed['reqid']].add_event(parsed)
    return requests

def get_osds_info(requests):
    all_requests = [(i.duration(), i) for i in requests.itervalues()]
    all_requests.sort()

    pairs = {}
    for _, i in all_requests:
       if tuple(i.replicas()) not in pairs:
          pairs[tuple(i.replicas())] = 0
       pairs[tuple(i.replicas())] += 1
    print pairs

    osds = {}
    for _, i in all_requests:
       if i.primary() not in osds:
          osds[i.primary()] = 0
       osds[i.primary()] += 1
    print osds

#fileter events num
# if num_events ==0 , stat all events
def get_stat(requests,num_events):
    skip=0
    for i in requests.itervalues():
       num = i.get_events_num()
       if( num_events!=0 and  num != num_events):
          skip += 1
          continue
       i.add_stat()
       d = i.duration()
       if d >= 4 :  # print duration > 4 ms
          print i.pretty_print()
    return skip
 
def dump_stat(requests,skip):
    num = len(requests)
    num -= skip

    print "************ All stat info  count:%d  ************************" %(num)
    length=len(all_stat)
    for i in range(0,length):
       print "avg duraion:%.3f ms \t event:%s\t op:%s\t " %( all_stat[i][0] / num, all_stat[i][1], all_stat[i][2])

    print "************ All stat info  end, skip:%d   ******************" %(skip)


logs = get_logs(sys.argv[1])
requests = get_request(logs)

get_osds_info(requests)

skip = get_stat(requests,12)

dump_stat(requests,skip)

