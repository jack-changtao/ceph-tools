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
all_avg_stat={}

long_event_stat = {} #long event stat
lat_time = [1,2,5,10,20,40,50,80,100,200,400,500,1000]
long_lat = 20 #long event latency ms
lat_stat = {}   

skip_events = set(['send apply ack','op_applied','done','sub_op_applied'])
#skip_events = set([])

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
        if (self.last_event == None  or self.first_event == None):
	  return 0.0
        return (self.last_event - self.first_event).total_seconds() * 1000

    def __repr__(self):
        return str(self.events) + " " + \
               str(self.duration()) + " " + self.parsed[0]['reqid']

    def pretty_print(self):
        outstr = "reqid: %s, duration: %0.3fms, events:%d "%(
            self.parsed[0]['reqid'], self.duration() ,len(self.events))
        outstr += "\n=====================\n"
        count = 0
        event_regex = re.compile('waiting for subops from (\d),(\d)')
        slave1 = ""
        slave2 = "" 
        for (time, event, osd, op) in self.events:
            if(count==0):
                last_time = time
            duration = (time - last_time).total_seconds() * 1000
            match = event_regex.match(event)
            if match:
               slave1 = match.group(1)
               slave2 = match.group(2)
            if str(osd) == slave1:
               outstr += "duration(%.3fms)\t%s\t\t(osd.%s)\t%s\t%s\n"%(duration,str(time), str(osd), event, op)
            elif str(osd) == slave2:
               outstr += "duration(%.3fms)\t%s\t\t\t(osd.%s)\t%s\t%s\n"%(duration,str(time), str(osd), event, op)
            else:
               outstr += "duration(%.3fms)\t%s\t(osd.%s)\t%s\t%s\n"%(duration,str(time), str(osd), event, op)
            last_time = time
            count= count + 1
        outstr += "=====================\n"
        return outstr
    def add_avg_stat(self):
        count = 0
        for (time, event, osd, op) in self.events:
            if(count==0):
                last_time = time
            duration = (time - last_time).total_seconds() * 1000
            #print event
            if all_avg_stat.has_key(count) == False :
                all_avg_stat[count] = [0,'event','op']
            d = all_avg_stat[count][0]
            d += duration
            all_avg_stat[count] = [d,event,op]
            last_time = time
            count = count + 1
    def get_long_events(self):
        import pdb
        #pdb.set_trace()
        count = 0
        max_duration = 0
        item=()
	for (time, event, osd, op) in self.events:
           if(count==0):
                last_time = time
           duration = (time - last_time).total_seconds() * 1000
           if duration > max_duration:
              max_duration = duration
              item=(max_duration,event,osd,op)
           last_time = time
           count = count + 1
        print "long event: reqid:%s, duration:%.3f, event:%s, osd.%d, %s" %(self.parsed[0]['reqid'], item[0], item[1], item[2], item[3])
   
        long_event=item[1]
        if long_event_stat.get(long_event,0) == 0:
           long_event_stat[long_event] = 1
        else:
           long_event_stat[long_event] += 1

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



def dump_lat_stat(num):
    for time in lat_time:
       if lat_stat.get(time,0)==0: 
          lat_stat[time] = 0
       print "[<%d ms] count:%d ratio:%.2f" % (time, lat_stat[time], (lat_stat[time]*1.00*100)/num) 
    if lat_stat.get('other',0) == 0:
       lat_stat['other']=0
    print "[>%d ms] count:%d ratio:%.2f" % (time, lat_stat['other'], (lat_stat['other']*1.00*100)/num)

def get_lat_stat(request):
    d = request.duration()
    length = len(lat_time)
    for i in range(0, length):
       if d < lat_time[i]:
          key = lat_time[i]
          if lat_stat.get(key,0) == 0:
             lat_stat[key] = 1
          else:
             lat_stat[key] += 1
          break
    if d > lat_time[length-1]:
       if lat_stat.get('other',0) == 0:
          lat_stat['other'] =1
       else:
          lat_stat['other'] += 1
    if d > long_lat:
       request.get_long_events()  
       print request.pretty_print()
#fileter events num
# if num_events ==0 , stat all events
def get_stat(requests,num_events):
    skip=0
    for i in requests.itervalues():
       num = i.get_events_num()
       if( num_events != 0 and  num != num_events):
          skip += 1
          continue
       get_lat_stat(i)
       i.add_avg_stat()
    return skip
 
def dump_avg_stat(num,skip):
    num -= skip
    length=len(all_avg_stat)
    print "************ All avg stat info  count:%d, stat events:%d  ************************" %(num,length)
    for i in range(0,length):
       print "avg duraion:%.3f ms \t event:%s\t op:%s\t " %( all_avg_stat[i][0] / num, all_avg_stat[i][1], all_avg_stat[i][2])

    print "************ All avg stat info  end, skip:%d   ******************" %(skip)


logs = get_logs(sys.argv[1])
requests = get_request(logs)

#get_osds_info(requests)

num = len(requests)
#1 replication 12 events 
#2 replication 
#3 replication 51 events

num_events = 0

skip = get_stat(requests, num_events)

dump_lat_stat(num-skip)

print "long event stat:"
print long_event_stat



dump_avg_stat(num,skip)



