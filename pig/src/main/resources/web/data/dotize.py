#!/usr/bin/env python

import sys, re, json
# JSON file to convert
if (len(sys.argv) > 1):
  filename = sys.argv[1]
else:
  print "Usage: ./dotize.py myfile.json"
  exit(0)

def convert_id(orig):
  return re.sub('-', '_', orig)

f = open(filename, 'r')
json_string = f.read()
data = json.loads(json_string)


# We print DOT format to STDOUT.
# See http://www.graphviz.org/doc/info/lang.html and http://www.graphviz.org/doc/info/shapes.html#record
print "digraph pig {"
print "  node [shape=record];"

for datum in data:
  node_id = convert_id(datum['name'])
  out_links = datum['successorNames']
  job_id = datum['jobId']
  aliases = datum['aliases']
  features = datum['features']
  
  label = "  {node_id} [label=\"{{<f0>{aliases}|<f1>{features}|<f2>{job_id}}}\"];".format(
    node_id=node_id,
    aliases=' '.join(aliases),
    features=' '.join(features),
    job_id=job_id
  )
  print label
  
  for link in out_links:
    print "  {node_id} -> {to};".format(node_id=node_id, to=convert_id(link))
print "}"
