import numpy as np
import json

results_path = './results/GEFCom/rndsrch_resultfile.json'


with open(results_path,'r') as f:
	results=json.load(f)

fn_outputs = np.zeros(len(results))
for i,r in enumerate(results):
	fn_outputs[i] = r['fn_output'][1]

best_result_i = np.argmin(fn_outputs)

print "Function output: " + str(fn_outputs[best_result_i])

print "Number of instances in results file: " + str(len(fn_outputs))

for i,argval in enumerate(results[best_result_i]["args"]):
	print "Arg. #", str(i), " = ", argval

for key,val in results[best_result_i]["kwargs"].iteritems():
	print "KeyArg. ", key, " = ", val
