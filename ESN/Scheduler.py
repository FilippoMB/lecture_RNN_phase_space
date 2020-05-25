import os
import time
import itertools
import cPickle
import json
from datetime import datetime
from functools import partial
import multiprocessing
#from multiprocessing import Pool
from pathos.multiprocessing import ProcessingPool as Pool # pathos is necessary here to extend the picklability of pool map functions
#import filelock


class Scheduler:

    def __init__(self, fn, resultfile_path=None, savefile_path=None, verbose=True):
        if callable(fn):
            self.fn = [fn]
        else: # if fn is a list
            self.fn = fn
        self.fn_calls = []
        self.curr_call = 0
        self.savefile_path = savefile_path
        self.resultfile_path = resultfile_path
        
        if os.path.exists(resultfile_path):
            resp = raw_input("Scheduler: A previous result file with the name " + resultfile_path + " has been found. Do you want to overwrite (o) or append (a)?")
            if resp == "o":
                with open(resultfile_path,'w') as f:
                    json.dump([],f)
        else:
            with open(resultfile_path,'w') as f:
                json.dump([],f)

        if savefile_path is not None and os.path.exists(savefile_path):
            resp = raw_input("Scheduler: A previous schedule file with the name " + savefile_path + " has been found. Do you want to load it? (y/n)")
            if resp == "y":
                self.load_schedule(savefile_path)

        self.verbose = verbose

    def mf_schedule(self, fnid, *args, **kwargs):
        if fnid >= 0 and fnid < len(self.fn):
            self.fn_calls.append([fnid, args, kwargs])
        else:
            raise ValueError("Scheduler: Invalid fnid.")

    def schedule(self, *args, **kwargs):
        if len(self.fn) == 1:
            self.mf_schedule(0, *args, **kwargs)
        else:
            raise ValueError("Scheduler: schedule function cannot be called since multiple functions fn have been provided. Use mf_schedule() instead.")

    def mf_multi_schedule(self, fnid, variable_index, *args, **kwargs):
        ### Example usages:
        # [[0,"par_a"],[1,"par_b"]]  ---->  (#0, par_a) x (#1, par_b)
        # "par_a"  --->  par_a
        # [0, 1] --->  (#0, #1)
        if isinstance(variable_index, int):
            for i in range(len(args[variable_index])):
                newargs = list(args)
                newargs[variable_index] = args[variable_index][i]
                self.schedule(*newargs, **kwargs)
        elif isinstance(variable_index, basestring):
            for i in range(len(kwargs[variable_index])):
                newkwargs = kwargs.copy()
                newkwargs[variable_index] = kwargs[variable_index][i]
                self.schedule(*args, **newkwargs)
        elif isinstance(variable_index, list):
            vars = [var_ind for i in variable_index for var_ind in i]
            vars_dict = {}
            for v in vars:
                if isinstance(v, int):
                    vars_dict[v] = args[v]
                else:
                    vars_dict[v] = kwargs[v]
            arglists = itertools.product(*[zip(*[[[a,e] for e in vars_dict[a]] for a in arg]) for arg in variable_index])
            for arglist in arglists:
                f_arglist = [elem for temp in arglist for elem in temp] # flatten arglist array
                newargs = list(args)
                newkwargs = kwargs.copy()
                for arg in f_arglist:
                    if isinstance(arg[0], int):
                        newargs[arg[0]] = arg[1]
                    elif isinstance(arg[0], basestring):
                        newkwargs[arg[0]] = arg[1]
                self.mf_schedule(fnid, *newargs, **newkwargs)
        else:
            raise ValueError("Scheduler: Invalid variable_index provided.")

    def multi_schedule(self, variable_index, *args, **kwargs):
        if len(self.fn) == 1:
            self.mf_multi_schedule(0, variable_index, *args, **kwargs)
        else:
            raise ValueError("Scheduler: schedule function cannot be called since multiple functions fn have been provided. Use mf_multi_schedule() instead.")

    def execute_next(self):
        fn_call = self.fn_calls.pop(0)
        fnid = fn_call[0]
        args = fn_call[1]
        kwargs = fn_call[2]
        self.speak("Executing function call with arguments:\n", args, " ", kwargs)
        fn_output = self.fn[fnid](*args, **kwargs)

        self.save_result(fnid, fn_output, args, kwargs)
        if self.savefile_path is not None:
            self.save_schedule()
        self.curr_call += 1
        return fn_output

    def execute_all(self):
        tot_calls = len(self.fn_calls)
        fn_outputs = []
        t0 = time.time()
        while len(self.fn_calls) > 0:
            self.speak("Iteration ", self.curr_call+1, " of ", tot_calls)
            t1 = time.time()
            fn_outputs.append(self.execute_next())
            t2 = time.time()
            self.speak("Iteration complete. Time elapsed: ", str(t2-t1), ", function output: ", fn_outputs[-1])
            self.speak("------------------------------")
        t3 = time.time()
        self.speak("Multi-schedule complete. Total time elapsed: ", t3-t0)
        if self.savefile_path is not None:
            os.remove(self.savefile_path) # delete (now empty) snapshot file
        return fn_outputs

    def execute_all_parallel(self, n_threads = multiprocessing.cpu_count(), checkpoint_size=None, prepend_existing=True):
        pool = Pool(n_threads)
        
        # wrapper because of map function's lack of multi argument support

        def func_star(fnid_args_kwargs):
            print "Executing function..."
            t0 = time.time()
            fn_output = self.fn[0](*(fnid_args_kwargs[1]),**(fnid_args_kwargs[2]))
            t1 = time.time()
            print "...completed (Time elapsed: ", str(t1-t0), ")."
            return fn_output
        
        self.speak("Starting parallel scheduled execution (", str(n_threads), " threads)...")
        if checkpoint_size is not None:
            t0 = time.time()
            checkpoints = range(0,len(self.fn_calls),checkpoint_size)
            for chkid,chk in enumerate(checkpoints):
                fn_call_ids = range(min(checkpoint_size, len(self.fn_calls)))
                self.speak("Starting scheduled batch " + str(chkid+1) + " of " + str(len(checkpoints)) + "...")
                t2 = time.time()
                fn_outputs = pool.map(func_star, self.fn_calls[min(fn_call_ids):max(fn_call_ids)+1])
                t3 = time.time()
                self.speak("Scheduled batch complete. Time elapsed: ", str(t3-t2))
                self.speak("-----------------------")
                self.save_results(fn_call_ids, fn_outputs, prepend_existing)
                del self.fn_calls[min(fn_call_ids):max(fn_call_ids)+1]
                if self.savefile_path is not None:
                    self.save_schedule()
            if self.savefile_path is not None:
                os.remove(self.savefile_path) # delete (now empty) snapshot file
            t1 = time.time()
            
        else:
            t0 = time.time()
            fn_outputs = pool.map(func_star, self.fn_calls)
            t1 = time.time()
            self.save_all_results(fn_outputs,prepend_existing)
        self.speak("Multi-schedule complete. Total time elapsed: ", str(t1-t0))
        
        return fn_outputs

    def execute_all_parallel_experimental(self, n_threads = multiprocessing.cpu_count()):
        pool = Pool(n_threads)
        schedule_lock = filelock.FileLock(self.savefile_path)
        results_lock = filelock.FileLock(self.resultfile_path)

        callids = range(len(self.fn_calls))
        # wrapper because of map function's lack of multi argument support
        def func_star(callid_fnid_args_kwargs):
            callid = callid_fnid_args_kwargs[0]
            fnid, args, kwargs = callid_fnid_args_kwargs[1][0],callid_fnid_args_kwargs[1][1],callid_fnid_args_kwargs[1][2]
            fn_output = self.fn[0](*(args),**(kwargs))
            print fn_output
            with results_lock.acquire(timeout = 10):
                self.save_result(fnid, fn_output, args, kwargs)
            with schedule_lock.acquire(timeout = 10):
                for i in range(len(callids)):
                    if callids[i] == callid:
                        del self.fn_calls[i]
                        del callids[i]
                        break
                if self.savefile_path is not None:
                    self.save_schedule()
                print "Remaining iterations: ", str(len(self.fn_calls))

        self.speak("Starting parallel scheduled execution...")
        t0 = time.time()
        print zip(range(len(self.fn_calls)), self.fn_calls)
        fn_outputs = pool.map(func_star, zip(range(len(self.fn_calls)), self.fn_calls),parallel=False)
        #fn_outputs = []
        #for i,fn_output in enumerate(pool.imap(func_star, self.fn_calls, chunksize=n_threads)):
        #    print("{} (Time elapsed: {}s)".format(x, int(time.time() - t0)))
        #    fn_call = self.fn_calls.pop(0)
        #    fnid = fn_call[0]
        #    args = fn_call[1]
        #    kwargs = fn_call[2]
        #    fn_outputs.append(fn_output)
        #    self.save_result(fnid, fn_output, args, kwargs)
        #    if self.savefile_path is not None:
        #        self.save_schedule()
        #    self.curr_call += 1
        t1 = time.time()
        self.speak("Multi-schedule complete. Total time elapsed: ", str(t1-t0))
        if self.savefile_path is not None:
            os.remove(self.savefile_path) # delete (now empty) snapshot file
        return fn_outputs

    def save_schedule(self):
        with open(self.savefile_path, 'w') as file_w:
            #cPickle.dump(self.fn_calls, file_w, protocol=cPickle.HIGHEST_PROTOCOL)
            json.dump(self.fn_calls, file_w)

    def load_schedule(self, filename):
        with open(filename, 'r') as file_r:
            #self.fn_calls = cPickle.load(file_r)
            self.fn_calls = json.load(file_r)

    def gen_result_dict(self, fnid, fn_output, args, kwargs):
        # this check is to include the class name if the funtion is a class method
        fname = ""
        try: 
            fname = self.fn[fnid].__self__.__class__.__name__ + "."
        except AttributeError:
            pass
        fname += self.fn[fnid].__name__

        resultdict = {"fn_name":fname, "fn_output":fn_output, "args": args, "kwargs": kwargs, "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        return resultdict

    def save_result(self, fnid, fn_output, args, kwargs):
        with open(self.resultfile_path,"r") as f:
            resultlist = json.load(f)

        resultdict = self.gen_result_dict(fnid, fn_output, args, kwargs)
        
        resultlist.append(resultdict)
        
        with open(self.resultfile_path,"w") as f:
            json.dump(resultlist, f)

    def save_results(self, fn_call_ids, fn_outputs, prepend_existing=True):
        assert len(fn_call_ids) == len(fn_outputs) 
        if prepend_existing:
            with open(self.resultfile_path,"r") as f:
                resultlist = json.load(f)
                print len(resultlist)
        else:
            resultlist = []
        self.speak("Saving results...")
        
        for i, callid in enumerate(fn_call_ids):
            resultdict = self.gen_result_dict(self.fn_calls[callid][0], fn_outputs[i], self.fn_calls[callid][1], self.fn_calls[callid][2])
            resultlist.append(resultdict)

        print "Before saving"
        print len(resultlist)

        with open(self.resultfile_path,"w") as f:
            json.dump(resultlist, f)

        self.speak("Results saved.")

    def save_all_results(self, fn_outputs, prepend_existing=True):
        self.save_results(range(len(self.fn_calls)), fn_outputs, prepend_existing)      

    def speak(self, *args):
        if self.verbose:
            string = "".join([str(arg) for arg in args])
            print "Scheduler: ", string

if __name__ == "__main__":
    class Testone:
        def test(self, arg1, arg2, kwarg1=True):
            time.sleep(0.5)
            return 10
    t = Testone()
    s = Scheduler(t.test, "./test/resultfile_test.json", "./test/savefile_test.json")
    s.multi_schedule([[0,1],["kwarg1"]], range(15), range(15), kwarg1=[True,False])
    print "Total calls: ", str(len(s.fn_calls))
    s.execute_all_parallel(checkpoint_size=9)
    with open("./test/resultfile_test.json",'r') as f:
        resultslist = json.load(f)
        print "Total results: ", len(resultslist)
