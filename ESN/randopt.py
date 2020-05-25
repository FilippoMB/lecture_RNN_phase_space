import argparse
import json
import numpy as np
import os
import random
import sys
import esnet
from Scheduler import Scheduler
import multiprocessing

DEFAULT_N_ITERATIONS = 100
DEFAULT_N_EVAL = 3

############################################################################
# Parse input arguments
############################################################################
parser = argparse.ArgumentParser()
parser.add_argument("data", help="path to data file", type=str)
parser.add_argument("results_folder", help="folder path where to write results file", type=str)
parser.add_argument("--n_iter", help="Number of iterations", type=int, nargs='?', default=DEFAULT_N_ITERATIONS)
parser.add_argument("--n_eval", help="Number of ESN evaluations for each iteration", type=int, nargs='?', default=DEFAULT_N_EVAL)
parser.add_argument("--n_threads", help="Number of threads", type=int, nargs='?', default=multiprocessing.cpu_count())
parser.add_argument("--batch_size", help="Dimension of batch evaluation before saving checkpoint", type=int, nargs='?', default=None)
#parser.add_argument("optconfig", help="path to optimization config file", type=str)
#parser.add_argument("esnconfig", help="path to where the ESN config file should be saved", type=str)
args = parser.parse_args()

############################################################################
# Load data
############################################################################
print("Loading data (%s)"%args.data)
# If the data is stored in a directory, load the data from there. Otherwise,
# load from the single file and split it.
if os.path.isdir(args.data):
    Xtr, Ytr, Xval, Yval, _, _ = esnet.load_from_dir(args.data)

else:
    X, Y = esnet.load_from_text(args.data)

    # Construct training/test sets
    Xtr, Ytr, Xval, Yval, _, _ = esnet.generate_datasets(X, Y)

###############################################################################################

def run_esn_config(n_internal_units, spectral_radius, connectivity, input_scaling, input_shift, teacher_scaling, teacher_shift, feedback_scaling, noise_level,
        n_drop, regression_method, regression_parameters, embedding, n_dim, embedding_parameters):
    """
    Fitness function.
    Trains a randomly initiated ESN using the parameters in 'individual' and
    the config file.

    Returns touple with error metric (touple required by DEAP)
    """
    config = esnet.format_config(n_internal_units, spectral_radius, connectivity, input_scaling, input_shift, teacher_scaling, teacher_shift, feedback_scaling, noise_level,
        n_drop, regression_method, regression_parameters, embedding, n_dim, embedding_parameters)
    
    tr_errors = np.zeros(args.n_eval, dtype=float)
    val_errors = np.zeros(args.n_eval, dtype=float)
    for i in range(args.n_eval):
        tr_errors[i], val_errors[i] = esnet.run_from_config_return_errors(Xtr, Ytr, Xval, Yval, config)
    mean_tr_err = np.mean(tr_errors)
    mean_val_err = np.mean(val_errors)

    return mean_tr_err, mean_val_err

main_fold = args.results_folder

if main_fold[-1] != "/":
    main_fold += "/"

s = Scheduler(run_esn_config, savefile_path=main_fold+"rndsrch_savefile.json", resultfile_path=main_fold+"rndsrch_resultfile.json", verbose=True)


for i in range(args.n_iter):
    n_internal_units = np.random.choice([400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900])
    spectral_radius = np.random.uniform(0.5, 1.8)
    connectivity = np.random.uniform(0.15, 0.45)
    input_scaling = np.random.uniform(0.1, 1.0)
    input_shift = 0.0
    teacher_scaling = np.random.uniform(0.1, 1.0)
    teacher_shift = 0.0
    feedback_scaling = np.random.uniform(0.0, 0.5)
    noise_level = np.random.uniform(0.0, 0.1)
    n_drop = 100
    alpha = np.random.uniform(0.001, 0.4)
    C = np.random.uniform(0.001, 10.0)
    nu = np.random.uniform(0.001, 1.0)
    gamma = np.random.uniform(0.001, 1.0)
    embedding = 'identity'
    n_dim = None
    embedding_parameters = []
    

    #if i<int(N_ITERATIONS/2):
    if True:
        regression_method = 'ridge'
        regression_parameters = alpha
    else:
        regression_method = 'nusvr'
        regression_parameters = [C, nu, gamma]

    # insert a configuration in the list of the ones to be executed
    s.schedule( n_internal_units=n_internal_units, 
                spectral_radius=spectral_radius, 
                connectivity=connectivity, 
                input_scaling=input_scaling, 
                input_shift=input_shift, 
                teacher_scaling=teacher_scaling, 
                teacher_shift=teacher_shift, 
                feedback_scaling=feedback_scaling, 
                noise_level=noise_level,
                n_drop=n_drop, 
                regression_method=regression_method, 
                regression_parameters=regression_parameters, 
                embedding=embedding, 
                n_dim=n_dim, 
                embedding_parameters=embedding_parameters)

s.execute_all_parallel(n_threads=args.n_threads, checkpoint_size=args.batch_size)

