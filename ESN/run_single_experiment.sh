#!/bin/bash

# NOTE: This file needs to be executable (i.e. chmod +x run_experiment.sh before attempting to run)

DATAFILE=$1
OPTCONFIG=$2
ESNCONFIG=$3
RUNS=$4

#DATAFILE=./data/NARMA
#OPTCONFIG=ridge_identity
#ESNCONFIG=esnconfig
#RUNS=30

# Tune parameters. Note: the config file for the best parameters are saved at the location in $ESNCONFIG
#python -m scoop -n 4 ./genoptesn.py $DATAFILE $OPTCONFIG $ESNCONFIG

# Run experiments with these parameters
python -m scoop -n 4 ./esn_experiment.py $DATAFILE $ESNCONFIG $RUNS
