#!/bin/bash
#    If you want to use /bin/csh, you have to change things below!
#
#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#  April 22th, 2013
#
#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#   ____________________________________________________________
#  |                                                            | 
#  | Set initial information for the Queuing system             | 
#  | ==============================================             | 
#  |                                                            | 
#  | All PBS directives (the lines starting with #PBS) below    |
#  | are optional and can be omitted, resulting in the use of   | 
#  | the system defaults.                                       | 
#  |                                                            | 
#  | Please send comments and questions to support-uit@notur.no | 
#  |                                                            | 
#  |____________________________________________________________|
#
#  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#PBS -lnodes=1:ppn=16
#
#    Number of nodes and CPU's.
#    Number of nodes and CPU's per node (ppn) requested, here we ask for a
#    total of 32 CPU's, only necessary for parallel jobs.  This creates a
#    file, accessible through the environment variable $PBS_NODEFILE in
#    this script, that can be used by mpirun etc., see below.
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#PBS -lwalltime=24:00:00
#
#    Expecting to run for up to 24 hours.
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
#PBS -m abe
#
#    The queuing system will send an email on job Abort, Begin, End
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#

# no bash command may be placed before the last PBS directive

# create temporary scratch area for this job on the global file system
SCRATCH_DIRECTORY=/global/work/$USER/$PBS_JOBID
mkdir -p $SCRATCH_DIRECTORY

# we will run the calculation in the scratch directory and not
# in the home directory
cd $SCRATCH_DIRECTORY

# Copy config files to the scratch directory
echo "Copying files to scratch directory"
cp -r $PBS_O_WORKDIR/* $SCRATCH_DIRECTORY

# Load modules
module load python

# Find config/data files and names
shopt -s nullglob
BASEDIR=$SCRATCH_DIRECTORY
CONFIGDIR=configs/user
CONFIGPREFIX=${BASEDIR}/${CONFIGDIR}/
CONFIGFILES=(${CONFIGPREFIX}*.json)

DATADIR=data
DATAPREFIX=$BASEDIR/$DATADIR/
DATAFILES=(${DATAPREFIX}*)

RESULTDIR=$BASEDIR/results
shopt -u nullglob

# Remove json file extension (and path for confignames/datanames)
SUFFIX=".json"

idx=0
for i in ${CONFIGFILES[@]}; do
  i=${i%$SUFFIX}
  CONFIGFILES[idx]=${i}
  CONFIGNAMES[idx]=${i#$CONFIGPREFIX}

  idx=${idx}+1
done

idx=0
for i in ${DATAFILES[@]}; do
  i=${i#$DATAPREFIX}
  DATANAMES[idx]=${i}

  idx=${idx}+1
done

# Initialize the experiment
idx_i=0
idx_j=0
for i in ${DATANAMES[@]}; do
  for j in ${CONFIGNAMES[@]}; do
    DATAFILE=$DATAFILES[$idx_i]
    CONFIGFILE=$CONFIGFILES[$idx_j]
    ESNCONFIG=$PBS_O_WORKDIR/esn/${i}_${j}
    RUNS=32

    # Spawn process
    pbsdsh -c 1 bash ./run_experiment.sh $DATAFILE $CONFIGFILE $ESNCONFIG $RUNS > $PBS_O_WORKDIR/results/${i}_${j}

    idx_j=${idx_j}+1
  done

  idx_i=${idx_i}+1
done

# before cleaning up remember to copy back important files

# clean up the scratchdir (commented out)
cd /tmp
rm -rf $SCRATCH_DIRECTORY

