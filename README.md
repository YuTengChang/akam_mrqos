# akam_mrqos

# folder explanation

## configuration
contains the modules beeline and hdfsutil
as well as config.py (configuration file for the system)

## crondaemon
module developed by Scott. Used in managing the cronjobs

## local_other_script
script that run on the desktop or VM 
including SQLite3 DB initializations (under VM)

## local_python
python scripts that run on local or VM

## mrqos_daemon
adapted daemon classes for different usage

## mrqos_data
data that is on the cluster related to MRQOS

## mrqos_hive_init
hive table initialization

## mrqos_hive_query
hive query run on the cluster

## mrqos_python_script
python script run on the cluster

## mrqos_query
akamai query used on the cluster to obtain data from Akamai agg

## mrqos_shell_script
shell script used on the cluster

## project_mpd_cluster
data and spark script on the cluster related to 
- mpg clustering
- region criticality

## project_ra_summary
data and spark script on the cluster related to 
- ra summarization
