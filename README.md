# EUM_Datastore

A set of scripts and utilities to assist with using the [EUMETSAT Data Store](https://data.eumetsat.int).

To use these utilities, you must be registered with EUMETSAT and have a valid API key. This can be passed to the scripts via an environment variable (`EUM_ACCESS_KEY`) or:
 - In the `Datastore_Search_Download` script via command line argument `--eum_access_key`
 - In the `EUM_Datastore` notebook via `eum_access_key` defined in the third cell.
 
 Substantial parts of these scripts are adapted from the [EUMETSAT example code](https://eumetsatspace.atlassian.net/wiki/spaces/DSDS/overview), which is released under an MIT license.
