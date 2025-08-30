# README

This plugin Force Runs DropOldAssets.  It can be configured to run on a scheduled basis by editing the plugin.properties file.  It attempts to balance reliability, performance and not taking down your db server with huge queries. On large installations this can take awhile.  One installation had 90,000,000 old versions.  This plugin would delete about 50,000,000 a day.   

These are the configurable values
```
## When to run the plugin
CRON_EXPRESSION=0 15 10 13 * ?

## Query every 2 days - the smaller the window the more queries, but they are lighter
DROP_OLD_ASSET_ITERATE_BY_DAYS=2

## Only drop versions older than this
DROP_OLD_ASSET_OLDER_THAN_DAYS=60

## batch delete size.  Smaller equals lighter queries
DROP_OLD_ASSET_BATCH_SIZE-100

## dont delete, just show what would be deleted
DROP_OLD_ASSET_DRY_RUN=false

## rm -rf the old asset inode directory
CLEAN_DEAD_INODE_FROM_FS=true
```








## How to build this example

To build the JAR, run the following Maven command: 
```sh
mvn clean install
```
The plugin jar will be under the `./target/` directory

