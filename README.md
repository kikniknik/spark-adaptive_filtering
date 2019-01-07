# Adaptive filtering in Spark SQL

## Overview

Normally, Spark applies filter predicates in the order they were given by the user. This extension adds adaptive selection ordering techniques in filtering in Spark SQL in order to constantly re-order filter predicates in the best order during query execution.


For example, suppose we collect data from a server and we want to test its heavy load times during office hours, when we mainly care for its performance. Let's say we have the following query:

```scala
df.filter(hour('timestamp) > 7 && hour('timestamp) < 16 && 'memory_usage > 60 && 'cpu_usage > 60 && 'network_usage > 30)
```
With adaptive filtering, this could be executed with an average of 1-2 cheap checks per row, depending on real data, while in default implementation execution time depends only on user specified predicates' order.

## Implementation details

Once at every `k` rows all predicates are evaluated and some metrics are collected. Once at every `n` rows (`n` >> `k`), predicate ranks are calculated from the metrics that were collected in this round and they are sorted based on their ranks. Metrics are zeroed for the next round. Also, in each round it's possible to preserve a percentage `m` of previous rank for stability.

Executors act separately, so that each one keeps its own statistics and predicate order. This way no network traffic is added but statistics collected from one task, remains for others in same executor.

## Configuration options

`k`, `n` and `m` are configurable via configuration options `spark.sql.adaptiveFilter.collectRate`, `spark.sql.adaptiveFilter.calculateRate` and `spark.sql.adaptiveFilter.momentum` respectively. Generally the following configuration options are available:

| Name 											| Default value | Description 
|-----------------------------------------------|---------------|-------------
| `spark.sql.adaptiveFilter.enabled`   			| `true`        | Enables adaptive extension.
| `spark.sql.adaptiveFilter.verbose`   			| `false`       | Enables verbose mode. Information about predicates statistics and ranks will be produced in executors' stdouts.
| `spark.sql.adaptiveFilter.collectRate`     	| 1000          | Statistics collection rate (in rows)
| `spark.sql.adaptiveFilter.calculateRate`     	| 1000000       | Ranks calculation rate (in rows)
| `spark.sql.adaptiveFilter.momentum` 			| 0.3           | Previous rank preservation factor (range [0,1])

## Usage

You can use this extension with a Spark 2.x installation. Use version 0.2 (branch-2.2) for Spark versions prior to 2.3 and version 0.4 (branch-2.4) for Spark versions 2.3, 2.4.

Download and build:
```
$ git clone https://github.com/kikniknik/spark-adaptive_filtering
$ cd spark-adaptive_filtering
$ sbt clean package
```

Start Spark shell/submit adding the generated package and the following extension injector:
```
$ $SPARK_HOME/bin/spark-shell --jars target/scala-2.11/spark-adaptive_filtering_2.11-0.4-SNAPSHOT.jar \
--conf "spark.sql.extensions=gr.auth.csd.datalab.spark.sql.AdaptiveFilterExtensionInjector" \
# set master and other configurations...
```

Adding a configuration option is available via `--conf` option in `spark-shell`/`spark-submit`, `config` method of `Builder` for `SparkSession` and via `SparkEnv.get.conf.set()` at runtime.

