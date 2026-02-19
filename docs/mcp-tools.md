MCP Tools Reference
===================

This document lists all MCP tools exposed via `@mcp.tool`, with response shape
summaries and full sample outputs. All tools return JSON-like dictionaries.
On failure, most tools include an `error` field.

Samples
-------
Captured from Spark History Server reading `examples/basic/events` on 2026-02-19T06:56:07Z.
Samples use full outputs with `SHS_COMPACT_TOOL_OUTPUT=false` for this capture.
Application IDs used: `app_small=spark-bcec39f6201b42b9925124595baad260` and `app_other=spark-110be3a8424d4a2789cb88134418217b`.

Conventions
-----------
- `app_id`, `stage_id`, `job_id`, `executor_id` are Spark identifiers.
- Timestamps are ISO strings unless noted.
- Numeric units are noted per field (ms, bytes, GB, etc).

Applications
------------

### `list_applications`
Response format:
- list of applications (id, name, attempts, resource hints)
Sample (full):
```json
[
  {
    "id": "spark-cc4d115f011443d787f03a71a476a745",
    "name": "NewYorkTaxiData_2025_06_27_03_56_52",
    "cores_granted": null,
    "max_cores": null,
    "cores_per_executor": null,
    "memory_per_executor_mb": null,
    "attempts": [
      {
        "attempt_id": null,
        "start_time": "2025-06-27T03:56:52.238000Z",
        "end_time": "2025-06-27T04:05:20.852000Z",
        "last_updated": "2025-08-17T22:03:48.105000Z",
        "duration": 508614,
        "spark_user": "spark",
        "app_spark_version": "3.5.3",
        "completed": true
      }
    ]
  },
  {
    "id": "spark-110be3a8424d4a2789cb88134418217b",
    "name": "NewYorkTaxiData_2025_06_27_00_24_44",
    "cores_granted": null,
    "max_cores": null,
    "cores_per_executor": null,
    "memory_per_executor_mb": null,
    "attempts": [
      {
        "attempt_id": null,
        "start_time": "2025-06-27T00:24:44.577000Z",
        "end_time": "2025-06-27T00:29:48.493000Z",
        "last_updated": "2025-08-17T22:03:48.104000Z",
        "duration": 303916,
        "spark_user": "spark",
        "app_spark_version": "3.5.3",
        "completed": true
      }
    ]
  },
  {
    "id": "spark-bcec39f6201b42b9925124595baad260",
    "name": "PythonPi",
    "cores_granted": null,
    "max_cores": null,
    "cores_per_executor": null,
    "memory_per_executor_mb": null,
    "attempts": [
      {
        "attempt_id": null,
        "start_time": "2025-06-26T00:06:51.168000Z",
        "end_time": "2025-06-26T00:06:58.590000Z",
        "last_updated": "2025-08-17T22:03:48.104000Z",
        "duration": 7422,
        "spark_user": "spark",
        "app_spark_version": "3.5.3",
        "completed": true
      }
    ]
  }
]
```

### `get_application`
Response format:
- application details (id, name, attempts, cores, memory, etc)
Sample (full):
```json
{
  "id": "spark-bcec39f6201b42b9925124595baad260",
  "name": "PythonPi",
  "cores_granted": null,
  "max_cores": null,
  "cores_per_executor": null,
  "memory_per_executor_mb": null,
  "attempts": [
    {
      "attempt_id": null,
      "start_time": "2025-06-26T00:06:51.168000Z",
      "end_time": "2025-06-26T00:06:58.590000Z",
      "last_updated": "2025-08-17T22:03:48.104000Z",
      "duration": 7422,
      "spark_user": "spark",
      "app_spark_version": "3.5.3",
      "completed": true
    }
  ]
}
```

### `get_environment`
Response format:
- spark_properties, system_properties, classpath_entries, runtime info
Sample (full):
```json
{
  "runtime": {
    "java_version": "17.0.12 (Eclipse Adoptium)",
    "java_home": "/opt/java/openjdk",
    "scala_version": "version 2.12.18"
  },
  "spark_properties": [
    [
      "spark.app.id",
      "spark-bcec39f6201b42b9925124595baad260"
    ],
    [
      "spark.app.name",
      "PythonPi"
    ],
    [
      "spark.app.startTime",
      "1750896411168"
    ],
    [
      "spark.app.submitTime",
      "1750896410589"
    ],
    [
      "spark.driver.bindAddress",
      "100.64.10.2"
    ],
    [
      "spark.driver.blockManager.port",
      "7079"
    ],
    [
      "spark.driver.cores",
      "1"
    ],
    [
      "spark.driver.extraJavaOptions",
      "-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false"
    ],
    [
      "spark.driver.host",
      "spark-pi-python-ade66897a98eed73-driver-svc.spark-team-a.svc"
    ],
    [
      "spark.driver.memory",
      "512m"
    ],
    [
      "spark.driver.port",
      "7078"
    ],
    [
      "spark.eventLog.dir",
      "s3a://spark-operator-doeks-spark-logs-20250224211454008900000007/spark-event-logs"
    ],
    [
      "spark.eventLog.enabled",
      "true"
    ],
    [
      "spark.eventLog.rolling.enabled",
      "true"
    ],
    [
      "spark.eventLog.rolling.maxFileSize",
      "64m"
    ],
    [
      "spark.executor.cores",
      "1"
    ],
    [
      "spark.executor.extraJavaOptions",
      "-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false"
    ],
    [
      "spark.executor.id",
      "driver"
    ],
    [
      "spark.executor.instances",
      "1"
    ],
    [
      "spark.executor.memory",
      "512m"
    ],
    [
      "spark.executorEnv.SPARK_DRIVER_POD_IP",
      "100.64.10.2"
    ],
    [
      "spark.hadoop.fs.s3.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ],
    [
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "*********(redacted)"
    ],
    [
      "spark.hadoop.fs.s3a.aws.credentials.provider.mapping",
      "*********(redacted)"
    ],
    [
      "spark.hadoop.fs.s3a.connection.maximum",
      "200"
    ],
    [
      "spark.hadoop.fs.s3a.connection.timeout",
      "1200000"
    ],
    [
      "spark.hadoop.fs.s3a.fast.upload",
      "true"
    ],
    [
      "spark.hadoop.fs.s3a.input.fadvise",
      "random"
    ],
    [
      "spark.hadoop.fs.s3a.path.style.access",
      "true"
    ],
    [
      "spark.hadoop.fs.s3a.readahead.range",
      "256K"
    ],
    [
      "spark.kubernetes.authenticate.driver.serviceAccountName",
      "spark-team-a"
    ],
    [
      "spark.kubernetes.authenticate.executor.serviceAccountName",
      "spark-team-a"
    ],
    [
      "spark.kubernetes.container.image",
      "public.ecr.aws/data-on-eks/spark:3.5.3-scala2.12-java17-python3-ubuntu"
    ],
    [
      "spark.kubernetes.container.image.pullPolicy",
      "IfNotPresent"
    ],
    [
      "spark.kubernetes.driver.container.image",
      "public.ecr.aws/data-on-eks/spark:3.5.3-scala2.12-java17-python3-ubuntu"
    ],
    [
      "spark.kubernetes.driver.label.sparkoperator.k8s.io/app-name",
      "spark-pi-python"
    ],
    [
      "spark.kubernetes.driver.label.sparkoperator.k8s.io/launched-by-spark-operator",
      "true"
    ],
    [
      "spark.kubernetes.driver.label.sparkoperator.k8s.io/submission-id",
      "37383231-e716-404b-96dd-74b58edcd84f"
    ],
    [
      "spark.kubernetes.driver.pod.name",
      "spark-pi-python-driver"
    ],
    [
      "spark.kubernetes.executor.container.image",
      "public.ecr.aws/data-on-eks/spark:3.5.3-scala2.12-java17-python3-ubuntu"
    ],
    [
      "spark.kubernetes.executor.label.sparkoperator.k8s.io/app-name",
      "spark-pi-python"
    ],
    [
      "spark.kubernetes.executor.label.sparkoperator.k8s.io/launched-by-spark-operator",
      "true"
    ],
    [
      "spark.kubernetes.executor.label.sparkoperator.k8s.io/submission-id",
      "37383231-e716-404b-96dd-74b58edcd84f"
    ],
    [
      "spark.kubernetes.executor.podNamePrefix",
      "pythonpi-009fa697a98f0548"
    ],
    [
      "spark.kubernetes.memoryOverheadFactor",
      "0.4"
    ],
    [
      "spark.kubernetes.namespace",
      "spark-team-a"
    ],
    [
      "spark.kubernetes.pyspark.pythonVersion",
      "3"
    ],
    [
      "spark.kubernetes.resource.type",
      "python"
    ],
    [
      "spark.kubernetes.submission.waitAppCompletion",
      "false"
    ],
    [
      "spark.kubernetes.submitInDriver",
      "true"
    ],
    [
      "spark.master",
      "k8s://https://172.20.0.1:443"
    ],
    [
      "spark.rdd.compress",
      "True"
    ],
    [
      "spark.scheduler.mode",
      "FIFO"
    ],
    [
      "spark.serializer.objectStreamReset",
      "100"
    ],
    [
      "spark.submit.deployMode",
      "client"
    ],
    [
      "spark.submit.pyFiles",
      ""
    ]
  ],
  "hadoop_properties": [
    [
      "adl.feature.ownerandgroup.enableupn",
      "false"
    ],
    [
      "adl.http.timeout",
      "-1"
    ],
    [
      "dfs.client.ignore.namenode.default.kms.uri",
      "false"
    ],
    [
      "dfs.ha.fencing.ssh.connect-timeout",
      "30000"
    ],
    [
      "file.blocksize",
      "67108864"
    ],
    [
      "file.bytes-per-checksum",
      "512"
    ],
    [
      "file.client-write-packet-size",
      "65536"
    ],
    [
      "file.replication",
      "1"
    ],
    [
      "file.stream-buffer-size",
      "4096"
    ],
    [
      "fs.AbstractFileSystem.abfs.impl",
      "org.apache.hadoop.fs.azurebfs.Abfs"
    ],
    [
      "fs.AbstractFileSystem.abfss.impl",
      "org.apache.hadoop.fs.azurebfs.Abfss"
    ],
    [
      "fs.AbstractFileSystem.adl.impl",
      "org.apache.hadoop.fs.adl.Adl"
    ],
    [
      "fs.AbstractFileSystem.file.impl",
      "org.apache.hadoop.fs.local.LocalFs"
    ],
    [
      "fs.AbstractFileSystem.ftp.impl",
      "org.apache.hadoop.fs.ftp.FtpFs"
    ],
    [
      "fs.AbstractFileSystem.gs.impl",
      "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    ],
    [
      "fs.AbstractFileSystem.har.impl",
      "org.apache.hadoop.fs.HarFs"
    ],
    [
      "fs.AbstractFileSystem.hdfs.impl",
      "org.apache.hadoop.fs.Hdfs"
    ],
    [
      "fs.AbstractFileSystem.o3fs.impl",
      "org.apache.hadoop.fs.ozone.OzFs"
    ],
    [
      "fs.AbstractFileSystem.ofs.impl",
      "org.apache.hadoop.fs.ozone.RootedOzFs"
    ],
    [
      "fs.AbstractFileSystem.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3A"
    ],
    [
      "fs.AbstractFileSystem.swebhdfs.impl",
      "org.apache.hadoop.fs.SWebHdfs"
    ],
    [
      "fs.AbstractFileSystem.viewfs.impl",
      "org.apache.hadoop.fs.viewfs.ViewFs"
    ],
    [
      "fs.AbstractFileSystem.wasb.impl",
      "org.apache.hadoop.fs.azure.Wasb"
    ],
    [
      "fs.AbstractFileSystem.wasbs.impl",
      "org.apache.hadoop.fs.azure.Wasbs"
    ],
    [
      "fs.AbstractFileSystem.webhdfs.impl",
      "org.apache.hadoop.fs.WebHdfs"
    ],
    [
      "fs.abfs.impl",
      "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"
    ],
    [
      "fs.abfss.impl",
      "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
    ],
    [
      "fs.adl.impl",
      "org.apache.hadoop.fs.adl.AdlFileSystem"
    ],
    [
      "fs.adl.oauth2.access.token.provider.type",
      "*********(redacted)"
    ],
    [
      "fs.automatic.close",
      "true"
    ],
    [
      "fs.azure.authorization",
      "false"
    ],
    [
      "fs.azure.authorization.caching.enable",
      "true"
    ],
    [
      "fs.azure.buffer.dir",
      "${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/abfs"
    ],
    [
      "fs.azure.enable.readahead",
      "true"
    ],
    [
      "fs.azure.local.sas.key.mode",
      "false"
    ],
    [
      "fs.azure.sas.expiry.period",
      "90d"
    ],
    [
      "fs.azure.saskey.usecontainersaskeyforallaccess",
      "true"
    ],
    [
      "fs.azure.secure.mode",
      "false"
    ],
    [
      "fs.azure.user.agent.prefix",
      "unknown"
    ],
    [
      "fs.client.resolve.remote.symlinks",
      "true"
    ],
    [
      "fs.client.resolve.topology.enabled",
      "false"
    ],
    [
      "fs.creation.parallel.count",
      "64"
    ],
    [
      "fs.defaultFS",
      "file:///"
    ],
    [
      "fs.df.interval",
      "60000"
    ],
    [
      "fs.du.interval",
      "600000"
    ],
    [
      "fs.ftp.data.connection.mode",
      "ACTIVE_LOCAL_DATA_CONNECTION_MODE"
    ],
    [
      "fs.ftp.host",
      "0.0.0.0"
    ],
    [
      "fs.ftp.host.port",
      "21"
    ],
    [
      "fs.ftp.impl",
      "org.apache.hadoop.fs.ftp.FTPFileSystem"
    ],
    [
      "fs.ftp.timeout",
      "0"
    ],
    [
      "fs.ftp.transfer.mode",
      "BLOCK_TRANSFER_MODE"
    ],
    [
      "fs.getspaceused.jitterMillis",
      "60000"
    ],
    [
      "fs.har.impl.disable.cache",
      "true"
    ],
    [
      "fs.iostatistics.logging.level",
      "debug"
    ],
    [
      "fs.iostatistics.thread.level.enabled",
      "true"
    ],
    [
      "fs.permissions.umask-mode",
      "022"
    ],
    [
      "fs.s3.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ],
    [
      "fs.s3a.accesspoint.required",
      "false"
    ],
    [
      "fs.s3a.assumed.role.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ],
    [
      "fs.s3a.assumed.role.session.duration",
      "30m"
    ],
    [
      "fs.s3a.attempts.maximum",
      "5"
    ],
    [
      "fs.s3a.audit.enabled",
      "true"
    ],
    [
      "fs.s3a.aws.credentials.provider",
      "*********(redacted)"
    ],
    [
      "fs.s3a.aws.credentials.provider.mapping",
      "*********(redacted)"
    ],
    [
      "fs.s3a.block.size",
      "32M"
    ],
    [
      "fs.s3a.buffer.dir",
      "${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a"
    ],
    [
      "fs.s3a.change.detection.mode",
      "server"
    ],
    [
      "fs.s3a.change.detection.source",
      "etag"
    ],
    [
      "fs.s3a.change.detection.version.required",
      "true"
    ],
    [
      "fs.s3a.committer.abort.pending.uploads",
      "true"
    ],
    [
      "fs.s3a.committer.magic.enabled",
      "true"
    ],
    [
      "fs.s3a.committer.name",
      "file"
    ],
    [
      "fs.s3a.committer.staging.conflict-mode",
      "append"
    ],
    [
      "fs.s3a.committer.staging.tmp.path",
      "tmp/staging"
    ],
    [
      "fs.s3a.committer.staging.unique-filenames",
      "true"
    ],
    [
      "fs.s3a.committer.threads",
      "8"
    ],
    [
      "fs.s3a.connection.establish.timeout",
      "30s"
    ],
    [
      "fs.s3a.connection.maximum",
      "200"
    ],
    [
      "fs.s3a.connection.ssl.enabled",
      "true"
    ],
    [
      "fs.s3a.connection.timeout",
      "1200000"
    ],
    [
      "fs.s3a.connection.ttl",
      "5m"
    ],
    [
      "fs.s3a.downgrade.syncable.exceptions",
      "true"
    ],
    [
      "fs.s3a.endpoint",
      "s3.amazonaws.com"
    ],
    [
      "fs.s3a.etag.checksum.enabled",
      "false"
    ],
    [
      "fs.s3a.executor.capacity",
      "16"
    ],
    [
      "fs.s3a.fast.upload",
      "true"
    ],
    [
      "fs.s3a.fast.upload.active.blocks",
      "4"
    ],
    [
      "fs.s3a.fast.upload.buffer",
      "disk"
    ],
    [
      "fs.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ],
    [
      "fs.s3a.input.fadvise",
      "random"
    ],
    [
      "fs.s3a.list.version",
      "2"
    ],
    [
      "fs.s3a.max.total.tasks",
      "32"
    ],
    [
      "fs.s3a.multiobjectdelete.enable",
      "true"
    ],
    [
      "fs.s3a.multipart.purge",
      "false"
    ],
    [
      "fs.s3a.multipart.purge.age",
      "24h"
    ],
    [
      "fs.s3a.multipart.size",
      "64M"
    ],
    [
      "fs.s3a.multipart.threshold",
      "128M"
    ],
    [
      "fs.s3a.paging.maximum",
      "5000"
    ],
    [
      "fs.s3a.path.style.access",
      "true"
    ],
    [
      "fs.s3a.readahead.range",
      "256K"
    ],
    [
      "fs.s3a.retry.interval",
      "500ms"
    ],
    [
      "fs.s3a.retry.limit",
      "7"
    ],
    [
      "fs.s3a.retry.throttle.interval",
      "100ms"
    ],
    [
      "fs.s3a.retry.throttle.limit",
      "20"
    ],
    [
      "fs.s3a.select.enabled",
      "true"
    ],
    [
      "fs.s3a.select.errors.include.sql",
      "false"
    ],
    [
      "fs.s3a.select.input.compression",
      "none"
    ],
    [
      "fs.s3a.select.input.csv.comment.marker",
      "#"
    ],
    [
      "fs.s3a.select.input.csv.field.delimiter",
      ","
    ],
    [
      "fs.s3a.select.input.csv.header",
      "none"
    ],
    [
      "fs.s3a.select.input.csv.quote.character",
      "\""
    ],
    [
      "fs.s3a.select.input.csv.quote.escape.character",
      "\\\\"
    ],
    [
      "fs.s3a.select.input.csv.record.delimiter",
      "\\n"
    ],
    [
      "fs.s3a.select.output.csv.field.delimiter",
      ","
    ],
    [
      "fs.s3a.select.output.csv.quote.character",
      "\""
    ],
    [
      "fs.s3a.select.output.csv.quote.escape.character",
      "\\\\"
    ],
    [
      "fs.s3a.select.output.csv.quote.fields",
      "always"
    ],
    [
      "fs.s3a.select.output.csv.record.delimiter",
      "\\n"
    ],
    [
      "fs.s3a.socket.recv.buffer",
      "8192"
    ],
    [
      "fs.s3a.socket.send.buffer",
      "8192"
    ],
    [
      "fs.s3a.ssl.channel.mode",
      "default_jsse"
    ],
    [
      "fs.s3a.threads.keepalivetime",
      "60s"
    ],
    [
      "fs.s3a.threads.max",
      "96"
    ],
    [
      "fs.trash.checkpoint.interval",
      "0"
    ],
    [
      "fs.trash.clean.trashroot.enable",
      "false"
    ],
    [
      "fs.trash.interval",
      "0"
    ],
    [
      "fs.viewfs.overload.scheme.target.abfs.impl",
      "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.abfss.impl",
      "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.file.impl",
      "org.apache.hadoop.fs.LocalFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.ftp.impl",
      "org.apache.hadoop.fs.ftp.FTPFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.gs.impl",
      "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    ],
    [
      "fs.viewfs.overload.scheme.target.hdfs.impl",
      "org.apache.hadoop.hdfs.DistributedFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.http.impl",
      "org.apache.hadoop.fs.http.HttpFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.https.impl",
      "org.apache.hadoop.fs.http.HttpsFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.o3fs.impl",
      "org.apache.hadoop.fs.ozone.OzoneFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.ofs.impl",
      "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.oss.impl",
      "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.swebhdfs.impl",
      "org.apache.hadoop.hdfs.web.SWebHdfsFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.wasb.impl",
      "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    ],
    [
      "fs.viewfs.overload.scheme.target.webhdfs.impl",
      "org.apache.hadoop.hdfs.web.WebHdfsFileSystem"
    ],
    [
      "fs.viewfs.rename.strategy",
      "SAME_MOUNTPOINT"
    ],
    [
      "fs.wasb.impl",
      "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    ],
    [
      "fs.wasbs.impl",
      "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure"
    ],
    [
      "ftp.blocksize",
      "67108864"
    ],
    [
      "ftp.bytes-per-checksum",
      "512"
    ],
    [
      "ftp.client-write-packet-size",
      "65536"
    ],
    [
      "ftp.replication",
      "3"
    ],
    [
      "ftp.stream-buffer-size",
      "4096"
    ],
    [
      "ha.failover-controller.active-standby-elector.zk.op.retries",
      "3"
    ],
    [
      "ha.failover-controller.cli-check.rpc-timeout.ms",
      "20000"
    ],
    [
      "ha.failover-controller.graceful-fence.connection.retries",
      "1"
    ],
    [
      "ha.failover-controller.graceful-fence.rpc-timeout.ms",
      "5000"
    ],
    [
      "ha.failover-controller.new-active.rpc-timeout.ms",
      "60000"
    ],
    [
      "ha.health-monitor.check-interval.ms",
      "1000"
    ],
    [
      "ha.health-monitor.connect-retry-interval.ms",
      "1000"
    ],
    [
      "ha.health-monitor.rpc-timeout.ms",
      "45000"
    ],
    [
      "ha.health-monitor.rpc.connect.max.retries",
      "1"
    ],
    [
      "ha.health-monitor.sleep-after-disconnect.ms",
      "1000"
    ],
    [
      "ha.zookeeper.acl",
      "world:anyone:rwcda"
    ],
    [
      "ha.zookeeper.parent-znode",
      "/hadoop-ha"
    ],
    [
      "ha.zookeeper.session-timeout.ms",
      "10000"
    ],
    [
      "hadoop.caller.context.enabled",
      "false"
    ],
    [
      "hadoop.caller.context.max.size",
      "128"
    ],
    [
      "hadoop.caller.context.separator",
      ","
    ],
    [
      "hadoop.caller.context.signature.max.size",
      "40"
    ],
    [
      "hadoop.common.configuration.version",
      "3.0.0"
    ],
    [
      "hadoop.domainname.resolver.impl",
      "org.apache.hadoop.net.DNSDomainNameResolver"
    ],
    [
      "hadoop.http.authentication.kerberos.keytab",
      "${user.home}/hadoop.keytab"
    ],
    [
      "hadoop.http.authentication.kerberos.principal",
      "HTTP/_HOST@LOCALHOST"
    ],
    [
      "hadoop.http.authentication.signature.secret.file",
      "*********(redacted)"
    ],
    [
      "hadoop.http.authentication.simple.anonymous.allowed",
      "true"
    ],
    [
      "hadoop.http.authentication.token.validity",
      "*********(redacted)"
    ],
    [
      "hadoop.http.authentication.type",
      "simple"
    ],
    [
      "hadoop.http.cross-origin.allowed-headers",
      "X-Requested-With,Content-Type,Accept,Origin"
    ],
    [
      "hadoop.http.cross-origin.allowed-methods",
      "GET,POST,HEAD"
    ],
    [
      "hadoop.http.cross-origin.allowed-origins",
      "*"
    ],
    [
      "hadoop.http.cross-origin.enabled",
      "false"
    ],
    [
      "hadoop.http.cross-origin.max-age",
      "1800"
    ],
    [
      "hadoop.http.filter.initializers",
      "org.apache.hadoop.http.lib.StaticUserWebFilter"
    ],
    [
      "hadoop.http.idle_timeout.ms",
      "60000"
    ],
    [
      "hadoop.http.jmx.nan-filter.enabled",
      "false"
    ],
    [
      "hadoop.http.logs.enabled",
      "true"
    ],
    [
      "hadoop.http.metrics.enabled",
      "true"
    ],
    [
      "hadoop.http.sni.host.check.enabled",
      "false"
    ],
    [
      "hadoop.http.staticuser.user",
      "dr.who"
    ],
    [
      "hadoop.jetty.logs.serve.aliases",
      "true"
    ],
    [
      "hadoop.kerberos.keytab.login.autorenewal.enabled",
      "false"
    ],
    [
      "hadoop.kerberos.kinit.command",
      "kinit"
    ],
    [
      "hadoop.kerberos.min.seconds.before.relogin",
      "60"
    ],
    [
      "hadoop.metrics.jvm.use-thread-mxbean",
      "false"
    ],
    [
      "hadoop.prometheus.endpoint.enabled",
      "false"
    ],
    [
      "hadoop.registry.jaas.context",
      "Client"
    ],
    [
      "hadoop.registry.secure",
      "false"
    ],
    [
      "hadoop.registry.system.acls",
      "sasl:yarn@, sasl:mapred@, sasl:hdfs@"
    ],
    [
      "hadoop.registry.zk.connection.timeout.ms",
      "15000"
    ],
    [
      "hadoop.registry.zk.quorum",
      "localhost:2181"
    ],
    [
      "hadoop.registry.zk.retry.ceiling.ms",
      "60000"
    ],
    [
      "hadoop.registry.zk.retry.interval.ms",
      "1000"
    ],
    [
      "hadoop.registry.zk.retry.times",
      "5"
    ],
    [
      "hadoop.registry.zk.root",
      "/registry"
    ],
    [
      "hadoop.registry.zk.session.timeout.ms",
      "60000"
    ],
    [
      "hadoop.rpc.protection",
      "authentication"
    ],
    [
      "hadoop.rpc.socket.factory.class.default",
      "org.apache.hadoop.net.StandardSocketFactory"
    ],
    [
      "hadoop.security.auth_to_local.mechanism",
      "hadoop"
    ],
    [
      "hadoop.security.authentication",
      "simple"
    ],
    [
      "hadoop.security.authorization",
      "false"
    ],
    [
      "hadoop.security.credential.clear-text-fallback",
      "true"
    ],
    [
      "hadoop.security.crypto.buffer.size",
      "8192"
    ],
    [
      "hadoop.security.crypto.cipher.suite",
      "AES/CTR/NoPadding"
    ],
    [
      "hadoop.security.crypto.codec.classes.aes.ctr.nopadding",
      "org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec, org.apache.hadoop.crypto.JceAesCtrCryptoCodec"
    ],
    [
      "hadoop.security.crypto.codec.classes.sm4.ctr.nopadding",
      "org.apache.hadoop.crypto.OpensslSm4CtrCryptoCodec, org.apache.hadoop.crypto.JceSm4CtrCryptoCodec"
    ],
    [
      "hadoop.security.dns.log-slow-lookups.enabled",
      "false"
    ],
    [
      "hadoop.security.dns.log-slow-lookups.threshold.ms",
      "1000"
    ],
    [
      "hadoop.security.group.mapping",
      "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback"
    ],
    [
      "hadoop.security.group.mapping.ldap.connection.timeout.ms",
      "60000"
    ],
    [
      "hadoop.security.group.mapping.ldap.conversion.rule",
      "none"
    ],
    [
      "hadoop.security.group.mapping.ldap.directory.search.timeout",
      "10000"
    ],
    [
      "hadoop.security.group.mapping.ldap.num.attempts",
      "3"
    ],
    [
      "hadoop.security.group.mapping.ldap.num.attempts.before.failover",
      "3"
    ],
    [
      "hadoop.security.group.mapping.ldap.posix.attr.gid.name",
      "gidNumber"
    ],
    [
      "hadoop.security.group.mapping.ldap.posix.attr.uid.name",
      "uidNumber"
    ],
    [
      "hadoop.security.group.mapping.ldap.read.timeout.ms",
      "60000"
    ],
    [
      "hadoop.security.group.mapping.ldap.search.attr.group.name",
      "cn"
    ],
    [
      "hadoop.security.group.mapping.ldap.search.attr.member",
      "member"
    ],
    [
      "hadoop.security.group.mapping.ldap.search.filter.group",
      "(objectClass=group)"
    ],
    [
      "hadoop.security.group.mapping.ldap.search.filter.user",
      "(&(objectClass=user)(sAMAccountName={0}))"
    ],
    [
      "hadoop.security.group.mapping.ldap.search.group.hierarchy.levels",
      "0"
    ],
    [
      "hadoop.security.group.mapping.ldap.ssl",
      "false"
    ],
    [
      "hadoop.security.group.mapping.providers.combined",
      "true"
    ],
    [
      "hadoop.security.groups.cache.background.reload",
      "false"
    ],
    [
      "hadoop.security.groups.cache.background.reload.threads",
      "3"
    ],
    [
      "hadoop.security.groups.cache.secs",
      "300"
    ],
    [
      "hadoop.security.groups.cache.warn.after.ms",
      "5000"
    ],
    [
      "hadoop.security.groups.negative-cache.secs",
      "30"
    ],
    [
      "hadoop.security.groups.shell.command.timeout",
      "0s"
    ],
    [
      "hadoop.security.instrumentation.requires.admin",
      "false"
    ],
    [
      "hadoop.security.java.secure.random.algorithm",
      "SHA1PRNG"
    ],
    [
      "hadoop.security.key.default.bitlength",
      "128"
    ],
    [
      "hadoop.security.key.default.cipher",
      "AES/CTR/NoPadding"
    ],
    [
      "hadoop.security.kms.client.authentication.retry-count",
      "1"
    ],
    [
      "hadoop.security.kms.client.encrypted.key.cache.expiry",
      "43200000"
    ],
    [
      "hadoop.security.kms.client.encrypted.key.cache.low-watermark",
      "0.3f"
    ],
    [
      "hadoop.security.kms.client.encrypted.key.cache.num.refill.threads",
      "2"
    ],
    [
      "hadoop.security.kms.client.encrypted.key.cache.size",
      "500"
    ],
    [
      "hadoop.security.kms.client.failover.sleep.base.millis",
      "100"
    ],
    [
      "hadoop.security.kms.client.failover.sleep.max.millis",
      "2000"
    ],
    [
      "hadoop.security.kms.client.timeout",
      "60"
    ],
    [
      "hadoop.security.random.device.file.path",
      "/dev/urandom"
    ],
    [
      "hadoop.security.resolver.impl",
      "org.apache.hadoop.net.DNSDomainNameResolver"
    ],
    [
      "hadoop.security.secure.random.impl",
      "org.apache.hadoop.crypto.random.OpensslSecureRandom"
    ],
    [
      "hadoop.security.sensitive-config-keys",
      "*********(redacted)"
    ],
    [
      "hadoop.security.token.service.use_ip",
      "*********(redacted)"
    ],
    [
      "hadoop.security.uid.cache.secs",
      "14400"
    ],
    [
      "hadoop.service.shutdown.timeout",
      "30s"
    ],
    [
      "hadoop.shell.missing.defaultFs.warning",
      "false"
    ],
    [
      "hadoop.shell.safely.delete.limit.num.files",
      "100"
    ],
    [
      "hadoop.ssl.client.conf",
      "ssl-client.xml"
    ],
    [
      "hadoop.ssl.enabled.protocols",
      "TLSv1.2"
    ],
    [
      "hadoop.ssl.hostname.verifier",
      "DEFAULT"
    ],
    [
      "hadoop.ssl.keystores.factory.class",
      "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory"
    ],
    [
      "hadoop.ssl.require.client.cert",
      "false"
    ],
    [
      "hadoop.ssl.server.conf",
      "ssl-server.xml"
    ],
    [
      "hadoop.system.tags",
      "YARN,HDFS,NAMENODE,DATANODE,REQUIRED,SECURITY,KERBEROS,PERFORMANCE,CLIENT\n      ,SERVER,DEBUG,DEPRECATED,COMMON,OPTIONAL"
    ],
    [
      "hadoop.tags.system",
      "YARN,HDFS,NAMENODE,DATANODE,REQUIRED,SECURITY,KERBEROS,PERFORMANCE,CLIENT\n      ,SERVER,DEBUG,DEPRECATED,COMMON,OPTIONAL"
    ],
    [
      "hadoop.tmp.dir",
      "/tmp/hadoop-${user.name}"
    ],
    [
      "hadoop.user.group.static.mapping.overrides",
      "dr.who=;"
    ],
    [
      "hadoop.util.hash.type",
      "murmur"
    ],
    [
      "hadoop.workaround.non.threadsafe.getpwuid",
      "true"
    ],
    [
      "hadoop.zk.acl",
      "world:anyone:rwcda"
    ],
    [
      "hadoop.zk.num-retries",
      "1000"
    ],
    [
      "hadoop.zk.retry-interval-ms",
      "1000"
    ],
    [
      "hadoop.zk.timeout-ms",
      "10000"
    ],
    [
      "io.bytes.per.checksum",
      "512"
    ],
    [
      "io.compression.codec.bzip2.library",
      "system-native"
    ],
    [
      "io.compression.codec.lz4.buffersize",
      "262144"
    ],
    [
      "io.compression.codec.lz4.use.lz4hc",
      "false"
    ],
    [
      "io.compression.codec.lzo.buffersize",
      "65536"
    ],
    [
      "io.compression.codec.lzo.class",
      "org.apache.hadoop.io.compress.LzoCodec"
    ],
    [
      "io.compression.codec.snappy.buffersize",
      "262144"
    ],
    [
      "io.compression.codec.zstd.buffersize",
      "0"
    ],
    [
      "io.compression.codec.zstd.level",
      "3"
    ],
    [
      "io.erasurecode.codec.native.enabled",
      "true"
    ],
    [
      "io.erasurecode.codec.rs-legacy.rawcoders",
      "rs-legacy_java"
    ],
    [
      "io.erasurecode.codec.rs.rawcoders",
      "rs_native,rs_java"
    ],
    [
      "io.erasurecode.codec.xor.rawcoders",
      "xor_native,xor_java"
    ],
    [
      "io.file.buffer.size",
      "65536"
    ],
    [
      "io.map.index.interval",
      "128"
    ],
    [
      "io.map.index.skip",
      "0"
    ],
    [
      "io.mapfile.bloom.error.rate",
      "0.005"
    ],
    [
      "io.mapfile.bloom.size",
      "1048576"
    ],
    [
      "io.seqfile.compress.blocksize",
      "1000000"
    ],
    [
      "io.seqfile.local.dir",
      "${hadoop.tmp.dir}/io/local"
    ],
    [
      "io.serializations",
      "org.apache.hadoop.io.serializer.WritableSerialization, org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization, org.apache.hadoop.io.serializer.avro.AvroReflectSerialization"
    ],
    [
      "io.skip.checksum.errors",
      "false"
    ],
    [
      "ipc.[port_number].backoff.enable",
      "false"
    ],
    [
      "ipc.[port_number].callqueue.impl",
      "java.util.concurrent.LinkedBlockingQueue"
    ],
    [
      "ipc.[port_number].callqueue.overflow.trigger.failover",
      "false"
    ],
    [
      "ipc.[port_number].cost-provider.impl",
      "org.apache.hadoop.ipc.DefaultCostProvider"
    ],
    [
      "ipc.[port_number].decay-scheduler.backoff.responsetime.enable",
      "false"
    ],
    [
      "ipc.[port_number].decay-scheduler.backoff.responsetime.thresholds",
      "10s,20s,30s,40s"
    ],
    [
      "ipc.[port_number].decay-scheduler.decay-factor",
      "0.5"
    ],
    [
      "ipc.[port_number].decay-scheduler.metrics.top.user.count",
      "10"
    ],
    [
      "ipc.[port_number].decay-scheduler.period-ms",
      "5000"
    ],
    [
      "ipc.[port_number].decay-scheduler.thresholds",
      "13,25,50"
    ],
    [
      "ipc.[port_number].faircallqueue.multiplexer.weights",
      "8,4,2,1"
    ],
    [
      "ipc.[port_number].identity-provider.impl",
      "org.apache.hadoop.ipc.UserIdentityProvider"
    ],
    [
      "ipc.[port_number].scheduler.impl",
      "org.apache.hadoop.ipc.DefaultRpcScheduler"
    ],
    [
      "ipc.[port_number].scheduler.priority.levels",
      "4"
    ],
    [
      "ipc.[port_number].weighted-cost.handler",
      "1"
    ],
    [
      "ipc.[port_number].weighted-cost.lockexclusive",
      "100"
    ],
    [
      "ipc.[port_number].weighted-cost.lockfree",
      "1"
    ],
    [
      "ipc.[port_number].weighted-cost.lockshared",
      "10"
    ],
    [
      "ipc.[port_number].weighted-cost.response",
      "1"
    ],
    [
      "ipc.backoff.enable",
      "false"
    ],
    [
      "ipc.callqueue.impl",
      "java.util.concurrent.LinkedBlockingQueue"
    ],
    [
      "ipc.callqueue.overflow.trigger.failover",
      "false"
    ],
    [
      "ipc.client.async.calls.max",
      "100"
    ],
    [
      "ipc.client.bind.wildcard.addr",
      "false"
    ],
    [
      "ipc.client.connect.max.retries",
      "10"
    ],
    [
      "ipc.client.connect.max.retries.on.sasl",
      "5"
    ],
    [
      "ipc.client.connect.max.retries.on.timeouts",
      "45"
    ],
    [
      "ipc.client.connect.retry.interval",
      "1000"
    ],
    [
      "ipc.client.connect.timeout",
      "20000"
    ],
    [
      "ipc.client.connection.idle-scan-interval.ms",
      "10000"
    ],
    [
      "ipc.client.connection.maxidletime",
      "10000"
    ],
    [
      "ipc.client.fallback-to-simple-auth-allowed",
      "false"
    ],
    [
      "ipc.client.idlethreshold",
      "4000"
    ],
    [
      "ipc.client.kill.max",
      "10"
    ],
    [
      "ipc.client.low-latency",
      "false"
    ],
    [
      "ipc.client.ping",
      "true"
    ],
    [
      "ipc.client.rpc-timeout.ms",
      "120000"
    ],
    [
      "ipc.client.tcpnodelay",
      "true"
    ],
    [
      "ipc.cost-provider.impl",
      "org.apache.hadoop.ipc.DefaultCostProvider"
    ],
    [
      "ipc.identity-provider.impl",
      "org.apache.hadoop.ipc.UserIdentityProvider"
    ],
    [
      "ipc.maximum.data.length",
      "134217728"
    ],
    [
      "ipc.maximum.response.length",
      "134217728"
    ],
    [
      "ipc.ping.interval",
      "60000"
    ],
    [
      "ipc.scheduler.impl",
      "org.apache.hadoop.ipc.DefaultRpcScheduler"
    ],
    [
      "ipc.server.handler.queue.size",
      "100"
    ],
    [
      "ipc.server.listen.queue.size",
      "256"
    ],
    [
      "ipc.server.log.slow.rpc",
      "false"
    ],
    [
      "ipc.server.log.slow.rpc.threshold.ms",
      "0"
    ],
    [
      "ipc.server.max.connections",
      "0"
    ],
    [
      "ipc.server.max.response.size",
      "1048576"
    ],
    [
      "ipc.server.metrics.update.runner.interval",
      "5000"
    ],
    [
      "ipc.server.purge.interval",
      "15"
    ],
    [
      "ipc.server.read.connection-queue.size",
      "100"
    ],
    [
      "ipc.server.read.threadpool.size",
      "1"
    ],
    [
      "ipc.server.reuseaddr",
      "true"
    ],
    [
      "ipc.server.tcpnodelay",
      "true"
    ],
    [
      "map.sort.class",
      "org.apache.hadoop.util.QuickSort"
    ],
    [
      "mapreduce.am.max-attempts",
      "2"
    ],
    [
      "mapreduce.app-submission.cross-platform",
      "false"
    ],
    [
      "mapreduce.client.completion.pollinterval",
      "5000"
    ],
    [
      "mapreduce.client.libjars.wildcard",
      "true"
    ],
    [
      "mapreduce.client.output.filter",
      "FAILED"
    ],
    [
      "mapreduce.client.progressmonitor.pollinterval",
      "1000"
    ],
    [
      "mapreduce.client.submit.file.replication",
      "10"
    ],
    [
      "mapreduce.cluster.acls.enabled",
      "false"
    ],
    [
      "mapreduce.cluster.local.dir",
      "${hadoop.tmp.dir}/mapred/local"
    ],
    [
      "mapreduce.fileoutputcommitter.algorithm.version",
      "1"
    ],
    [
      "mapreduce.fileoutputcommitter.task.cleanup.enabled",
      "false"
    ],
    [
      "mapreduce.framework.name",
      "local"
    ],
    [
      "mapreduce.ifile.readahead",
      "true"
    ],
    [
      "mapreduce.ifile.readahead.bytes",
      "4194304"
    ],
    [
      "mapreduce.input.fileinputformat.list-status.num-threads",
      "1"
    ],
    [
      "mapreduce.input.fileinputformat.split.minsize",
      "0"
    ],
    [
      "mapreduce.input.lineinputformat.linespermap",
      "1"
    ],
    [
      "mapreduce.job.acl-modify-job",
      " "
    ],
    [
      "mapreduce.job.acl-view-job",
      " "
    ],
    [
      "mapreduce.job.cache.limit.max-resources",
      "0"
    ],
    [
      "mapreduce.job.cache.limit.max-resources-mb",
      "0"
    ],
    [
      "mapreduce.job.cache.limit.max-single-resource-mb",
      "0"
    ],
    [
      "mapreduce.job.classloader",
      "false"
    ],
    [
      "mapreduce.job.committer.setup.cleanup.needed",
      "true"
    ],
    [
      "mapreduce.job.complete.cancel.delegation.tokens",
      "*********(redacted)"
    ],
    [
      "mapreduce.job.counters.max",
      "120"
    ],
    [
      "mapreduce.job.dfs.storage.capacity.kill-limit-exceed",
      "false"
    ],
    [
      "mapreduce.job.emit-timeline-data",
      "false"
    ],
    [
      "mapreduce.job.encrypted-intermediate-data",
      "false"
    ],
    [
      "mapreduce.job.encrypted-intermediate-data-key-size-bits",
      "128"
    ],
    [
      "mapreduce.job.encrypted-intermediate-data.buffer.kb",
      "128"
    ],
    [
      "mapreduce.job.end-notification.max.attempts",
      "5"
    ],
    [
      "mapreduce.job.end-notification.max.retry.interval",
      "5000"
    ],
    [
      "mapreduce.job.end-notification.retry.attempts",
      "0"
    ],
    [
      "mapreduce.job.end-notification.retry.interval",
      "1000"
    ],
    [
      "mapreduce.job.finish-when-all-reducers-done",
      "true"
    ],
    [
      "mapreduce.job.hdfs-servers",
      "${fs.defaultFS}"
    ],
    [
      "mapreduce.job.heap.memory-mb.ratio",
      "0.8"
    ],
    [
      "mapreduce.job.local-fs.single-disk-limit.bytes",
      "-1"
    ],
    [
      "mapreduce.job.local-fs.single-disk-limit.check.interval-ms",
      "5000"
    ],
    [
      "mapreduce.job.local-fs.single-disk-limit.check.kill-limit-exceed",
      "true"
    ],
    [
      "mapreduce.job.map.output.collector.class",
      "org.apache.hadoop.mapred.MapTask$MapOutputBuffer"
    ],
    [
      "mapreduce.job.maps",
      "2"
    ],
    [
      "mapreduce.job.max.map",
      "-1"
    ],
    [
      "mapreduce.job.max.split.locations",
      "15"
    ],
    [
      "mapreduce.job.maxtaskfailures.per.tracker",
      "3"
    ],
    [
      "mapreduce.job.queuename",
      "default"
    ],
    [
      "mapreduce.job.reduce.shuffle.consumer.plugin.class",
      "org.apache.hadoop.mapreduce.task.reduce.Shuffle"
    ],
    [
      "mapreduce.job.reduce.slowstart.completedmaps",
      "0.05"
    ],
    [
      "mapreduce.job.reducer.preempt.delay.sec",
      "0"
    ],
    [
      "mapreduce.job.reducer.unconditional-preempt.delay.sec",
      "300"
    ],
    [
      "mapreduce.job.reduces",
      "1"
    ],
    [
      "mapreduce.job.running.map.limit",
      "0"
    ],
    [
      "mapreduce.job.running.reduce.limit",
      "0"
    ],
    [
      "mapreduce.job.sharedcache.mode",
      "disabled"
    ],
    [
      "mapreduce.job.speculative.minimum-allowed-tasks",
      "10"
    ],
    [
      "mapreduce.job.speculative.retry-after-no-speculate",
      "1000"
    ],
    [
      "mapreduce.job.speculative.retry-after-speculate",
      "15000"
    ],
    [
      "mapreduce.job.speculative.slowtaskthreshold",
      "1.0"
    ],
    [
      "mapreduce.job.speculative.speculative-cap-running-tasks",
      "0.1"
    ],
    [
      "mapreduce.job.speculative.speculative-cap-total-tasks",
      "0.01"
    ],
    [
      "mapreduce.job.split.metainfo.maxsize",
      "10000000"
    ],
    [
      "mapreduce.job.token.tracking.ids.enabled",
      "*********(redacted)"
    ],
    [
      "mapreduce.job.ubertask.enable",
      "false"
    ],
    [
      "mapreduce.job.ubertask.maxmaps",
      "9"
    ],
    [
      "mapreduce.job.ubertask.maxreduces",
      "1"
    ],
    [
      "mapreduce.jobhistory.address",
      "0.0.0.0:10020"
    ],
    [
      "mapreduce.jobhistory.admin.acl",
      "*"
    ],
    [
      "mapreduce.jobhistory.admin.address",
      "0.0.0.0:10033"
    ],
    [
      "mapreduce.jobhistory.always-scan-user-dir",
      "false"
    ],
    [
      "mapreduce.jobhistory.cleaner.enable",
      "true"
    ],
    [
      "mapreduce.jobhistory.cleaner.interval-ms",
      "86400000"
    ],
    [
      "mapreduce.jobhistory.client.thread-count",
      "10"
    ],
    [
      "mapreduce.jobhistory.datestring.cache.size",
      "200000"
    ],
    [
      "mapreduce.jobhistory.done-dir",
      "${yarn.app.mapreduce.am.staging-dir}/history/done"
    ],
    [
      "mapreduce.jobhistory.http.policy",
      "HTTP_ONLY"
    ],
    [
      "mapreduce.jobhistory.intermediate-done-dir",
      "${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate"
    ],
    [
      "mapreduce.jobhistory.intermediate-user-done-dir.permissions",
      "770"
    ],
    [
      "mapreduce.jobhistory.jhist.format",
      "binary"
    ],
    [
      "mapreduce.jobhistory.joblist.cache.size",
      "20000"
    ],
    [
      "mapreduce.jobhistory.jobname.limit",
      "50"
    ],
    [
      "mapreduce.jobhistory.keytab",
      "/etc/security/keytab/jhs.service.keytab"
    ],
    [
      "mapreduce.jobhistory.loadedjob.tasks.max",
      "-1"
    ],
    [
      "mapreduce.jobhistory.loadedjobs.cache.size",
      "5"
    ],
    [
      "mapreduce.jobhistory.max-age-ms",
      "604800000"
    ],
    [
      "mapreduce.jobhistory.minicluster.fixed.ports",
      "false"
    ],
    [
      "mapreduce.jobhistory.move.interval-ms",
      "180000"
    ],
    [
      "mapreduce.jobhistory.move.thread-count",
      "3"
    ],
    [
      "mapreduce.jobhistory.principal",
      "jhs/_HOST@REALM.TLD"
    ],
    [
      "mapreduce.jobhistory.recovery.enable",
      "false"
    ],
    [
      "mapreduce.jobhistory.recovery.store.class",
      "org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService"
    ],
    [
      "mapreduce.jobhistory.recovery.store.fs.uri",
      "${hadoop.tmp.dir}/mapred/history/recoverystore"
    ],
    [
      "mapreduce.jobhistory.recovery.store.leveldb.path",
      "${hadoop.tmp.dir}/mapred/history/recoverystore"
    ],
    [
      "mapreduce.jobhistory.webapp.address",
      "0.0.0.0:19888"
    ],
    [
      "mapreduce.jobhistory.webapp.https.address",
      "0.0.0.0:19890"
    ],
    [
      "mapreduce.jobhistory.webapp.rest-csrf.custom-header",
      "X-XSRF-Header"
    ],
    [
      "mapreduce.jobhistory.webapp.rest-csrf.enabled",
      "false"
    ],
    [
      "mapreduce.jobhistory.webapp.rest-csrf.methods-to-ignore",
      "GET,OPTIONS,HEAD"
    ],
    [
      "mapreduce.jobhistory.webapp.xfs-filter.xframe-options",
      "SAMEORIGIN"
    ],
    [
      "mapreduce.jvm.add-opens-as-default",
      "true"
    ],
    [
      "mapreduce.jvm.system-properties-to-log",
      "os.name,os.version,java.home,java.runtime.version,java.vendor,java.version,java.vm.name,java.class.path,java.io.tmpdir,user.dir,user.name"
    ],
    [
      "mapreduce.map.cpu.vcores",
      "1"
    ],
    [
      "mapreduce.map.log.level",
      "INFO"
    ],
    [
      "mapreduce.map.maxattempts",
      "4"
    ],
    [
      "mapreduce.map.memory.mb",
      "-1"
    ],
    [
      "mapreduce.map.output.compress",
      "false"
    ],
    [
      "mapreduce.map.output.compress.codec",
      "org.apache.hadoop.io.compress.DefaultCodec"
    ],
    [
      "mapreduce.map.skip.maxrecords",
      "0"
    ],
    [
      "mapreduce.map.skip.proc-count.auto-incr",
      "true"
    ],
    [
      "mapreduce.map.sort.spill.percent",
      "0.80"
    ],
    [
      "mapreduce.map.speculative",
      "true"
    ],
    [
      "mapreduce.output.fileoutputformat.compress",
      "false"
    ],
    [
      "mapreduce.output.fileoutputformat.compress.codec",
      "org.apache.hadoop.io.compress.DefaultCodec"
    ],
    [
      "mapreduce.output.fileoutputformat.compress.type",
      "RECORD"
    ],
    [
      "mapreduce.outputcommitter.factory.scheme.abfs",
      "org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory"
    ],
    [
      "mapreduce.outputcommitter.factory.scheme.gs",
      "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory"
    ],
    [
      "mapreduce.outputcommitter.factory.scheme.s3a",
      "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
    ],
    [
      "mapreduce.reduce.cpu.vcores",
      "1"
    ],
    [
      "mapreduce.reduce.input.buffer.percent",
      "0.0"
    ],
    [
      "mapreduce.reduce.log.level",
      "INFO"
    ],
    [
      "mapreduce.reduce.markreset.buffer.percent",
      "0.0"
    ],
    [
      "mapreduce.reduce.maxattempts",
      "4"
    ],
    [
      "mapreduce.reduce.memory.mb",
      "-1"
    ],
    [
      "mapreduce.reduce.merge.inmem.threshold",
      "1000"
    ],
    [
      "mapreduce.reduce.shuffle.connect.timeout",
      "180000"
    ],
    [
      "mapreduce.reduce.shuffle.fetch.retry.enabled",
      "${yarn.nodemanager.recovery.enabled}"
    ],
    [
      "mapreduce.reduce.shuffle.fetch.retry.interval-ms",
      "1000"
    ],
    [
      "mapreduce.reduce.shuffle.fetch.retry.timeout-ms",
      "30000"
    ],
    [
      "mapreduce.reduce.shuffle.input.buffer.percent",
      "0.70"
    ],
    [
      "mapreduce.reduce.shuffle.memory.limit.percent",
      "0.25"
    ],
    [
      "mapreduce.reduce.shuffle.merge.percent",
      "0.66"
    ],
    [
      "mapreduce.reduce.shuffle.parallelcopies",
      "5"
    ],
    [
      "mapreduce.reduce.shuffle.read.timeout",
      "180000"
    ],
    [
      "mapreduce.reduce.shuffle.retry-delay.max.ms",
      "60000"
    ],
    [
      "mapreduce.reduce.skip.maxgroups",
      "0"
    ],
    [
      "mapreduce.reduce.skip.proc-count.auto-incr",
      "true"
    ],
    [
      "mapreduce.reduce.speculative",
      "true"
    ],
    [
      "mapreduce.shuffle.connection-keep-alive.enable",
      "false"
    ],
    [
      "mapreduce.shuffle.connection-keep-alive.timeout",
      "5"
    ],
    [
      "mapreduce.shuffle.listen.queue.size",
      "128"
    ],
    [
      "mapreduce.shuffle.max.connections",
      "0"
    ],
    [
      "mapreduce.shuffle.max.threads",
      "0"
    ],
    [
      "mapreduce.shuffle.pathcache.concurrency-level",
      "16"
    ],
    [
      "mapreduce.shuffle.pathcache.expire-after-access-minutes",
      "5"
    ],
    [
      "mapreduce.shuffle.pathcache.max-weight",
      "10485760"
    ],
    [
      "mapreduce.shuffle.port",
      "13562"
    ],
    [
      "mapreduce.shuffle.ssl.enabled",
      "false"
    ],
    [
      "mapreduce.shuffle.ssl.file.buffer.size",
      "65536"
    ],
    [
      "mapreduce.shuffle.transfer.buffer.size",
      "131072"
    ],
    [
      "mapreduce.task.combine.progress.records",
      "10000"
    ],
    [
      "mapreduce.task.exit.timeout",
      "60000"
    ],
    [
      "mapreduce.task.exit.timeout.check-interval-ms",
      "20000"
    ],
    [
      "mapreduce.task.files.preserve.failedtasks",
      "false"
    ],
    [
      "mapreduce.task.io.sort.factor",
      "10"
    ],
    [
      "mapreduce.task.io.sort.mb",
      "100"
    ],
    [
      "mapreduce.task.local-fs.write-limit.bytes",
      "-1"
    ],
    [
      "mapreduce.task.merge.progress.records",
      "10000"
    ],
    [
      "mapreduce.task.ping-for-liveliness-check.enabled",
      "false"
    ],
    [
      "mapreduce.task.profile",
      "false"
    ],
    [
      "mapreduce.task.profile.map.params",
      "${mapreduce.task.profile.params}"
    ],
    [
      "mapreduce.task.profile.maps",
      "0-2"
    ],
    [
      "mapreduce.task.profile.params",
      "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
    ],
    [
      "mapreduce.task.profile.reduce.params",
      "${mapreduce.task.profile.params}"
    ],
    [
      "mapreduce.task.profile.reduces",
      "0-2"
    ],
    [
      "mapreduce.task.skip.start.attempts",
      "2"
    ],
    [
      "mapreduce.task.spill.files.count.limit",
      "-1"
    ],
    [
      "mapreduce.task.stuck.timeout-ms",
      "600000"
    ],
    [
      "mapreduce.task.timeout",
      "600000"
    ],
    [
      "mapreduce.task.userlog.limit.kb",
      "0"
    ],
    [
      "net.topology.impl",
      "org.apache.hadoop.net.NetworkTopology"
    ],
    [
      "net.topology.node.switch.mapping.impl",
      "org.apache.hadoop.net.ScriptBasedMapping"
    ],
    [
      "net.topology.script.number.args",
      "100"
    ],
    [
      "nfs.exports.allowed.hosts",
      "* rw"
    ],
    [
      "rpc.metrics.quantile.enable",
      "false"
    ],
    [
      "rpc.metrics.timeunit",
      "MILLISECONDS"
    ],
    [
      "seq.io.sort.factor",
      "100"
    ],
    [
      "seq.io.sort.mb",
      "100"
    ],
    [
      "tfile.fs.input.buffer.size",
      "262144"
    ],
    [
      "tfile.fs.output.buffer.size",
      "262144"
    ],
    [
      "tfile.io.chunk.size",
      "1048576"
    ],
    [
      "yarn.acl.enable",
      "false"
    ],
    [
      "yarn.acl.reservation-enable",
      "false"
    ],
    [
      "yarn.admin.acl",
      "*"
    ],
    [
      "yarn.am.liveness-monitor.expiry-interval-ms",
      "900000"
    ],
    [
      "yarn.app.attempt.diagnostics.limit.kc",
      "64"
    ],
    [
      "yarn.app.mapreduce.am.command-opts",
      "-Xmx1024m"
    ],
    [
      "yarn.app.mapreduce.am.container.log.backups",
      "0"
    ],
    [
      "yarn.app.mapreduce.am.container.log.limit.kb",
      "0"
    ],
    [
      "yarn.app.mapreduce.am.containerlauncher.threadpool-initial-size",
      "10"
    ],
    [
      "yarn.app.mapreduce.am.hard-kill-timeout-ms",
      "10000"
    ],
    [
      "yarn.app.mapreduce.am.job.committer.cancel-timeout",
      "60000"
    ],
    [
      "yarn.app.mapreduce.am.job.committer.commit-window",
      "10000"
    ],
    [
      "yarn.app.mapreduce.am.job.task.listener.thread-count",
      "30"
    ],
    [
      "yarn.app.mapreduce.am.log.level",
      "INFO"
    ],
    [
      "yarn.app.mapreduce.am.resource.cpu-vcores",
      "1"
    ],
    [
      "yarn.app.mapreduce.am.resource.mb",
      "1536"
    ],
    [
      "yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms",
      "1000"
    ],
    [
      "yarn.app.mapreduce.am.staging-dir",
      "/tmp/hadoop-yarn/staging"
    ],
    [
      "yarn.app.mapreduce.am.staging-dir.erasurecoding.enabled",
      "false"
    ],
    [
      "yarn.app.mapreduce.am.webapp.https.client.auth",
      "false"
    ],
    [
      "yarn.app.mapreduce.am.webapp.https.enabled",
      "false"
    ],
    [
      "yarn.app.mapreduce.client-am.ipc.max-retries",
      "3"
    ],
    [
      "yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts",
      "3"
    ],
    [
      "yarn.app.mapreduce.client.job.max-retries",
      "3"
    ],
    [
      "yarn.app.mapreduce.client.job.retry-interval",
      "2000"
    ],
    [
      "yarn.app.mapreduce.client.max-retries",
      "3"
    ],
    [
      "yarn.app.mapreduce.shuffle.log.backups",
      "0"
    ],
    [
      "yarn.app.mapreduce.shuffle.log.limit.kb",
      "0"
    ],
    [
      "yarn.app.mapreduce.shuffle.log.separate",
      "true"
    ],
    [
      "yarn.app.mapreduce.task.container.log.backups",
      "0"
    ],
    [
      "yarn.apps.cache.enable",
      "false"
    ],
    [
      "yarn.apps.cache.expire",
      "30s"
    ],
    [
      "yarn.apps.cache.size",
      "1000"
    ],
    [
      "yarn.client.application-client-protocol.poll-interval-ms",
      "200"
    ],
    [
      "yarn.client.application-client-protocol.poll-timeout-ms",
      "-1"
    ],
    [
      "yarn.client.failover-no-ha-proxy-provider",
      "org.apache.hadoop.yarn.client.DefaultNoHARMFailoverProxyProvider"
    ],
    [
      "yarn.client.failover-proxy-provider",
      "org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider"
    ],
    [
      "yarn.client.failover-retries",
      "0"
    ],
    [
      "yarn.client.failover-retries-on-socket-timeouts",
      "0"
    ],
    [
      "yarn.client.load.resource-types.from-server",
      "false"
    ],
    [
      "yarn.client.max-cached-nodemanagers-proxies",
      "0"
    ],
    [
      "yarn.client.nodemanager-client-async.thread-pool-max-size",
      "500"
    ],
    [
      "yarn.client.nodemanager-connect.max-wait-ms",
      "180000"
    ],
    [
      "yarn.client.nodemanager-connect.retry-interval-ms",
      "10000"
    ],
    [
      "yarn.cluster.max-application-priority",
      "0"
    ],
    [
      "yarn.dispatcher.cpu-monitor.samples-per-min",
      "60"
    ],
    [
      "yarn.dispatcher.drain-events.timeout",
      "300000"
    ],
    [
      "yarn.dispatcher.print-events-info.threshold",
      "5000"
    ],
    [
      "yarn.dispatcher.print-thread-pool.core-pool-size",
      "1"
    ],
    [
      "yarn.dispatcher.print-thread-pool.keep-alive-time",
      "10s"
    ],
    [
      "yarn.dispatcher.print-thread-pool.maximum-pool-size",
      "5"
    ],
    [
      "yarn.fail-fast",
      "false"
    ],
    [
      "yarn.federation.amrmproxy.allocation.history.max.entry",
      "100"
    ],
    [
      "yarn.federation.amrmproxy.register.uam.interval",
      "100ms"
    ],
    [
      "yarn.federation.amrmproxy.register.uam.retry-count",
      "3"
    ],
    [
      "yarn.federation.cache-entity.nums",
      "1000"
    ],
    [
      "yarn.federation.cache-ttl.secs",
      "300"
    ],
    [
      "yarn.federation.cache.class",
      "org.apache.hadoop.yarn.server.federation.cache.FederationJCache"
    ],
    [
      "yarn.federation.enabled",
      "false"
    ],
    [
      "yarn.federation.failover.random.order",
      "false"
    ],
    [
      "yarn.federation.gpg.application.cleaner.class",
      "org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner.DefaultApplicationCleaner"
    ],
    [
      "yarn.federation.gpg.application.cleaner.contact.router.spec",
      "3,10,600000"
    ],
    [
      "yarn.federation.gpg.application.cleaner.interval-ms",
      "-1s"
    ],
    [
      "yarn.federation.gpg.policy.generator.class",
      "org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator.NoOpGlobalPolicy"
    ],
    [
      "yarn.federation.gpg.policy.generator.interval",
      "1h"
    ],
    [
      "yarn.federation.gpg.policy.generator.interval-ms",
      "3600000"
    ],
    [
      "yarn.federation.gpg.policy.generator.load-based.edit.maximum",
      "3"
    ],
    [
      "yarn.federation.gpg.policy.generator.load-based.pending.maximum",
      "1000"
    ],
    [
      "yarn.federation.gpg.policy.generator.load-based.pending.minimum",
      "100"
    ],
    [
      "yarn.federation.gpg.policy.generator.load-based.scaling",
      "LINEAR"
    ],
    [
      "yarn.federation.gpg.policy.generator.load-based.weight.minimum",
      "0"
    ],
    [
      "yarn.federation.gpg.policy.generator.readonly",
      "false"
    ],
    [
      "yarn.federation.gpg.scheduled.executor.threads",
      "10"
    ],
    [
      "yarn.federation.gpg.subcluster.cleaner.interval-ms",
      "-1ms"
    ],
    [
      "yarn.federation.gpg.subcluster.heartbeat.expiration-ms",
      "30m"
    ],
    [
      "yarn.federation.gpg.webapp.address",
      "0.0.0.0:8069"
    ],
    [
      "yarn.federation.gpg.webapp.connect-timeout",
      "30s"
    ],
    [
      "yarn.federation.gpg.webapp.cross-origin.enabled",
      "false"
    ],
    [
      "yarn.federation.gpg.webapp.https.address",
      "0.0.0.0:8070"
    ],
    [
      "yarn.federation.gpg.webapp.read-timeout",
      "30s"
    ],
    [
      "yarn.federation.non-ha.enabled",
      "false"
    ],
    [
      "yarn.federation.registry.base-dir",
      "yarnfederation/"
    ],
    [
      "yarn.federation.state-store.class",
      "org.apache.hadoop.yarn.server.federation.store.impl.MemoryFederationStateStore"
    ],
    [
      "yarn.federation.state-store.clean-up-retry-count",
      "1"
    ],
    [
      "yarn.federation.state-store.clean-up-retry-sleep-time",
      "1s"
    ],
    [
      "yarn.federation.state-store.heartbeat.initial-delay",
      "30s"
    ],
    [
      "yarn.federation.state-store.max-applications",
      "1000"
    ],
    [
      "yarn.federation.state-store.sql.conn-time-out",
      "10s"
    ],
    [
      "yarn.federation.state-store.sql.idle-time-out",
      "10m"
    ],
    [
      "yarn.federation.state-store.sql.max-life-time",
      "30m"
    ],
    [
      "yarn.federation.state-store.sql.minimum-idle",
      "1"
    ],
    [
      "yarn.federation.state-store.sql.pool-name",
      "YARN-Federation-DataBasePool"
    ],
    [
      "yarn.federation.subcluster-resolver.class",
      "org.apache.hadoop.yarn.server.federation.resolver.DefaultSubClusterResolverImpl"
    ],
    [
      "yarn.fs-store.file.replication",
      "0"
    ],
    [
      "yarn.http.policy",
      "HTTP_ONLY"
    ],
    [
      "yarn.intermediate-data-encryption.enable",
      "false"
    ],
    [
      "yarn.ipc.rpc.class",
      "org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC"
    ],
    [
      "yarn.is.minicluster",
      "false"
    ],
    [
      "yarn.log-aggregation-enable",
      "false"
    ],
    [
      "yarn.log-aggregation-status.time-out.ms",
      "600000"
    ],
    [
      "yarn.log-aggregation.debug.filesize",
      "104857600"
    ],
    [
      "yarn.log-aggregation.enable-local-cleanup",
      "true"
    ],
    [
      "yarn.log-aggregation.file-controller.TFile.class",
      "org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController"
    ],
    [
      "yarn.log-aggregation.file-formats",
      "TFile"
    ],
    [
      "yarn.log-aggregation.retain-check-interval-seconds",
      "-1"
    ],
    [
      "yarn.log-aggregation.retain-seconds",
      "-1"
    ],
    [
      "yarn.minicluster.control-resource-monitoring",
      "false"
    ],
    [
      "yarn.minicluster.fixed.ports",
      "false"
    ],
    [
      "yarn.minicluster.use-rpc",
      "false"
    ],
    [
      "yarn.minicluster.yarn.nodemanager.resource.memory-mb",
      "4096"
    ],
    [
      "yarn.nm.liveness-monitor.expiry-interval-ms",
      "600000"
    ],
    [
      "yarn.node-attribute.fs-store.impl.class",
      "org.apache.hadoop.yarn.server.resourcemanager.nodelabels.FileSystemNodeAttributeStore"
    ],
    [
      "yarn.node-labels.configuration-type",
      "centralized"
    ],
    [
      "yarn.node-labels.enabled",
      "false"
    ],
    [
      "yarn.node-labels.fs-store.impl.class",
      "org.apache.hadoop.yarn.nodelabels.FileSystemNodeLabelsStore"
    ],
    [
      "yarn.nodemanager.address",
      "${yarn.nodemanager.hostname}:0"
    ],
    [
      "yarn.nodemanager.admin-env",
      "MALLOC_ARENA_MAX=$MALLOC_ARENA_MAX"
    ],
    [
      "yarn.nodemanager.amrmproxy.address",
      "0.0.0.0:8049"
    ],
    [
      "yarn.nodemanager.amrmproxy.client.thread-count",
      "25"
    ],
    [
      "yarn.nodemanager.amrmproxy.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.amrmproxy.ha.enable",
      "false"
    ],
    [
      "yarn.nodemanager.amrmproxy.interceptor-class.pipeline",
      "org.apache.hadoop.yarn.server.nodemanager.amrmproxy.DefaultRequestInterceptor"
    ],
    [
      "yarn.nodemanager.amrmproxy.wait.uam-register.done",
      "false"
    ],
    [
      "yarn.nodemanager.aux-services.%s.classpath",
      "NONE"
    ],
    [
      "yarn.nodemanager.aux-services.manifest.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.aux-services.manifest.reload-ms",
      "0"
    ],
    [
      "yarn.nodemanager.aux-services.mapreduce_shuffle.class",
      "org.apache.hadoop.mapred.ShuffleHandler"
    ],
    [
      "yarn.nodemanager.collector-service.address",
      "${yarn.nodemanager.hostname}:8048"
    ],
    [
      "yarn.nodemanager.collector-service.thread-count",
      "5"
    ],
    [
      "yarn.nodemanager.container-diagnostics-maximum-size",
      "10000"
    ],
    [
      "yarn.nodemanager.container-executor.class",
      "org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor"
    ],
    [
      "yarn.nodemanager.container-executor.exit-code-file.timeout-ms",
      "2000"
    ],
    [
      "yarn.nodemanager.container-localizer.java.opts",
      "-Xmx256m"
    ],
    [
      "yarn.nodemanager.container-localizer.java.opts.add-exports-as-default",
      "true"
    ],
    [
      "yarn.nodemanager.container-localizer.log.level",
      "INFO"
    ],
    [
      "yarn.nodemanager.container-log-monitor.dir-size-limit-bytes",
      "1000000000"
    ],
    [
      "yarn.nodemanager.container-log-monitor.enable",
      "false"
    ],
    [
      "yarn.nodemanager.container-log-monitor.interval-ms",
      "60000"
    ],
    [
      "yarn.nodemanager.container-log-monitor.total-size-limit-bytes",
      "10000000000"
    ],
    [
      "yarn.nodemanager.container-manager.thread-count",
      "20"
    ],
    [
      "yarn.nodemanager.container-metrics.enable",
      "true"
    ],
    [
      "yarn.nodemanager.container-metrics.period-ms",
      "-1"
    ],
    [
      "yarn.nodemanager.container-metrics.unregister-delay-ms",
      "10000"
    ],
    [
      "yarn.nodemanager.container-monitor.enabled",
      "true"
    ],
    [
      "yarn.nodemanager.container-monitor.procfs-tree.smaps-based-rss.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.container-retry-minimum-interval-ms",
      "1000"
    ],
    [
      "yarn.nodemanager.container.stderr.pattern",
      "{*stderr*,*STDERR*}"
    ],
    [
      "yarn.nodemanager.container.stderr.tail.bytes",
      "4096"
    ],
    [
      "yarn.nodemanager.containers-launcher.class",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher"
    ],
    [
      "yarn.nodemanager.default-container-executor.log-dirs.permissions",
      "710"
    ],
    [
      "yarn.nodemanager.delete.debug-delay-sec",
      "0"
    ],
    [
      "yarn.nodemanager.delete.thread-count",
      "4"
    ],
    [
      "yarn.nodemanager.disk-health-checker.disk-free-space-threshold.enabled",
      "true"
    ],
    [
      "yarn.nodemanager.disk-health-checker.disk-utilization-threshold.enabled",
      "true"
    ],
    [
      "yarn.nodemanager.disk-health-checker.enable",
      "true"
    ],
    [
      "yarn.nodemanager.disk-health-checker.interval-ms",
      "120000"
    ],
    [
      "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "90.0"
    ],
    [
      "yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb",
      "0"
    ],
    [
      "yarn.nodemanager.disk-health-checker.min-free-space-per-disk-watermark-high-mb",
      "0"
    ],
    [
      "yarn.nodemanager.disk-health-checker.min-healthy-disks",
      "0.25"
    ],
    [
      "yarn.nodemanager.disk-validator",
      "basic"
    ],
    [
      "yarn.nodemanager.dispatcher.metric.enable",
      "false"
    ],
    [
      "yarn.nodemanager.distributed-scheduling.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.elastic-memory-control.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.elastic-memory-control.oom-handler",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.DefaultOOMHandler"
    ],
    [
      "yarn.nodemanager.elastic-memory-control.timeout-sec",
      "5"
    ],
    [
      "yarn.nodemanager.emit-container-events",
      "true"
    ],
    [
      "yarn.nodemanager.env-whitelist",
      "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ"
    ],
    [
      "yarn.nodemanager.health-checker.interval-ms",
      "600000"
    ],
    [
      "yarn.nodemanager.health-checker.run-before-startup",
      "false"
    ],
    [
      "yarn.nodemanager.health-checker.scripts",
      "script"
    ],
    [
      "yarn.nodemanager.health-checker.timeout-ms",
      "1200000"
    ],
    [
      "yarn.nodemanager.hostname",
      "0.0.0.0"
    ],
    [
      "yarn.nodemanager.keytab",
      "/etc/krb5.keytab"
    ],
    [
      "yarn.nodemanager.least-load-policy-selector.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.least-load-policy-selector.fail-on-error",
      "true"
    ],
    [
      "yarn.nodemanager.least-load-policy-selector.multiplier",
      "50000"
    ],
    [
      "yarn.nodemanager.least-load-policy-selector.pending-container.threshold",
      "10000"
    ],
    [
      "yarn.nodemanager.least-load-policy-selector.use-active-core",
      "false"
    ],
    [
      "yarn.nodemanager.linux-container-executor.cgroups.delete-delay-ms",
      "20"
    ],
    [
      "yarn.nodemanager.linux-container-executor.cgroups.delete-timeout-ms",
      "1000"
    ],
    [
      "yarn.nodemanager.linux-container-executor.cgroups.hierarchy",
      "/hadoop-yarn"
    ],
    [
      "yarn.nodemanager.linux-container-executor.cgroups.mount",
      "false"
    ],
    [
      "yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage",
      "false"
    ],
    [
      "yarn.nodemanager.linux-container-executor.nonsecure-mode.limit-users",
      "true"
    ],
    [
      "yarn.nodemanager.linux-container-executor.nonsecure-mode.local-user",
      "nobody"
    ],
    [
      "yarn.nodemanager.linux-container-executor.nonsecure-mode.user-pattern",
      "^[_.A-Za-z0-9][-@_.A-Za-z0-9]{0,255}?[$]?$"
    ],
    [
      "yarn.nodemanager.linux-container-executor.resources-handler.class",
      "org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler"
    ],
    [
      "yarn.nodemanager.local-cache.max-files-per-directory",
      "8192"
    ],
    [
      "yarn.nodemanager.local-dirs",
      "${hadoop.tmp.dir}/nm-local-dir"
    ],
    [
      "yarn.nodemanager.localizer.address",
      "${yarn.nodemanager.hostname}:8040"
    ],
    [
      "yarn.nodemanager.localizer.cache.cleanup.interval-ms",
      "600000"
    ],
    [
      "yarn.nodemanager.localizer.cache.target-size-mb",
      "10240"
    ],
    [
      "yarn.nodemanager.localizer.client.thread-count",
      "5"
    ],
    [
      "yarn.nodemanager.localizer.fetch.thread-count",
      "4"
    ],
    [
      "yarn.nodemanager.log-aggregation.compression-type",
      "none"
    ],
    [
      "yarn.nodemanager.log-aggregation.num-log-files-per-app",
      "30"
    ],
    [
      "yarn.nodemanager.log-aggregation.policy.class",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AllContainerLogAggregationPolicy"
    ],
    [
      "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds",
      "-1"
    ],
    [
      "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds.min",
      "3600"
    ],
    [
      "yarn.nodemanager.log-container-debug-info-on-error.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.log-container-debug-info.enabled",
      "true"
    ],
    [
      "yarn.nodemanager.log-dirs",
      "${yarn.log.dir}/userlogs"
    ],
    [
      "yarn.nodemanager.log.delete.threshold",
      "100g"
    ],
    [
      "yarn.nodemanager.log.deletion-threads-count",
      "4"
    ],
    [
      "yarn.nodemanager.log.retain-seconds",
      "10800"
    ],
    [
      "yarn.nodemanager.log.trigger.delete.by-size.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.logaggregation.threadpool-size-max",
      "100"
    ],
    [
      "yarn.nodemanager.node-attributes.provider.fetch-interval-ms",
      "600000"
    ],
    [
      "yarn.nodemanager.node-attributes.provider.fetch-timeout-ms",
      "1200000"
    ],
    [
      "yarn.nodemanager.node-attributes.resync-interval-ms",
      "120000"
    ],
    [
      "yarn.nodemanager.node-labels.provider.fetch-interval-ms",
      "600000"
    ],
    [
      "yarn.nodemanager.node-labels.provider.fetch-timeout-ms",
      "1200000"
    ],
    [
      "yarn.nodemanager.node-labels.resync-interval-ms",
      "120000"
    ],
    [
      "yarn.nodemanager.numa-awareness.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.numa-awareness.numactl.cmd",
      "/usr/bin/numactl"
    ],
    [
      "yarn.nodemanager.numa-awareness.read-topology",
      "false"
    ],
    [
      "yarn.nodemanager.opportunistic-containers-max-queue-length",
      "0"
    ],
    [
      "yarn.nodemanager.opportunistic-containers-queue-policy",
      "BY_QUEUE_LEN"
    ],
    [
      "yarn.nodemanager.opportunistic-containers-use-pause-for-preemption",
      "false"
    ],
    [
      "yarn.nodemanager.pluggable-device-framework.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.pmem-check-enabled",
      "true"
    ],
    [
      "yarn.nodemanager.process-kill-wait.ms",
      "5000"
    ],
    [
      "yarn.nodemanager.recovery.compaction-interval-secs",
      "3600"
    ],
    [
      "yarn.nodemanager.recovery.dir",
      "${hadoop.tmp.dir}/yarn-nm-recovery"
    ],
    [
      "yarn.nodemanager.recovery.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.recovery.supervised",
      "false"
    ],
    [
      "yarn.nodemanager.remote-app-log-dir",
      "/tmp/logs"
    ],
    [
      "yarn.nodemanager.remote-app-log-dir-include-older",
      "true"
    ],
    [
      "yarn.nodemanager.remote-app-log-dir-suffix",
      "logs"
    ],
    [
      "yarn.nodemanager.resource-monitor.interval-ms",
      "3000"
    ],
    [
      "yarn.nodemanager.resource-plugins.fpga.allowed-fpga-devices",
      "auto"
    ],
    [
      "yarn.nodemanager.resource-plugins.fpga.vendor-plugin.class",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.IntelFpgaOpenclPlugin"
    ],
    [
      "yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices",
      "auto"
    ],
    [
      "yarn.nodemanager.resource-plugins.gpu.docker-plugin",
      "nvidia-docker-v1"
    ],
    [
      "yarn.nodemanager.resource-plugins.gpu.docker-plugin.nvidia-docker-v1.endpoint",
      "http://localhost:3476/v1.0/docker/cli"
    ],
    [
      "yarn.nodemanager.resource.count-logical-processors-as-cores",
      "false"
    ],
    [
      "yarn.nodemanager.resource.cpu-vcores",
      "-1"
    ],
    [
      "yarn.nodemanager.resource.detect-hardware-capabilities",
      "false"
    ],
    [
      "yarn.nodemanager.resource.memory-mb",
      "-1"
    ],
    [
      "yarn.nodemanager.resource.memory.cgroups.soft-limit-percentage",
      "90.0"
    ],
    [
      "yarn.nodemanager.resource.memory.cgroups.swappiness",
      "0"
    ],
    [
      "yarn.nodemanager.resource.memory.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.resource.memory.enforced",
      "true"
    ],
    [
      "yarn.nodemanager.resource.pcores-vcores-multiplier",
      "1.0"
    ],
    [
      "yarn.nodemanager.resource.percentage-physical-cpu-limit",
      "100"
    ],
    [
      "yarn.nodemanager.resource.system-reserved-memory-mb",
      "-1"
    ],
    [
      "yarn.nodemanager.resourcemanager.minimum.version",
      "NONE"
    ],
    [
      "yarn.nodemanager.runtime.linux.allowed-runtimes",
      "default"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.allowed-container-networks",
      "host,none,bridge"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.allowed-container-runtimes",
      "runc"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.capabilities",
      "CHOWN,DAC_OVERRIDE,FSETID,FOWNER,MKNOD,NET_RAW,SETGID,SETUID,SETFCAP,SETPCAP,NET_BIND_SERVICE,SYS_CHROOT,KILL,AUDIT_WRITE"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.default-container-network",
      "host"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.delayed-removal.allowed",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.enable-userremapping.allowed",
      "true"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.host-pid-namespace.allowed",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.image-update",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.stop.grace-period",
      "10"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.userremapping-gid-threshold",
      "1"
    ],
    [
      "yarn.nodemanager.runtime.linux.docker.userremapping-uid-threshold",
      "1"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.allowed-container-networks",
      "host,none,bridge"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.allowed-container-runtimes",
      "runc"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.hdfs-manifest-to-resources-plugin.stat-cache-size",
      "500"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.hdfs-manifest-to-resources-plugin.stat-cache-timeout-interval-secs",
      "360"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.host-pid-namespace.allowed",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.ImageTagToManifestPlugin"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.cache-refresh-interval-secs",
      "60"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.hdfs-hash-file",
      "/runc-root/image-tag-to-hash"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.image-tag-to-manifest-plugin.num-manifests-to-cache",
      "10"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.image-toplevel-dir",
      "/runc-root"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.layer-mounts-interval-secs",
      "600"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.layer-mounts-to-keep",
      "100"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.manifest-to-resources-plugin",
      "org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc.HdfsManifestToResourcesPlugin"
    ],
    [
      "yarn.nodemanager.runtime.linux.runc.privileged-containers.allowed",
      "false"
    ],
    [
      "yarn.nodemanager.runtime.linux.sandbox-mode",
      "disabled"
    ],
    [
      "yarn.nodemanager.runtime.linux.sandbox-mode.local-dirs.permissions",
      "read"
    ],
    [
      "yarn.nodemanager.sleep-delay-before-sigkill.ms",
      "250"
    ],
    [
      "yarn.nodemanager.vmem-check-enabled",
      "true"
    ],
    [
      "yarn.nodemanager.vmem-pmem-ratio",
      "2.1"
    ],
    [
      "yarn.nodemanager.webapp.address",
      "${yarn.nodemanager.hostname}:8042"
    ],
    [
      "yarn.nodemanager.webapp.cross-origin.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.webapp.https.address",
      "0.0.0.0:8044"
    ],
    [
      "yarn.nodemanager.webapp.rest-csrf.custom-header",
      "X-XSRF-Header"
    ],
    [
      "yarn.nodemanager.webapp.rest-csrf.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.webapp.rest-csrf.methods-to-ignore",
      "GET,OPTIONS,HEAD"
    ],
    [
      "yarn.nodemanager.webapp.xfs-filter.xframe-options",
      "SAMEORIGIN"
    ],
    [
      "yarn.nodemanager.windows-container.cpu-limit.enabled",
      "false"
    ],
    [
      "yarn.nodemanager.windows-container.memory-limit.enabled",
      "false"
    ],
    [
      "yarn.registry.class",
      "org.apache.hadoop.registry.client.impl.FSRegistryOperationsService"
    ],
    [
      "yarn.resourcemanager.activities-manager.app-activities.max-queue-length",
      "100"
    ],
    [
      "yarn.resourcemanager.activities-manager.app-activities.ttl-ms",
      "600000"
    ],
    [
      "yarn.resourcemanager.activities-manager.cleanup-interval-ms",
      "5000"
    ],
    [
      "yarn.resourcemanager.activities-manager.scheduler-activities.ttl-ms",
      "600000"
    ],
    [
      "yarn.resourcemanager.address",
      "${yarn.resourcemanager.hostname}:8032"
    ],
    [
      "yarn.resourcemanager.admin.address",
      "${yarn.resourcemanager.hostname}:8033"
    ],
    [
      "yarn.resourcemanager.admin.client.thread-count",
      "1"
    ],
    [
      "yarn.resourcemanager.am-rm-tokens.master-key-rolling-interval-secs",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.am.max-attempts",
      "2"
    ],
    [
      "yarn.resourcemanager.amlauncher.thread-count",
      "50"
    ],
    [
      "yarn.resourcemanager.application-https.policy",
      "NONE"
    ],
    [
      "yarn.resourcemanager.application-tag-based-placement.enable",
      "false"
    ],
    [
      "yarn.resourcemanager.application-tag-based-placement.force-lowercase",
      "true"
    ],
    [
      "yarn.resourcemanager.application-timeouts.monitor.interval-ms",
      "3000"
    ],
    [
      "yarn.resourcemanager.application.max-tag.length",
      "100"
    ],
    [
      "yarn.resourcemanager.application.max-tags",
      "10"
    ],
    [
      "yarn.resourcemanager.auto-update.containers",
      "false"
    ],
    [
      "yarn.resourcemanager.client.thread-count",
      "50"
    ],
    [
      "yarn.resourcemanager.configuration.file-system-based-store",
      "/yarn/conf"
    ],
    [
      "yarn.resourcemanager.configuration.provider-class",
      "org.apache.hadoop.yarn.LocalConfigurationProvider"
    ],
    [
      "yarn.resourcemanager.connect.max-wait.ms",
      "900000"
    ],
    [
      "yarn.resourcemanager.connect.retry-interval.ms",
      "30000"
    ],
    [
      "yarn.resourcemanager.container-tokens.master-key-rolling-interval-secs",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.container.liveness-monitor.interval-ms",
      "600000"
    ],
    [
      "yarn.resourcemanager.decommissioning-nodes-watcher.poll-interval-secs",
      "20"
    ],
    [
      "yarn.resourcemanager.delayed.delegation-token.removal-interval-ms",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token-renewer.thread-count",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token-renewer.thread-retry-interval",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token-renewer.thread-retry-max-attempts",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token-renewer.thread-timeout",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token.always-cancel",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation-token.max-conf-size-bytes",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation.key.update-interval",
      "86400000"
    ],
    [
      "yarn.resourcemanager.delegation.token.max-lifetime",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation.token.remove-scan-interval",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.delegation.token.renew-interval",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.enable-node-untracked-without-include-path",
      "false"
    ],
    [
      "yarn.resourcemanager.epoch.range",
      "0"
    ],
    [
      "yarn.resourcemanager.fail-fast",
      "${yarn.fail-fast}"
    ],
    [
      "yarn.resourcemanager.fs.state-store.num-retries",
      "0"
    ],
    [
      "yarn.resourcemanager.fs.state-store.retry-interval-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.fs.state-store.uri",
      "${hadoop.tmp.dir}/yarn/system/rmstore"
    ],
    [
      "yarn.resourcemanager.ha.automatic-failover.embedded",
      "true"
    ],
    [
      "yarn.resourcemanager.ha.automatic-failover.enabled",
      "true"
    ],
    [
      "yarn.resourcemanager.ha.automatic-failover.zk-base-path",
      "/yarn-leader-election"
    ],
    [
      "yarn.resourcemanager.ha.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.history-writer.multi-threaded-dispatcher.pool-size",
      "10"
    ],
    [
      "yarn.resourcemanager.hostname",
      "0.0.0.0"
    ],
    [
      "yarn.resourcemanager.keytab",
      "/etc/krb5.keytab"
    ],
    [
      "yarn.resourcemanager.leveldb-state-store.compaction-interval-secs",
      "3600"
    ],
    [
      "yarn.resourcemanager.leveldb-state-store.path",
      "${hadoop.tmp.dir}/yarn/system/rmstore"
    ],
    [
      "yarn.resourcemanager.max-completed-applications",
      "1000"
    ],
    [
      "yarn.resourcemanager.max-log-aggregation-diagnostics-in-memory",
      "10"
    ],
    [
      "yarn.resourcemanager.metrics.runtime.buckets",
      "60,300,1440"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.load-comparator",
      "QUEUE_LENGTH"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.max-queue-length",
      "15"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.max-queue-wait-time-ms",
      "100"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.min-queue-length",
      "5"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.min-queue-wait-time-ms",
      "10"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.queue-limit-stdev",
      "1.0f"
    ],
    [
      "yarn.resourcemanager.nm-container-queuing.sorting-nodes-interval-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.nm-tokens.master-key-rolling-interval-secs",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.node-ip-cache.expiry-interval-secs",
      "-1"
    ],
    [
      "yarn.resourcemanager.node-labels.am.allow-non-exclusive-allocation",
      "false"
    ],
    [
      "yarn.resourcemanager.node-labels.provider.fetch-interval-ms",
      "1800000"
    ],
    [
      "yarn.resourcemanager.node-labels.provider.update-newly-registered-nodes-interval-ms",
      "30000"
    ],
    [
      "yarn.resourcemanager.node-removal-untracked.timeout-ms",
      "60000"
    ],
    [
      "yarn.resourcemanager.nodemanager-connect-retries",
      "10"
    ],
    [
      "yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs",
      "3600"
    ],
    [
      "yarn.resourcemanager.nodemanager.minimum.version",
      "NONE"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-max-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-min-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-scaling-enable",
      "false"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-slowdown-factor",
      "1.0"
    ],
    [
      "yarn.resourcemanager.nodemanagers.heartbeat-interval-speedup-factor",
      "1.0"
    ],
    [
      "yarn.resourcemanager.nodestore-rootdir.num-retries",
      "1000"
    ],
    [
      "yarn.resourcemanager.nodestore-rootdir.retry-interval-ms",
      "1000"
    ],
    [
      "yarn.resourcemanager.opportunistic-container-allocation.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.opportunistic-container-allocation.nodes-used",
      "10"
    ],
    [
      "yarn.resourcemanager.opportunistic.max.container-allocation.per.am.heartbeat",
      "-1"
    ],
    [
      "yarn.resourcemanager.placement-constraints.algorithm.class",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm"
    ],
    [
      "yarn.resourcemanager.placement-constraints.algorithm.iterator",
      "SERIAL"
    ],
    [
      "yarn.resourcemanager.placement-constraints.algorithm.pool-size",
      "1"
    ],
    [
      "yarn.resourcemanager.placement-constraints.handler",
      "disabled"
    ],
    [
      "yarn.resourcemanager.placement-constraints.retry-attempts",
      "3"
    ],
    [
      "yarn.resourcemanager.placement-constraints.scheduler.pool-size",
      "1"
    ],
    [
      "yarn.resourcemanager.proxy-user-privileges.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.proxy.connection.timeout",
      "60000"
    ],
    [
      "yarn.resourcemanager.proxy.timeout.enabled",
      "true"
    ],
    [
      "yarn.resourcemanager.recovery.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.reservation-system.enable",
      "false"
    ],
    [
      "yarn.resourcemanager.reservation-system.planfollower.time-step",
      "1000"
    ],
    [
      "yarn.resourcemanager.resource-profiles.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.resource-profiles.source-file",
      "resource-profiles.json"
    ],
    [
      "yarn.resourcemanager.resource-tracker.address",
      "${yarn.resourcemanager.hostname}:8031"
    ],
    [
      "yarn.resourcemanager.resource-tracker.client.thread-count",
      "50"
    ],
    [
      "yarn.resourcemanager.resource-tracker.nm.ip-hostname-check",
      "false"
    ],
    [
      "yarn.resourcemanager.rm.container-allocation.expiry-interval-ms",
      "600000"
    ],
    [
      "yarn.resourcemanager.scheduler.address",
      "${yarn.resourcemanager.hostname}:8030"
    ],
    [
      "yarn.resourcemanager.scheduler.class",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"
    ],
    [
      "yarn.resourcemanager.scheduler.client.thread-count",
      "50"
    ],
    [
      "yarn.resourcemanager.scheduler.monitor.enable",
      "false"
    ],
    [
      "yarn.resourcemanager.scheduler.monitor.policies",
      "org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy"
    ],
    [
      "yarn.resourcemanager.state-store.max-completed-applications",
      "${yarn.resourcemanager.max-completed-applications}"
    ],
    [
      "yarn.resourcemanager.store.class",
      "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore"
    ],
    [
      "yarn.resourcemanager.submission-preprocessor.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.submission-preprocessor.file-refresh-interval-ms",
      "60000"
    ],
    [
      "yarn.resourcemanager.system-metrics-publisher.dispatcher.pool-size",
      "10"
    ],
    [
      "yarn.resourcemanager.system-metrics-publisher.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.batch-size",
      "1000"
    ],
    [
      "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.enable-batch",
      "false"
    ],
    [
      "yarn.resourcemanager.system-metrics-publisher.timeline-server-v1.interval-seconds",
      "60"
    ],
    [
      "yarn.resourcemanager.webapp.address",
      "${yarn.resourcemanager.hostname}:8088"
    ],
    [
      "yarn.resourcemanager.webapp.cross-origin.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.webapp.https.address",
      "${yarn.resourcemanager.hostname}:8090"
    ],
    [
      "yarn.resourcemanager.webapp.rest-csrf.custom-header",
      "X-XSRF-Header"
    ],
    [
      "yarn.resourcemanager.webapp.rest-csrf.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.webapp.rest-csrf.methods-to-ignore",
      "GET,OPTIONS,HEAD"
    ],
    [
      "yarn.resourcemanager.webapp.ui-actions.enabled",
      "true"
    ],
    [
      "yarn.resourcemanager.webapp.xfs-filter.xframe-options",
      "SAMEORIGIN"
    ],
    [
      "yarn.resourcemanager.work-preserving-recovery.enabled",
      "true"
    ],
    [
      "yarn.resourcemanager.work-preserving-recovery.scheduling-wait-ms",
      "10000"
    ],
    [
      "yarn.resourcemanager.zk-appid-node.split-index",
      "0"
    ],
    [
      "yarn.resourcemanager.zk-client-ssl.enabled",
      "false"
    ],
    [
      "yarn.resourcemanager.zk-delegation-token-node.split-index",
      "*********(redacted)"
    ],
    [
      "yarn.resourcemanager.zk-max-znode-size.bytes",
      "1048576"
    ],
    [
      "yarn.resourcemanager.zk-state-store.parent-path",
      "/rmstore"
    ],
    [
      "yarn.rm.system-metrics-publisher.emit-container-events",
      "false"
    ],
    [
      "yarn.router.asc-interceptor-max-size",
      "1MB"
    ],
    [
      "yarn.router.clientrm.interceptor-class.pipeline",
      "org.apache.hadoop.yarn.server.router.clientrm.DefaultClientRequestInterceptor"
    ],
    [
      "yarn.router.deregister.subcluster.enabled",
      "true"
    ],
    [
      "yarn.router.interceptor.allow-partial-result.enable",
      "false"
    ],
    [
      "yarn.router.interceptor.user-thread-pool.allow-core-thread-time-out",
      "false"
    ],
    [
      "yarn.router.interceptor.user-thread-pool.keep-alive-time",
      "30s"
    ],
    [
      "yarn.router.interceptor.user-thread-pool.maximum-pool-size",
      "5"
    ],
    [
      "yarn.router.interceptor.user-thread-pool.minimum-pool-size",
      "5"
    ],
    [
      "yarn.router.interceptor.user.threadpool-size",
      "5"
    ],
    [
      "yarn.router.pipeline.cache-max-size",
      "25"
    ],
    [
      "yarn.router.rmadmin.interceptor-class.pipeline",
      "org.apache.hadoop.yarn.server.router.rmadmin.DefaultRMAdminRequestInterceptor"
    ],
    [
      "yarn.router.scheduled.executor.threads",
      "1"
    ],
    [
      "yarn.router.subcluster.cleaner.interval.time",
      "60s"
    ],
    [
      "yarn.router.subcluster.heartbeat.expiration.time",
      "30m"
    ],
    [
      "yarn.router.submit.interval.time",
      "10ms"
    ],
    [
      "yarn.router.webapp.address",
      "0.0.0.0:8089"
    ],
    [
      "yarn.router.webapp.appsinfo-cached-count",
      "100"
    ],
    [
      "yarn.router.webapp.appsinfo-enabled",
      "false"
    ],
    [
      "yarn.router.webapp.cross-origin.enabled",
      "false"
    ],
    [
      "yarn.router.webapp.https.address",
      "0.0.0.0:8091"
    ],
    [
      "yarn.router.webapp.interceptor-class.pipeline",
      "org.apache.hadoop.yarn.server.router.webapp.DefaultRequestInterceptorREST"
    ],
    [
      "yarn.router.webapp.proxy.enable",
      "true"
    ],
    [
      "yarn.scheduler.configuration.fs.path",
      "file://${hadoop.tmp.dir}/yarn/system/schedconf"
    ],
    [
      "yarn.scheduler.configuration.leveldb-store.compaction-interval-secs",
      "86400"
    ],
    [
      "yarn.scheduler.configuration.leveldb-store.path",
      "${hadoop.tmp.dir}/yarn/system/confstore"
    ],
    [
      "yarn.scheduler.configuration.max.version",
      "100"
    ],
    [
      "yarn.scheduler.configuration.mutation.acl-policy.class",
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.DefaultConfigurationMutationACLPolicy"
    ],
    [
      "yarn.scheduler.configuration.store.class",
      "file"
    ],
    [
      "yarn.scheduler.configuration.store.max-logs",
      "1000"
    ],
    [
      "yarn.scheduler.configuration.zk-store.parent-path",
      "/confstore"
    ],
    [
      "yarn.scheduler.include-port-in-node-name",
      "false"
    ],
    [
      "yarn.scheduler.maximum-allocation-mb",
      "8192"
    ],
    [
      "yarn.scheduler.maximum-allocation-vcores",
      "4"
    ],
    [
      "yarn.scheduler.minimum-allocation-mb",
      "1024"
    ],
    [
      "yarn.scheduler.minimum-allocation-vcores",
      "1"
    ],
    [
      "yarn.scheduler.queue-placement-rules",
      "user-group"
    ],
    [
      "yarn.scheduler.skip.node.multiplier",
      "2"
    ],
    [
      "yarn.sharedcache.admin.address",
      "0.0.0.0:8047"
    ],
    [
      "yarn.sharedcache.admin.thread-count",
      "1"
    ],
    [
      "yarn.sharedcache.app-checker.class",
      "org.apache.hadoop.yarn.server.sharedcachemanager.RemoteAppChecker"
    ],
    [
      "yarn.sharedcache.checksum.algo.impl",
      "org.apache.hadoop.yarn.sharedcache.ChecksumSHA256Impl"
    ],
    [
      "yarn.sharedcache.cleaner.initial-delay-mins",
      "10"
    ],
    [
      "yarn.sharedcache.cleaner.period-mins",
      "1440"
    ],
    [
      "yarn.sharedcache.cleaner.resource-sleep-ms",
      "0"
    ],
    [
      "yarn.sharedcache.client-server.address",
      "0.0.0.0:8045"
    ],
    [
      "yarn.sharedcache.client-server.thread-count",
      "50"
    ],
    [
      "yarn.sharedcache.enabled",
      "false"
    ],
    [
      "yarn.sharedcache.nested-level",
      "3"
    ],
    [
      "yarn.sharedcache.nm.uploader.replication.factor",
      "10"
    ],
    [
      "yarn.sharedcache.nm.uploader.thread-count",
      "20"
    ],
    [
      "yarn.sharedcache.root-dir",
      "/sharedcache"
    ],
    [
      "yarn.sharedcache.store.class",
      "org.apache.hadoop.yarn.server.sharedcachemanager.store.InMemorySCMStore"
    ],
    [
      "yarn.sharedcache.store.in-memory.check-period-mins",
      "720"
    ],
    [
      "yarn.sharedcache.store.in-memory.initial-delay-mins",
      "10"
    ],
    [
      "yarn.sharedcache.store.in-memory.staleness-period-mins",
      "10080"
    ],
    [
      "yarn.sharedcache.uploader.server.address",
      "0.0.0.0:8046"
    ],
    [
      "yarn.sharedcache.uploader.server.thread-count",
      "50"
    ],
    [
      "yarn.sharedcache.webapp.address",
      "0.0.0.0:8788"
    ],
    [
      "yarn.system-metrics-publisher.enabled",
      "false"
    ],
    [
      "yarn.timeline-service.address",
      "${yarn.timeline-service.hostname}:10200"
    ],
    [
      "yarn.timeline-service.app-aggregation-interval-secs",
      "15"
    ],
    [
      "yarn.timeline-service.app-collector.linger-period.ms",
      "60000"
    ],
    [
      "yarn.timeline-service.client.best-effort",
      "false"
    ],
    [
      "yarn.timeline-service.client.drain-entities.timeout.ms",
      "2000"
    ],
    [
      "yarn.timeline-service.client.fd-clean-interval-secs",
      "60"
    ],
    [
      "yarn.timeline-service.client.fd-flush-interval-secs",
      "10"
    ],
    [
      "yarn.timeline-service.client.fd-retain-secs",
      "300"
    ],
    [
      "yarn.timeline-service.client.internal-timers-ttl-secs",
      "420"
    ],
    [
      "yarn.timeline-service.client.max-retries",
      "30"
    ],
    [
      "yarn.timeline-service.client.retry-interval-ms",
      "1000"
    ],
    [
      "yarn.timeline-service.enabled",
      "false"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.active-dir",
      "/tmp/entity-file-history/active"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.app-cache-size",
      "10"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.cache-store-class",
      "org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.cleaner-interval-seconds",
      "3600"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.done-dir",
      "/tmp/entity-file-history/done/"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.leveldb-cache-read-cache-size",
      "10485760"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.retain-seconds",
      "604800"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.scan-interval-seconds",
      "60"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.summary-store",
      "org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore"
    ],
    [
      "yarn.timeline-service.entity-group-fs-store.with-user-dir",
      "false"
    ],
    [
      "yarn.timeline-service.flowname.max-size",
      "0"
    ],
    [
      "yarn.timeline-service.generic-application-history.max-applications",
      "10000"
    ],
    [
      "yarn.timeline-service.handler-thread-count",
      "10"
    ],
    [
      "yarn.timeline-service.hbase-schema.prefix",
      "prod."
    ],
    [
      "yarn.timeline-service.hbase.coprocessor.app-final-value-retention-milliseconds",
      "259200000"
    ],
    [
      "yarn.timeline-service.hbase.coprocessor.jar.hdfs.location",
      "/hbase/coprocessor/hadoop-yarn-server-timelineservice.jar"
    ],
    [
      "yarn.timeline-service.hostname",
      "0.0.0.0"
    ],
    [
      "yarn.timeline-service.http-authentication.simple.anonymous.allowed",
      "true"
    ],
    [
      "yarn.timeline-service.http-authentication.type",
      "simple"
    ],
    [
      "yarn.timeline-service.http-cross-origin.enabled",
      "false"
    ],
    [
      "yarn.timeline-service.keytab",
      "/etc/krb5.keytab"
    ],
    [
      "yarn.timeline-service.leveldb-state-store.path",
      "${hadoop.tmp.dir}/yarn/timeline"
    ],
    [
      "yarn.timeline-service.leveldb-timeline-store.path",
      "${hadoop.tmp.dir}/yarn/timeline"
    ],
    [
      "yarn.timeline-service.leveldb-timeline-store.read-cache-size",
      "104857600"
    ],
    [
      "yarn.timeline-service.leveldb-timeline-store.start-time-read-cache-size",
      "10000"
    ],
    [
      "yarn.timeline-service.leveldb-timeline-store.start-time-write-cache-size",
      "10000"
    ],
    [
      "yarn.timeline-service.leveldb-timeline-store.ttl-interval-ms",
      "300000"
    ],
    [
      "yarn.timeline-service.reader.class",
      "org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineReaderImpl"
    ],
    [
      "yarn.timeline-service.reader.webapp.address",
      "${yarn.timeline-service.webapp.address}"
    ],
    [
      "yarn.timeline-service.reader.webapp.https.address",
      "${yarn.timeline-service.webapp.https.address}"
    ],
    [
      "yarn.timeline-service.recovery.enabled",
      "false"
    ],
    [
      "yarn.timeline-service.state-store-class",
      "org.apache.hadoop.yarn.server.timeline.recovery.LeveldbTimelineStateStore"
    ],
    [
      "yarn.timeline-service.store-class",
      "org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore"
    ],
    [
      "yarn.timeline-service.timeline-client.number-of-async-entities-to-merge",
      "10"
    ],
    [
      "yarn.timeline-service.ttl-enable",
      "true"
    ],
    [
      "yarn.timeline-service.ttl-ms",
      "604800000"
    ],
    [
      "yarn.timeline-service.version",
      "1.0f"
    ],
    [
      "yarn.timeline-service.webapp.address",
      "${yarn.timeline-service.hostname}:8188"
    ],
    [
      "yarn.timeline-service.webapp.https.address",
      "${yarn.timeline-service.hostname}:8190"
    ],
    [
      "yarn.timeline-service.webapp.rest-csrf.custom-header",
      "X-XSRF-Header"
    ],
    [
      "yarn.timeline-service.webapp.rest-csrf.enabled",
      "false"
    ],
    [
      "yarn.timeline-service.webapp.rest-csrf.methods-to-ignore",
      "GET,OPTIONS,HEAD"
    ],
    [
      "yarn.timeline-service.webapp.xfs-filter.xframe-options",
      "SAMEORIGIN"
    ],
    [
      "yarn.timeline-service.writer.async.queue.capacity",
      "100"
    ],
    [
      "yarn.timeline-service.writer.class",
      "org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl"
    ],
    [
      "yarn.timeline-service.writer.flush-interval-seconds",
      "60"
    ],
    [
      "yarn.webapp.api-service.enable",
      "false"
    ],
    [
      "yarn.webapp.enable-rest-app-submissions",
      "true"
    ],
    [
      "yarn.webapp.filter-entity-list-by-user",
      "false"
    ],
    [
      "yarn.webapp.filter-invalid-xml-chars",
      "false"
    ],
    [
      "yarn.webapp.ui1.tools.enable",
      "true"
    ],
    [
      "yarn.webapp.ui2.enable",
      "false"
    ],
    [
      "yarn.webapp.xfs-filter.enabled",
      "true"
    ],
    [
      "yarn.workflow-id.tag-prefix",
      "workflowid:"
    ]
  ],
  "system_properties": [
    [
      "SPARK_SUBMIT",
      "true"
    ],
    [
      "file.encoding",
      "UTF-8"
    ],
    [
      "file.separator",
      "/"
    ],
    [
      "java.class.version",
      "61.0"
    ],
    [
      "java.home",
      "/opt/java/openjdk"
    ],
    [
      "java.io.tmpdir",
      "/tmp"
    ],
    [
      "java.library.path",
      "/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib"
    ],
    [
      "java.runtime.name",
      "OpenJDK Runtime Environment"
    ],
    [
      "java.runtime.version",
      "17.0.12+7"
    ],
    [
      "java.specification.name",
      "Java Platform API Specification"
    ],
    [
      "java.specification.vendor",
      "Oracle Corporation"
    ],
    [
      "java.specification.version",
      "17"
    ],
    [
      "java.vendor",
      "Eclipse Adoptium"
    ],
    [
      "java.vendor.url",
      "https://adoptium.net/"
    ],
    [
      "java.vendor.url.bug",
      "https://github.com/adoptium/adoptium-support/issues"
    ],
    [
      "java.vendor.version",
      "Temurin-17.0.12+7"
    ],
    [
      "java.version",
      "17.0.12"
    ],
    [
      "java.version.date",
      "2024-07-16"
    ],
    [
      "java.vm.compressedOopsMode",
      "32-bit"
    ],
    [
      "java.vm.info",
      "mixed mode, sharing"
    ],
    [
      "java.vm.name",
      "OpenJDK 64-Bit Server VM"
    ],
    [
      "java.vm.specification.name",
      "Java Virtual Machine Specification"
    ],
    [
      "java.vm.specification.vendor",
      "Oracle Corporation"
    ],
    [
      "java.vm.specification.version",
      "17"
    ],
    [
      "java.vm.vendor",
      "Eclipse Adoptium"
    ],
    [
      "java.vm.version",
      "17.0.12+7"
    ],
    [
      "jdk.debug",
      "release"
    ],
    [
      "jdk.reflect.useDirectMethodHandle",
      "false"
    ],
    [
      "jetty.git.hash",
      "cef3fbd6d736a21e7d541a5db490381d95a2047d"
    ],
    [
      "kubernetes.request.retry.backoffLimit",
      "3"
    ],
    [
      "line.separator",
      "\n"
    ],
    [
      "native.encoding",
      "UTF-8"
    ],
    [
      "os.arch",
      "amd64"
    ],
    [
      "os.name",
      "Linux"
    ],
    [
      "os.version",
      "6.1.134-150.224.amzn2023.x86_64"
    ],
    [
      "path.separator",
      ":"
    ],
    [
      "sun.arch.data.model",
      "64"
    ],
    [
      "sun.boot.library.path",
      "/opt/java/openjdk/lib"
    ],
    [
      "sun.cpu.endian",
      "little"
    ],
    [
      "sun.io.unicode.encoding",
      "UnicodeLittle"
    ],
    [
      "sun.java.command",
      "org.apache.spark.deploy.SparkSubmit --deploy-mode client --conf spark.driver.bindAddress=100.64.10.2 --conf spark.executorEnv.SPARK_DRIVER_POD_IP=100.64.10.2 --properties-file /opt/spark/conf/spark.properties --class org.apache.spark.deploy.PythonRunner local:///opt/spark/examples/src/main/python/pi.py"
    ],
    [
      "sun.java.launcher",
      "SUN_STANDARD"
    ],
    [
      "sun.jnu.encoding",
      "UTF-8"
    ],
    [
      "sun.management.compiler",
      "HotSpot 64-Bit Tiered Compilers"
    ],
    [
      "user.country",
      "US"
    ],
    [
      "user.dir",
      "/opt/spark"
    ],
    [
      "user.home",
      "/home/spark"
    ],
    [
      "user.language",
      "en"
    ],
    [
      "user.name",
      "spark"
    ],
    [
      "user.timezone",
      "Etc/UTC"
    ]
  ],
  "metrics_properties": [
    [
      "*.sink.servlet.class",
      "org.apache.spark.metrics.sink.MetricsServlet"
    ],
    [
      "*.sink.servlet.path",
      "/metrics/json"
    ],
    [
      "applications.sink.servlet.path",
      "/metrics/applications/json"
    ],
    [
      "master.sink.servlet.path",
      "/metrics/master/json"
    ]
  ],
  "classpath_entries": [
    [
      "/opt/spark/conf/",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/HikariCP-2.5.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/JLargeArrays-1.5.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/JTransforms-3.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/RoaringBitmap-0.9.45.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/ST4-4.0.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/activation-1.1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/aircompressor-0.27.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/algebra_2.12-2.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/annotations-17.0.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/antlr-runtime-3.5.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/antlr4-runtime-4.9.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/aopalliance-repackaged-2.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arpack-3.0.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arpack_combined_all-0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arrow-format-12.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arrow-memory-core-12.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arrow-memory-netty-12.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/arrow-vector-12.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/audience-annotations-0.5.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/avro-1.11.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/avro-ipc-1.11.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/avro-mapred-1.11.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/blas-3.0.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/bonecp-0.8.0.RELEASE.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/breeze-macros_2.12-2.1.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/breeze_2.12-2.1.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/bundle-2.29.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/cats-kernel_2.12-2.1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/chill-java-0.10.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/chill_2.12-0.10.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-cli-1.5.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-codec-1.16.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-collections-3.2.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-collections4-4.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-compiler-3.1.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-compress-1.23.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-crypto-1.1.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-dbcp-1.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-io-2.16.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-lang-2.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-lang3-3.12.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-logging-1.1.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-math3-3.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-pool-1.5.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/commons-text-1.10.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/compress-lzf-1.1.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/curator-client-2.13.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/curator-framework-2.13.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/curator-recipes-2.13.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/datanucleus-api-jdo-4.2.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/datanucleus-core-4.1.17.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/datanucleus-rdbms-4.1.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/datasketches-java-3.3.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/datasketches-memory-2.1.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/derby-10.14.2.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/dropwizard-metrics-hadoop-metrics2-reporter-0.1.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/flatbuffers-java-1.12.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/gson-2.2.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/guava-14.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-aws-3.4.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-client-api-3.4.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-client-runtime-3.4.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-common-3.4.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-shaded-guava-1.1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hadoop-yarn-server-web-proxy-3.4.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-beeline-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-cli-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-common-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-exec-2.3.9-core.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-jdbc-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-llap-common-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-metastore-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-serde-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-service-rpc-3.1.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-shims-0.23-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-shims-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-shims-common-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-shims-scheduler-2.3.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hive-storage-api-2.8.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hk2-api-2.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hk2-locator-2.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/hk2-utils-2.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/httpclient-4.5.14.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/httpcore-4.4.16.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/istack-commons-runtime-3.0.8.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/ivy-2.5.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-annotations-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-core-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-core-asl-1.9.13.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-databind-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-dataformat-yaml-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-datatype-jsr310-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-mapper-asl-1.9.13.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jackson-module-scala_2.12-2.15.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.annotation-api-1.3.5.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.inject-2.6.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.servlet-api-4.0.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.validation-api-2.0.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.ws.rs-api-2.1.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jakarta.xml.bind-api-2.3.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/janino-3.1.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/javassist-3.29.2-GA.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/javax.jdo-3.2.0-m3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/javolution-5.5.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jaxb-runtime-2.3.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jcl-over-slf4j-2.0.7.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jdo-api-3.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-client-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-common-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-container-servlet-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-container-servlet-core-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-hk2-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jersey-server-2.40.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jline-2.14.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/joda-time-2.12.5.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jodd-core-3.5.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jpam-1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/json-1.8.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/json4s-ast_2.12-3.7.0-M11.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/json4s-core_2.12-3.7.0-M11.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/json4s-jackson_2.12-3.7.0-M11.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/json4s-scalap_2.12-3.7.0-M11.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jsr305-3.0.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jta-1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/jul-to-slf4j-2.0.7.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kryo-shaded-4.0.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-client-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-client-api-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-httpclient-okhttp-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-admissionregistration-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-apiextensions-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-apps-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-autoscaling-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-batch-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-certificates-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-common-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-coordination-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-core-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-discovery-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-events-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-extensions-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-flowcontrol-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-gatewayapi-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-metrics-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-networking-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-node-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-policy-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-rbac-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-resource-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-scheduling-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/kubernetes-model-storageclass-6.7.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/lapack-3.0.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/leveldbjni-all-1.8.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/libfb303-0.9.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/libthrift-0.12.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/log4j-1.2-api-2.20.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/log4j-api-2.20.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/log4j-core-2.20.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/log4j-slf4j2-impl-2.20.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/logging-interceptor-3.12.12.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/lz4-java-1.8.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/mesos-1.4.3-shaded-protobuf.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/metrics-core-4.2.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/metrics-graphite-4.2.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/metrics-jmx-4.2.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/metrics-json-4.2.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/metrics-jvm-4.2.19.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/minlog-1.3.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-all-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-buffer-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-codec-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-codec-http-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-codec-http2-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-codec-socks-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-common-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-handler-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-handler-proxy-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-resolver-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-classes-epoll-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-classes-kqueue-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-native-epoll-4.1.96.Final-linux-aarch_64.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-native-epoll-4.1.96.Final-linux-x86_64.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-native-kqueue-4.1.96.Final-osx-aarch_64.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-native-kqueue-4.1.96.Final-osx-x86_64.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/netty-transport-native-unix-common-4.1.96.Final.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/objenesis-3.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/okhttp-3.12.12.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/okio-1.17.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/opencsv-2.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/orc-core-1.9.4-shaded-protobuf.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/orc-mapreduce-1.9.4-shaded-protobuf.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/orc-shims-1.9.4.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/oro-2.0.8.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/osgi-resource-locator-1.0.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/paranamer-2.8.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-column-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-common-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-encoding-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-format-structures-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-hadoop-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/parquet-jackson-1.13.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/pickle-1.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/py4j-0.10.9.7.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/rocksdbjni-8.3.2.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-collection-compat_2.12-2.7.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-compiler-2.12.18.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-library-2.12.18.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-parser-combinators_2.12-2.3.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-reflect-2.12.18.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/scala-xml_2.12-2.1.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/shims-0.9.45.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/slf4j-api-2.0.7.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/snakeyaml-2.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/snakeyaml-engine-2.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/snappy-java-1.1.10.5.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-catalyst_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-common-utils_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-core_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-graphx_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-hive-thriftserver_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-hive_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-kubernetes_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-kvstore_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-launcher_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-mesos_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-mllib-local_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-mllib_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-network-common_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-network-shuffle_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-repl_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-sketch_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-sql-api_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-sql_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-streaming_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-tags_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-unsafe_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spark-yarn_2.12-3.5.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spire-macros_2.12-0.17.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spire-platform_2.12-0.17.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spire-util_2.12-0.17.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/spire_2.12-0.17.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/stax-api-1.0.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/stream-2.9.6.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/super-csv-2.2.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/threeten-extra-1.7.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/tink-1.9.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/transaction-api-1.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/univocity-parsers-2.9.1.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/xbean-asm9-shaded-4.23.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/xz-1.9.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/zjsonpatch-0.3.0.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/zookeeper-3.6.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/zookeeper-jute-3.6.3.jar",
      "System Classpath"
    ],
    [
      "/opt/spark/jars/zstd-jni-1.5.5-4.jar",
      "System Classpath"
    ]
  ],
  "resource_profiles": [
    {
      "id": 0,
      "executor_resources": {
        "cores": {
          "resourceName": "cores",
          "amount": 1,
          "discoveryScript": "",
          "vendor": ""
        },
        "memory": {
          "resourceName": "memory",
          "amount": 512,
          "discoveryScript": "",
          "vendor": ""
        },
        "offHeap": {
          "resourceName": "offHeap",
          "amount": 0,
          "discoveryScript": "",
          "vendor": ""
        }
      },
      "task_resources": {
        "cpus": {
          "resourceName": "cpus",
          "amount": 1.0
        }
      }
    }
  ]
}
```

### `get_application_insights`
Response format:
- insights, bottlenecks, recommendations
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "application_name": "PythonPi",
  "analysis_timestamp": "2026-02-18T22:55:31.685169",
  "analysis_type": "Comprehensive SparkInsight Analysis",
  "analyses": {
    "auto_scaling": {
      "application_id": "spark-bcec39f6201b42b9925124595baad260",
      "analysis_type": "Auto-scaling Configuration",
      "target_stage_duration_minutes": 2,
      "recommendations": {
        "initial_executors": {
          "type": "auto_scaling",
          "priority": "medium",
          "issue": "Initial executors could be optimized (current: Not set)",
          "suggestion": "Set spark.dynamicAllocation.initialExecutors to 2",
          "configuration": {
            "parameter": "spark.dynamicAllocation.initialExecutors",
            "current_value": "Not set",
            "recommended_value": "2",
            "description": "Based on stages running in first 2 minutes"
          }
        },
        "max_executors": {
          "type": "auto_scaling",
          "priority": "medium",
          "issue": "Max executors could be optimized (current: Not set)",
          "suggestion": "Set spark.dynamicAllocation.maxExecutors to 2",
          "configuration": {
            "parameter": "spark.dynamicAllocation.maxExecutors",
            "current_value": "Not set",
            "recommended_value": "2",
            "description": "Based on peak concurrent stage demand"
          }
        }
      },
      "analysis_details": {
        "total_stages": 1,
        "initial_stages_analyzed": 0,
        "peak_concurrent_demand": 2,
        "calculation_method": "Aims to complete stages in 2 minutes",
        "configuration_analysis": {
          "initial_executors": {
            "current": "Not set",
            "recommended": "2",
            "description": "Based on stages running in first 2 minutes"
          },
          "max_executors": {
            "current": "Not set",
            "recommended": "2",
            "description": "Based on peak concurrent stage demand"
          }
        }
      }
    },
    "shuffle_skew": {
      "application_id": "spark-bcec39f6201b42b9925124595baad260",
      "analysis_type": "Shuffle Skew Analysis",
      "parameters": {
        "shuffle_threshold_gb": 10,
        "skew_ratio_threshold": 2.0
      },
      "skewed_stages": [],
      "summary": {
        "total_stages_analyzed": 0,
        "skewed_stages_count": 0,
        "task_skewed_count": 0,
        "executor_skewed_count": 0,
        "max_skew_ratio": 0
      },
      "recommendations": []
    },
    "failed_tasks": {
      "application_id": "spark-bcec39f6201b42b9925124595baad260",
      "analysis_type": "Failed Task Analysis",
      "parameters": {
        "failure_threshold": 1
      },
      "failed_stages": [],
      "problematic_executors": [],
      "summary": {
        "total_failed_tasks": 0,
        "stages_with_failures": 0,
        "executors_with_failures": 0,
        "overall_failure_impact": "low"
      },
      "recommendations": []
    }
  },
  "summary": {
    "total_analyses_run": 3,
    "total_recommendations": 2,
    "critical_issues": 0,
    "high_priority_recommendations": 0,
    "overall_health": "good"
  },
  "recommendations": [
    {
      "type": "auto_scaling",
      "priority": "medium",
      "issue": "Initial executors could be optimized (current: Not set)",
      "suggestion": "Set spark.dynamicAllocation.initialExecutors to 2",
      "configuration": {
        "parameter": "spark.dynamicAllocation.initialExecutors",
        "current_value": "Not set",
        "recommended_value": "2",
        "description": "Based on stages running in first 2 minutes"
      },
      "source_analysis": "auto_scaling",
      "recommendation_type": "initial_executors"
    },
    {
      "type": "auto_scaling",
      "priority": "medium",
      "issue": "Max executors could be optimized (current: Not set)",
      "suggestion": "Set spark.dynamicAllocation.maxExecutors to 2",
      "configuration": {
        "parameter": "spark.dynamicAllocation.maxExecutors",
        "current_value": "Not set",
        "recommended_value": "2",
        "description": "Based on peak concurrent stage demand"
      },
      "source_analysis": "auto_scaling",
      "recommendation_type": "max_executors"
    }
  ]
}
```

### `get_app_summary`
Response format:
- compact app summary metrics
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "application_name": "PythonPi",
  "analysis_timestamp": "2026-02-18T22:55:31.685605",
  "application_duration_minutes": 0.12,
  "total_executor_runtime_minutes": 0.02,
  "executor_cpu_time_minutes": 0.0,
  "jvm_gc_time_minutes": 0.0,
  "executor_utilization_percent": 12.2,
  "input_data_size_gb": 0.0,
  "output_data_size_gb": 0.0,
  "shuffle_read_size_gb": 0.0,
  "shuffle_write_size_gb": 0.0,
  "memory_spilled_gb": 0.0,
  "disk_spilled_gb": 0.0,
  "shuffle_read_wait_time_minutes": 0.0,
  "shuffle_write_time_minutes": 0.0,
  "failed_tasks": 0,
  "total_stages": 1,
  "completed_stages": 1,
  "failed_stages": 0
}
```

Jobs and Stages
---------------

### `list_jobs`
Response format:
- list of job summaries
Sample (full):
```json
[
  {
    "job_id": 0,
    "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
    "description": null,
    "submission_time": "2025-06-26T00:06:57.033000Z",
    "completion_time": "2025-06-26T00:06:58.574000Z",
    "stage_ids": [
      0
    ],
    "job_group": null,
    "job_tags": [],
    "status": "SUCCEEDED",
    "num_tasks": 2,
    "num_active_tasks": 0,
    "num_completed_tasks": 2,
    "num_skipped_tasks": 0,
    "num_failed_tasks": 0,
    "num_killed_tasks": 0,
    "num_completed_indices": 2,
    "num_active_stages": 0,
    "num_completed_stages": 1,
    "num_skipped_stages": 0,
    "num_failed_stages": 0,
    "killed_tasks_summary": {}
  }
]
```

### `list_slowest_jobs`
Response format:
- list of slowest jobs with durations
Sample (full):
```json
[
  {
    "job_id": 0,
    "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
    "description": null,
    "submission_time": "2025-06-26T00:06:57.033000Z",
    "completion_time": "2025-06-26T00:06:58.574000Z",
    "stage_ids": [
      0
    ],
    "job_group": null,
    "job_tags": [],
    "status": "SUCCEEDED",
    "num_tasks": 2,
    "num_active_tasks": 0,
    "num_completed_tasks": 2,
    "num_skipped_tasks": 0,
    "num_failed_tasks": 0,
    "num_killed_tasks": 0,
    "num_completed_indices": 2,
    "num_active_stages": 0,
    "num_completed_stages": 1,
    "num_skipped_stages": 0,
    "num_failed_stages": 0,
    "killed_tasks_summary": {}
  }
]
```

### `list_stages`
Response format:
- list of stage summaries
Sample (full):
```json
[
  {
    "status": "COMPLETE",
    "stage_id": 0,
    "attempt_id": 0,
    "num_tasks": 2,
    "num_active_tasks": 0,
    "num_complete_tasks": 2,
    "num_failed_tasks": 0,
    "num_killed_tasks": 0,
    "num_completed_indices": 2,
    "submission_time": "2025-06-26T00:06:57.054000Z",
    "first_task_launched_time": "2025-06-26T00:06:57.189000Z",
    "completion_time": "2025-06-26T00:06:58.569000Z",
    "failure_reason": null,
    "executor_deserialize_time": 383,
    "executor_deserialize_cpu_time": 274639661,
    "executor_run_time": 924,
    "executor_cpu_time": 79853525,
    "result_size": 2738,
    "jvm_gc_time": 34,
    "result_serialization_time": 2,
    "memory_bytes_spilled": 0,
    "disk_bytes_spilled": 0,
    "peak_execution_memory": 0,
    "input_bytes": 0,
    "input_records": 0,
    "output_bytes": 0,
    "output_records": 0,
    "shuffle_remote_blocks_fetched": 0,
    "shuffle_local_blocks_fetched": 0,
    "shuffle_fetch_wait_time": 0,
    "shuffle_remote_bytes_read": 0,
    "shuffle_remote_bytes_read_to_disk": 0,
    "shuffle_local_bytes_read": 0,
    "shuffle_read_bytes": 0,
    "shuffle_read_records": 0,
    "shuffle_corrupt_merged_block_chunks": 0,
    "shuffle_merged_fetch_fallback_count": 0,
    "shuffle_merged_remote_blocks_fetched": 0,
    "shuffle_merged_local_blocks_fetched": 0,
    "shuffle_merged_remote_chunks_fetched": 0,
    "shuffle_merged_local_chunks_fetched": 0,
    "shuffle_merged_remote_bytes_read": 0,
    "shuffle_merged_local_bytes_read": 0,
    "shuffle_remote_reqs_duration": 0,
    "shuffle_merged_remote_reqs_duration": 0,
    "shuffle_write_bytes": 0,
    "shuffle_write_time": 0,
    "shuffle_write_records": 0,
    "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
    "description": null,
    "details": "org.apache.spark.rdd.RDD.collect(RDD.scala:1048)\norg.apache.spark.api.python.PythonRDD$.collectAndServe(PythonRDD.scala:195)\norg.apache.spark.api.python.PythonRDD.collectAndServe(PythonRDD.scala)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)\njava.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\njava.base/java.lang.reflect.Method.invoke(Method.java:569)\npy4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)\npy4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)\npy4j.Gateway.invoke(Gateway.java:282)\npy4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)\npy4j.commands.CallCommand.execute(CallCommand.java:79)\npy4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)\npy4j.ClientServerConnection.run(ClientServerConnection.java:106)\njava.base/java.lang.Thread.run(Thread.java:840)",
    "scheduling_pool": "default",
    "accumulator_updates": [],
    "tasks": null,
    "executor_summary": null,
    "speculation_summary": null,
    "killed_tasks_summary": {},
    "resource_profile_id": 0,
    "peak_executor_metrics": {
      "metrics": null
    },
    "task_metrics_distributions": null,
    "executor_metrics_distributions": null,
    "is_shuffle_push_enabled": false,
    "shuffle_mergers_count": 0
  }
]
```

### `list_slowest_stages`
Response format:
- list of slowest stages with durations
Sample (full):
```json
[
  {
    "status": "COMPLETE",
    "stage_id": 0,
    "attempt_id": 0,
    "num_tasks": 2,
    "num_active_tasks": 0,
    "num_complete_tasks": 2,
    "num_failed_tasks": 0,
    "num_killed_tasks": 0,
    "num_completed_indices": 2,
    "submission_time": "2025-06-26T00:06:57.054000Z",
    "first_task_launched_time": "2025-06-26T00:06:57.189000Z",
    "completion_time": "2025-06-26T00:06:58.569000Z",
    "failure_reason": null,
    "executor_deserialize_time": 383,
    "executor_deserialize_cpu_time": 274639661,
    "executor_run_time": 924,
    "executor_cpu_time": 79853525,
    "result_size": 2738,
    "jvm_gc_time": 34,
    "result_serialization_time": 2,
    "memory_bytes_spilled": 0,
    "disk_bytes_spilled": 0,
    "peak_execution_memory": 0,
    "input_bytes": 0,
    "input_records": 0,
    "output_bytes": 0,
    "output_records": 0,
    "shuffle_remote_blocks_fetched": 0,
    "shuffle_local_blocks_fetched": 0,
    "shuffle_fetch_wait_time": 0,
    "shuffle_remote_bytes_read": 0,
    "shuffle_remote_bytes_read_to_disk": 0,
    "shuffle_local_bytes_read": 0,
    "shuffle_read_bytes": 0,
    "shuffle_read_records": 0,
    "shuffle_corrupt_merged_block_chunks": 0,
    "shuffle_merged_fetch_fallback_count": 0,
    "shuffle_merged_remote_blocks_fetched": 0,
    "shuffle_merged_local_blocks_fetched": 0,
    "shuffle_merged_remote_chunks_fetched": 0,
    "shuffle_merged_local_chunks_fetched": 0,
    "shuffle_merged_remote_bytes_read": 0,
    "shuffle_merged_local_bytes_read": 0,
    "shuffle_remote_reqs_duration": 0,
    "shuffle_merged_remote_reqs_duration": 0,
    "shuffle_write_bytes": 0,
    "shuffle_write_time": 0,
    "shuffle_write_records": 0,
    "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
    "description": null,
    "details": "org.apache.spark.rdd.RDD.collect(RDD.scala:1048)\norg.apache.spark.api.python.PythonRDD$.collectAndServe(PythonRDD.scala:195)\norg.apache.spark.api.python.PythonRDD.collectAndServe(PythonRDD.scala)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)\njava.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\njava.base/java.lang.reflect.Method.invoke(Method.java:569)\npy4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)\npy4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)\npy4j.Gateway.invoke(Gateway.java:282)\npy4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)\npy4j.commands.CallCommand.execute(CallCommand.java:79)\npy4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)\npy4j.ClientServerConnection.run(ClientServerConnection.java:106)\njava.base/java.lang.Thread.run(Thread.java:840)",
    "scheduling_pool": "default",
    "accumulator_updates": [],
    "tasks": null,
    "executor_summary": null,
    "speculation_summary": null,
    "killed_tasks_summary": {},
    "resource_profile_id": 0,
    "peak_executor_metrics": {
      "metrics": null
    },
    "task_metrics_distributions": null,
    "executor_metrics_distributions": null,
    "is_shuffle_push_enabled": false,
    "shuffle_mergers_count": 0
  }
]
```

### `get_stage`
Response format:
- stage details (metrics, tasks, status)
Sample (full):
```json
{
  "status": "COMPLETE",
  "stage_id": 0,
  "attempt_id": 0,
  "num_tasks": 2,
  "num_active_tasks": 0,
  "num_complete_tasks": 2,
  "num_failed_tasks": 0,
  "num_killed_tasks": 0,
  "num_completed_indices": 2,
  "submission_time": "2025-06-26T00:06:57.054000Z",
  "first_task_launched_time": "2025-06-26T00:06:57.189000Z",
  "completion_time": "2025-06-26T00:06:58.569000Z",
  "failure_reason": null,
  "executor_deserialize_time": 383,
  "executor_deserialize_cpu_time": 274639661,
  "executor_run_time": 924,
  "executor_cpu_time": 79853525,
  "result_size": 2738,
  "jvm_gc_time": 34,
  "result_serialization_time": 2,
  "memory_bytes_spilled": 0,
  "disk_bytes_spilled": 0,
  "peak_execution_memory": 0,
  "input_bytes": 0,
  "input_records": 0,
  "output_bytes": 0,
  "output_records": 0,
  "shuffle_remote_blocks_fetched": 0,
  "shuffle_local_blocks_fetched": 0,
  "shuffle_fetch_wait_time": 0,
  "shuffle_remote_bytes_read": 0,
  "shuffle_remote_bytes_read_to_disk": 0,
  "shuffle_local_bytes_read": 0,
  "shuffle_read_bytes": 0,
  "shuffle_read_records": 0,
  "shuffle_corrupt_merged_block_chunks": 0,
  "shuffle_merged_fetch_fallback_count": 0,
  "shuffle_merged_remote_blocks_fetched": 0,
  "shuffle_merged_local_blocks_fetched": 0,
  "shuffle_merged_remote_chunks_fetched": 0,
  "shuffle_merged_local_chunks_fetched": 0,
  "shuffle_merged_remote_bytes_read": 0,
  "shuffle_merged_local_bytes_read": 0,
  "shuffle_remote_reqs_duration": 0,
  "shuffle_merged_remote_reqs_duration": 0,
  "shuffle_write_bytes": 0,
  "shuffle_write_time": 0,
  "shuffle_write_records": 0,
  "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
  "description": null,
  "details": "org.apache.spark.rdd.RDD.collect(RDD.scala:1048)\norg.apache.spark.api.python.PythonRDD$.collectAndServe(PythonRDD.scala:195)\norg.apache.spark.api.python.PythonRDD.collectAndServe(PythonRDD.scala)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\njava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)\njava.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\njava.base/java.lang.reflect.Method.invoke(Method.java:569)\npy4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)\npy4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)\npy4j.Gateway.invoke(Gateway.java:282)\npy4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)\npy4j.commands.CallCommand.execute(CallCommand.java:79)\npy4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)\npy4j.ClientServerConnection.run(ClientServerConnection.java:106)\njava.base/java.lang.Thread.run(Thread.java:840)",
  "scheduling_pool": "default",
  "accumulator_updates": [],
  "tasks": null,
  "executor_summary": null,
  "speculation_summary": null,
  "killed_tasks_summary": {},
  "resource_profile_id": 0,
  "peak_executor_metrics": {
    "metrics": null
  },
  "task_metrics_distributions": null,
  "executor_metrics_distributions": null,
  "is_shuffle_push_enabled": false,
  "shuffle_mergers_count": 0
}
```

### `get_stage_task_summary`
Response format:
- task-level summary and metric distributions
Sample (full):
```json
{
  "quantiles": [
    0.05,
    0.25,
    0.5,
    0.75,
    0.95
  ],
  "duration": [
    194.0,
    194.0,
    1190.0,
    1190.0,
    1190.0
  ],
  "executor_deserialize_time": [
    4.0,
    4.0,
    379.0,
    379.0,
    379.0
  ],
  "executor_deserialize_cpu_time": [
    4367132.0,
    4367132.0,
    270272529.0,
    270272529.0,
    270272529.0
  ],
  "executor_run_time": [
    176.0,
    176.0,
    748.0,
    748.0,
    748.0
  ],
  "executor_cpu_time": [
    5186259.0,
    5186259.0,
    74667266.0,
    74667266.0,
    74667266.0
  ],
  "result_size": [
    1326.0,
    1326.0,
    1412.0,
    1412.0,
    1412.0
  ],
  "jvm_gc_time": [
    0.0,
    0.0,
    34.0,
    34.0,
    34.0
  ],
  "result_serialization_time": [
    0.0,
    0.0,
    2.0,
    2.0,
    2.0
  ],
  "getting_result_time": [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
  ],
  "scheduler_delay": [
    14.0,
    14.0,
    61.0,
    61.0,
    61.0
  ],
  "peak_execution_memory": [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
  ],
  "memory_bytes_spilled": [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
  ],
  "disk_bytes_spilled": [
    0.0,
    0.0,
    0.0,
    0.0,
    0.0
  ],
  "input_metrics": {
    "bytes_read": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "records_read": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ]
  },
  "output_metrics": {
    "bytes_written": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "records_written": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ]
  },
  "shuffle_read_metrics": {
    "read_bytes": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "read_records": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "remote_blocks_fetched": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "local_blocks_fetched": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "fetch_wait_time": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "remote_bytes_read": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "remote_bytes_read_to_disk": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "total_blocks_fetched": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "remote_reqs_duration": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "shuffle_push_read_metrics_dist": {
      "corrupt_merged_block_chunks": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "merged_fetch_fallback_count": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "remote_merged_blocks_fetched": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "local_merged_blocks_fetched": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "remote_merged_chunks_fetched": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "local_merged_chunks_fetched": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "remote_merged_bytes_read": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "local_merged_bytes_read": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ],
      "remote_merged_reqs_duration": [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0
      ]
    }
  },
  "shuffle_write_metrics": {
    "write_bytes": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "write_records": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ],
    "write_time": [
      0.0,
      0.0,
      0.0,
      0.0,
      0.0
    ]
  }
}
```

### `list_slowest_sql_queries`
Response format:
- list of slowest SQL executions
Sample (full):
```json
[]
```

Executors
---------

### `list_executors`
Response format:
- list of executors with metrics
Sample (full):
```json
[
  {
    "id": "driver",
    "host_port": "spark-pi-python-ade66897a98eed73-driver-svc.spark-team-a.svc:7079",
    "is_active": true,
    "rdd_blocks": 0,
    "memory_used": 0,
    "disk_used": 0,
    "total_cores": 0,
    "max_tasks": 0,
    "active_tasks": 0,
    "failed_tasks": 0,
    "completed_tasks": 0,
    "total_tasks": 0,
    "total_duration": 7422,
    "total_gc_time": 0,
    "total_input_bytes": 0,
    "total_shuffle_read": 0,
    "total_shuffle_write": 0,
    "is_blacklisted": false,
    "max_memory": 122644070,
    "add_time": "2025-06-26T00:06:53.057000Z",
    "remove_time": null,
    "remove_reason": null,
    "executor_logs": {},
    "memory_metrics": {
      "used_on_heap_storage_memory": 0,
      "used_off_heap_storage_memory": 0,
      "total_on_heap_storage_memory": 122644070,
      "total_off_heap_storage_memory": 0
    },
    "blacklisted_in_stages": [],
    "peak_memory_metrics": null,
    "attributes": {},
    "resources": {},
    "resource_profile_id": 0,
    "is_excluded": false,
    "excluded_in_stages": []
  },
  {
    "id": "1",
    "host_port": "100.64.183.186:35935",
    "is_active": true,
    "rdd_blocks": 0,
    "memory_used": 0,
    "disk_used": 0,
    "total_cores": 1,
    "max_tasks": 1,
    "active_tasks": 0,
    "failed_tasks": 0,
    "completed_tasks": 2,
    "total_tasks": 2,
    "total_duration": 1384,
    "total_gc_time": 34,
    "total_input_bytes": 0,
    "total_shuffle_read": 0,
    "total_shuffle_write": 0,
    "is_blacklisted": false,
    "max_memory": 122644070,
    "add_time": "2025-06-26T00:06:56.549000Z",
    "remove_time": null,
    "remove_reason": null,
    "executor_logs": {},
    "memory_metrics": {
      "used_on_heap_storage_memory": 0,
      "used_off_heap_storage_memory": 0,
      "total_on_heap_storage_memory": 122644070,
      "total_off_heap_storage_memory": 0
    },
    "blacklisted_in_stages": [],
    "peak_memory_metrics": {
      "metrics": null
    },
    "attributes": {},
    "resources": {},
    "resource_profile_id": 0,
    "is_excluded": false,
    "excluded_in_stages": []
  }
]
```

### `get_executor`
Response format:
- executor details and metrics
Sample (full):
```json
{
  "id": "1",
  "host_port": "100.64.183.186:35935",
  "is_active": true,
  "rdd_blocks": 0,
  "memory_used": 0,
  "disk_used": 0,
  "total_cores": 1,
  "max_tasks": 1,
  "active_tasks": 0,
  "failed_tasks": 0,
  "completed_tasks": 2,
  "total_tasks": 2,
  "total_duration": 1384,
  "total_gc_time": 34,
  "total_input_bytes": 0,
  "total_shuffle_read": 0,
  "total_shuffle_write": 0,
  "is_blacklisted": false,
  "max_memory": 122644070,
  "add_time": "2025-06-26T00:06:56.549000Z",
  "remove_time": null,
  "remove_reason": null,
  "executor_logs": {},
  "memory_metrics": {
    "used_on_heap_storage_memory": 0,
    "used_off_heap_storage_memory": 0,
    "total_on_heap_storage_memory": 122644070,
    "total_off_heap_storage_memory": 0
  },
  "blacklisted_in_stages": [],
  "peak_memory_metrics": {
    "metrics": null
  },
  "attributes": {},
  "resources": {},
  "resource_profile_id": 0,
  "is_excluded": false,
  "excluded_in_stages": []
}
```

### `get_executor_summary`
Response format:
- aggregated executor metrics for the app
Sample (full):
```json
{
  "total_executors": 2,
  "active_executors": 2,
  "memory_used": 0,
  "disk_used": 0,
  "completed_tasks": 2,
  "failed_tasks": 0,
  "total_duration": 8806,
  "total_gc_time": 34,
  "total_input_bytes": 0,
  "total_shuffle_read": 0,
  "total_shuffle_write": 0,
  "peak_executors": 0,
  "average_executors": 0,
  "utilization_efficiency_percent": 0,
  "executor_utilization_percent": 116.27
}
```

### `get_resource_usage_timeline`
Response format:
- time series of resource usage
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "application_name": "PythonPi",
  "timeline": [
    {
      "timestamp": "2025-06-26T00:06:53.057000+00:00",
      "active_executors": 1,
      "total_cores": 0,
      "total_memory_mb": 116.96249961853027,
      "event": {
        "timestamp": "2025-06-26T00:06:53.057000+00:00",
        "type": "executor_add",
        "executor_id": "driver",
        "cores": 0,
        "memory_mb": 116.96249961853027
      }
    },
    {
      "timestamp": "2025-06-26T00:06:56.549000+00:00",
      "active_executors": 2,
      "total_cores": 1,
      "total_memory_mb": 233.92499923706055,
      "event": {
        "timestamp": "2025-06-26T00:06:56.549000+00:00",
        "type": "executor_add",
        "executor_id": "1",
        "cores": 1,
        "memory_mb": 116.96249961853027
      }
    },
    {
      "timestamp": "2025-06-26T00:06:57.054000+00:00",
      "active_executors": 2,
      "total_cores": 1,
      "total_memory_mb": 233.92499923706055,
      "event": {
        "timestamp": "2025-06-26T00:06:57.054000+00:00",
        "type": "stage_start",
        "stage_id": 0,
        "attempt_id": 0,
        "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
        "task_count": 2
      }
    },
    {
      "timestamp": "2025-06-26T00:06:58.569000+00:00",
      "active_executors": 2,
      "total_cores": 1,
      "total_memory_mb": 233.92499923706055,
      "event": {
        "timestamp": "2025-06-26T00:06:58.569000+00:00",
        "type": "stage_end",
        "stage_id": 0,
        "attempt_id": 0,
        "status": "COMPLETE",
        "duration_seconds": 1.515
      }
    }
  ],
  "summary": {
    "total_events": 4,
    "executor_additions": 2,
    "executor_removals": 0,
    "stage_executions": 1,
    "peak_executors": 2,
    "peak_cores": 1
  }
}
```

Analysis
--------

### `get_job_bottlenecks`
Response format:
- bottleneck analysis for a job
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "performance_bottlenecks": {
    "slowest_stages": [
      {
        "stage_id": 0,
        "attempt_id": 0,
        "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
        "duration_seconds": 1.515,
        "task_count": 2,
        "failed_tasks": 0
      }
    ],
    "slowest_jobs": [
      {
        "job_id": 0,
        "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
        "duration_seconds": 1.541,
        "failed_tasks": 0,
        "status": "SUCCEEDED"
      }
    ]
  },
  "resource_bottlenecks": {
    "memory_spill_stages": [],
    "gc_pressure_ratio": 0.003861003861003861,
    "executor_utilization": {
      "total_executors": 2,
      "active_executors": 2,
      "utilization_ratio": 1.0
    }
  },
  "recommendations": []
}
```

### `analyze_auto_scaling`
Response format:
- auto-scaling analysis and recommendations
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "analysis_type": "Auto-scaling Configuration",
  "target_stage_duration_minutes": 2,
  "recommendations": {
    "initial_executors": {
      "type": "auto_scaling",
      "priority": "medium",
      "issue": "Initial executors could be optimized (current: Not set)",
      "suggestion": "Set spark.dynamicAllocation.initialExecutors to 2",
      "configuration": {
        "parameter": "spark.dynamicAllocation.initialExecutors",
        "current_value": "Not set",
        "recommended_value": "2",
        "description": "Based on stages running in first 2 minutes"
      }
    },
    "max_executors": {
      "type": "auto_scaling",
      "priority": "medium",
      "issue": "Max executors could be optimized (current: Not set)",
      "suggestion": "Set spark.dynamicAllocation.maxExecutors to 2",
      "configuration": {
        "parameter": "spark.dynamicAllocation.maxExecutors",
        "current_value": "Not set",
        "recommended_value": "2",
        "description": "Based on peak concurrent stage demand"
      }
    }
  },
  "analysis_details": {
    "total_stages": 1,
    "initial_stages_analyzed": 0,
    "peak_concurrent_demand": 2,
    "calculation_method": "Aims to complete stages in 2 minutes",
    "configuration_analysis": {
      "initial_executors": {
        "current": "Not set",
        "recommended": "2",
        "description": "Based on stages running in first 2 minutes"
      },
      "max_executors": {
        "current": "Not set",
        "recommended": "2",
        "description": "Based on peak concurrent stage demand"
      }
    }
  }
}
```

### `analyze_shuffle_skew`
Response format:
- skewed stages and recommendations
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "analysis_type": "Shuffle Skew Analysis",
  "parameters": {
    "shuffle_threshold_gb": 10,
    "skew_ratio_threshold": 2.0
  },
  "skewed_stages": [],
  "summary": {
    "total_stages_analyzed": 0,
    "skewed_stages_count": 0,
    "task_skewed_count": 0,
    "executor_skewed_count": 0,
    "max_skew_ratio": 0
  },
  "recommendations": []
}
```

### `analyze_failed_tasks`
Response format:
- failed stages, problematic executors, summary
Sample (full):
```json
{
  "application_id": "spark-bcec39f6201b42b9925124595baad260",
  "analysis_type": "Failed Task Analysis",
  "parameters": {
    "failure_threshold": 1
  },
  "failed_stages": [],
  "problematic_executors": [],
  "summary": {
    "total_failed_tasks": 0,
    "stages_with_failures": 0,
    "executors_with_failures": 0,
    "overall_failure_impact": "low"
  },
  "recommendations": []
}
```

Comparisons
-----------

### `compare_app_performance`
Response format:
- aggregated overview, stage deep dive, app summary diff, recommendations
Sample (full):
```json
{
  "schema_version": 1,
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "name": "PythonPi",
      "stage_count": 1
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "name": "NewYorkTaxiData_2025_06_27_00_24_44",
      "stage_count": 8
    }
  },
  "aggregated_overview": {
    "application_summary": {},
    "job_performance": {},
    "stage_metrics": {
      "applications": {
        "app1": {
          "id": "spark-bcec39f6201b42b9925124595baad260",
          "stage_metrics": {
            "total_stages": 1,
            "total_stage_duration": 1.515,
            "total_executor_run_time": 924.0,
            "total_memory_spilled": 0.0,
            "total_shuffle_read_bytes": 0.0,
            "total_shuffle_write_bytes": 0.0,
            "total_input_bytes": 0.0,
            "total_output_bytes": 0.0,
            "total_tasks": 2,
            "total_failed_tasks": 0,
            "completed_stages": 1,
            "failed_stages": 0
          }
        },
        "app2": {
          "id": "spark-110be3a8424d4a2789cb88134418217b",
          "stage_metrics": {
            "total_stages": 8,
            "total_stage_duration": 259.70799999999997,
            "total_executor_run_time": 552516.0,
            "total_memory_spilled": 0.0,
            "total_shuffle_read_bytes": 5387322083.0,
            "total_shuffle_write_bytes": 5387322083.0,
            "total_input_bytes": 1984229200.0,
            "total_output_bytes": 2702896161.0,
            "total_tasks": 73,
            "total_failed_tasks": 0,
            "completed_stages": 6,
            "failed_stages": 0
          }
        }
      },
      "stage_comparison": {
        "stage_count_ratio": 0.125,
        "duration_ratio": 0.005833474517535078,
        "executor_runtime_ratio": 0.00167234976000695,
        "shuffle_read_ratio": 0.0,
        "shuffle_write_ratio": 0.0,
        "input_ratio": 0.0,
        "output_ratio": 0.0
      },
      "efficiency_analysis": {
        "app1_avg_tasks_per_stage": 2.0,
        "app2_avg_tasks_per_stage": 9.125,
        "app1_input_throughput_bps": 0.0,
        "app1_output_throughput_bps": 0.0,
        "app2_input_throughput_bps": 7640231.336732023,
        "app2_output_throughput_bps": 10407442.824248772
      },
      "efficiency_ratios": {
        "tasks_per_stage_ratio": 4.5625
      },
      "recommendations": [],
      "filtering_summary": {
        "stage_comparison": {
          "total_metrics": 9,
          "significant_metrics": 7,
          "filtering_applied": true
        },
        "efficiency_ratios": {
          "total_metrics": 1,
          "significant_metrics": 1,
          "filtering_applied": false
        },
        "significance_threshold": 0.1
      }
    },
    "executor_performance": {
      "applications": {
        "app1": {
          "id": "spark-bcec39f6201b42b9925124595baad260",
          "executor_metrics": {
            "total_executors": 2,
            "active_executors": 2,
            "memory_used": 0,
            "disk_used": 0,
            "completed_tasks": 2,
            "failed_tasks": 0,
            "total_duration": 8806,
            "total_gc_time": 34,
            "total_input_bytes": 0,
            "total_shuffle_read": 0,
            "total_shuffle_write": 0,
            "peak_executors": 0,
            "average_executors": 0,
            "utilization_efficiency_percent": 0,
            "executor_utilization_percent": 116.27
          }
        },
        "app2": {
          "id": "spark-110be3a8424d4a2789cb88134418217b",
          "executor_metrics": {
            "total_executors": 5,
            "active_executors": 5,
            "memory_used": 0,
            "disk_used": 0,
            "completed_tasks": 39,
            "failed_tasks": 0,
            "total_duration": 858943,
            "total_gc_time": 938,
            "total_input_bytes": 1984229200,
            "total_shuffle_read": 5387322083,
            "total_shuffle_write": 5387322083,
            "peak_executors": 5,
            "average_executors": 3.5,
            "utilization_efficiency_percent": 70.0,
            "executor_utilization_percent": 69.29
          }
        }
      },
      "executor_comparison": {
        "total_executors_ratio": 0.4,
        "completed_tasks_ratio": 0.05128205128205128,
        "total_duration_ratio": 0.010252135473483106,
        "total_gc_time_ratio": 0.03624733475479744,
        "total_input_bytes_ratio": 0.0,
        "total_shuffle_read_ratio": 0.0,
        "total_shuffle_write_ratio": 0.0,
        "peak_executors_ratio": 0.0,
        "average_executors_ratio": 0.0,
        "utilization_efficiency_percent_ratio": 0.0,
        "executor_utilization_percent_ratio": 1.6780199162938372
      },
      "efficiency_metrics": {
        "app1_tasks_per_executor": 1.0,
        "app2_tasks_per_executor": 7.8
      },
      "efficiency_ratios": {
        "tasks_per_executor_ratio": 7.8
      },
      "recommendations": [
        {
          "type": "executor_scaling",
          "priority": "high",
          "issue": "App2 uses 0.4x fewer executors than App1",
          "suggestion": "App2 may benefit from increased parallelism - consider scaling up executors"
        }
      ],
      "filtering_summary": {
        "executor_comparison": {
          "total_metrics": 14,
          "significant_metrics": 11,
          "filtering_applied": true
        },
        "efficiency_ratios": {
          "total_metrics": 1,
          "significant_metrics": 1,
          "filtering_applied": false
        },
        "significance_threshold": 0.1
      }
    }
  },
  "stage_deep_dive": {
    "error": "No matching stages found between applications (similarity threshold: 0.6)",
    "applications": {
      "app1": {
        "id": "spark-bcec39f6201b42b9925124595baad260",
        "name": "PythonPi",
        "stage_count": 1
      },
      "app2": {
        "id": "spark-110be3a8424d4a2789cb88134418217b",
        "name": "NewYorkTaxiData_2025_06_27_00_24_44",
        "stage_count": 8
      }
    },
    "analysis_parameters": {
      "requested_top_n": 3,
      "similarity_threshold": 0.6,
      "available_stages_app1": 1,
      "available_stages_app2": 8,
      "matched_stages": 0
    },
    "stage_summary": {
      "matched_stages": 0,
      "total_time_difference_seconds": 0.0,
      "average_time_difference_seconds": 0.0,
      "max_time_difference_seconds": 0.0
    },
    "suggestion": "Try lowering the similarity_threshold parameter or check that applications are performing similar operations"
  },
  "error": "No matching stages found between applications (similarity threshold: 0.6)",
  "recommendations": [],
  "key_recommendations": []
}
```

### `compare_app_summaries`
Response format:
- app1_summary, app2_summary, diff, optional aggregated_stage_comparison
Sample (full):
```json
{
  "app1_summary": {
    "application_duration_minutes": 0.12,
    "total_executor_runtime_minutes": 0.02,
    "executor_utilization_percent": 12.2,
    "total_stages": 1,
    "completed_stages": 1,
    "application_id": "spark-bcec39f6201b42b9925124595baad260"
  },
  "app2_summary": {
    "application_duration_minutes": 5.07,
    "total_executor_runtime_minutes": 9.21,
    "executor_utilization_percent": 44.57,
    "total_stages": 8,
    "completed_stages": 6,
    "application_id": "spark-110be3a8424d4a2789cb88134418217b"
  },
  "diff": {
    "application_duration_minutes_change": "+4125.0%",
    "total_executor_runtime_minutes_change": "+45950.0%",
    "executor_utilization_percent_change": "+265.3%",
    "total_stages_change": "+700.0%",
    "completed_stages_change": "+500.0%"
  },
  "filtering_summary": {
    "total_metrics": 17,
    "significant_metrics": 5,
    "significance_threshold": 0.1,
    "filtering_applied": true
  },
  "aggregated_stage_comparison": {
    "applications": {
      "app1": {
        "id": "spark-bcec39f6201b42b9925124595baad260",
        "stage_metrics": {
          "total_stages": 1,
          "total_stage_duration": 1.515,
          "total_executor_run_time": 924.0,
          "total_memory_spilled": 0.0,
          "total_shuffle_read_bytes": 0.0,
          "total_shuffle_write_bytes": 0.0,
          "total_input_bytes": 0.0,
          "total_output_bytes": 0.0,
          "total_tasks": 2,
          "total_failed_tasks": 0,
          "completed_stages": 1,
          "failed_stages": 0
        }
      },
      "app2": {
        "id": "spark-110be3a8424d4a2789cb88134418217b",
        "stage_metrics": {
          "total_stages": 8,
          "total_stage_duration": 259.70799999999997,
          "total_executor_run_time": 552516.0,
          "total_memory_spilled": 0.0,
          "total_shuffle_read_bytes": 5387322083.0,
          "total_shuffle_write_bytes": 5387322083.0,
          "total_input_bytes": 1984229200.0,
          "total_output_bytes": 2702896161.0,
          "total_tasks": 73,
          "total_failed_tasks": 0,
          "completed_stages": 6,
          "failed_stages": 0
        }
      }
    },
    "stage_comparison": {
      "stage_count_ratio": 0.125,
      "duration_ratio": 0.005833474517535078,
      "executor_runtime_ratio": 0.00167234976000695,
      "shuffle_read_ratio": 0.0,
      "shuffle_write_ratio": 0.0,
      "input_ratio": 0.0,
      "output_ratio": 0.0
    },
    "efficiency_analysis": {
      "app1_avg_tasks_per_stage": 2.0,
      "app2_avg_tasks_per_stage": 9.125,
      "app1_input_throughput_bps": 0.0,
      "app1_output_throughput_bps": 0.0,
      "app2_input_throughput_bps": 7640231.336732023,
      "app2_output_throughput_bps": 10407442.824248772
    },
    "efficiency_ratios": {
      "tasks_per_stage_ratio": 4.5625
    },
    "recommendations": [],
    "filtering_summary": {
      "stage_comparison": {
        "total_metrics": 9,
        "significant_metrics": 7,
        "filtering_applied": true
      },
      "efficiency_ratios": {
        "total_metrics": 1,
        "significant_metrics": 1,
        "filtering_applied": false
      },
      "significance_threshold": 0.1
    }
  }
}
```

### `compare_app_executors`
Response format:
- executor metrics and efficiency ratios
Sample (full):
```json
{
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "executor_metrics": {
        "total_executors": 2,
        "active_executors": 2,
        "memory_used": 0,
        "disk_used": 0,
        "completed_tasks": 2,
        "failed_tasks": 0,
        "total_duration": 8806,
        "total_gc_time": 34,
        "total_input_bytes": 0,
        "total_shuffle_read": 0,
        "total_shuffle_write": 0,
        "peak_executors": 0,
        "average_executors": 0,
        "utilization_efficiency_percent": 0,
        "executor_utilization_percent": 116.27
      }
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "executor_metrics": {
        "total_executors": 5,
        "active_executors": 5,
        "memory_used": 0,
        "disk_used": 0,
        "completed_tasks": 39,
        "failed_tasks": 0,
        "total_duration": 858943,
        "total_gc_time": 938,
        "total_input_bytes": 1984229200,
        "total_shuffle_read": 5387322083,
        "total_shuffle_write": 5387322083,
        "peak_executors": 5,
        "average_executors": 3.5,
        "utilization_efficiency_percent": 70.0,
        "executor_utilization_percent": 69.29
      }
    }
  },
  "executor_comparison": {
    "total_executors_ratio": 0.4,
    "completed_tasks_ratio": 0.05128205128205128,
    "total_duration_ratio": 0.010252135473483106,
    "total_gc_time_ratio": 0.03624733475479744,
    "total_input_bytes_ratio": 0.0,
    "total_shuffle_read_ratio": 0.0,
    "total_shuffle_write_ratio": 0.0,
    "peak_executors_ratio": 0.0,
    "average_executors_ratio": 0.0,
    "utilization_efficiency_percent_ratio": 0.0,
    "executor_utilization_percent_ratio": 1.6780199162938372
  },
  "efficiency_metrics": {
    "app1_tasks_per_executor": 1.0,
    "app2_tasks_per_executor": 7.8
  },
  "efficiency_ratios": {
    "tasks_per_executor_ratio": 7.8
  },
  "recommendations": [
    {
      "type": "executor_scaling",
      "priority": "high",
      "issue": "App2 uses 0.4x fewer executors than App1",
      "suggestion": "App2 may benefit from increased parallelism - consider scaling up executors"
    }
  ],
  "filtering_summary": {
    "executor_comparison": {
      "total_metrics": 14,
      "significant_metrics": 11,
      "filtering_applied": true
    },
    "efficiency_ratios": {
      "total_metrics": 1,
      "significant_metrics": 1,
      "filtering_applied": false
    },
    "significance_threshold": 0.1
  }
}
```

### `compare_app_jobs`
Response format:
- job statistics and ratios
Sample (full):
```json
{
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "job_stats": {
        "count": 1,
        "completed_count": 1,
        "failed_count": 0,
        "avg_duration": 1.541,
        "total_duration": 1.541
      },
      "success_rate": 1.0
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "job_stats": {
        "count": 6,
        "completed_count": 6,
        "failed_count": 0,
        "avg_duration": 43.29066666666666,
        "total_duration": 259.74399999999997
      },
      "success_rate": 1.0
    }
  },
  "job_comparison": {
    "job_count_ratio": 6.0,
    "avg_duration_ratio": 28.092580575383945,
    "total_duration_ratio": 168.5554834523037,
    "completion_rate_ratio": 1.0
  },
  "timing_analysis": {
    "avg_duration_difference_seconds": 41.74966666666666,
    "avg_duration_improvement_percent": -2709.258057538395
  },
  "recommendations": [
    {
      "type": "job_complexity",
      "priority": "medium",
      "issue": "App2 has 6.0x more jobs than App1",
      "suggestion": "App2 may have more complex workflow or different job decomposition strategy"
    },
    {
      "type": "job_performance",
      "priority": "high",
      "issue": "App2 jobs are 28.1x slower on average than App1",
      "suggestion": "Investigate job-level performance bottlenecks in App2 - may need optimization or resource scaling"
    },
    {
      "type": "overall_efficiency",
      "priority": "medium",
      "issue": "App2 takes 168.6x longer total execution time",
      "suggestion": "App2 may benefit from better parallelization or resource optimization"
    }
  ]
}
```

### `compare_app_stages_aggregated`
Response format:
- aggregated stage metrics and ratios
Sample (full):
```json
{
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "stage_metrics": {
        "total_stages": 1,
        "total_stage_duration": 1.515,
        "total_executor_run_time": 924.0,
        "total_memory_spilled": 0.0,
        "total_shuffle_read_bytes": 0.0,
        "total_shuffle_write_bytes": 0.0,
        "total_input_bytes": 0.0,
        "total_output_bytes": 0.0,
        "total_tasks": 2,
        "total_failed_tasks": 0,
        "completed_stages": 1,
        "failed_stages": 0
      }
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "stage_metrics": {
        "total_stages": 8,
        "total_stage_duration": 259.70799999999997,
        "total_executor_run_time": 552516.0,
        "total_memory_spilled": 0.0,
        "total_shuffle_read_bytes": 5387322083.0,
        "total_shuffle_write_bytes": 5387322083.0,
        "total_input_bytes": 1984229200.0,
        "total_output_bytes": 2702896161.0,
        "total_tasks": 73,
        "total_failed_tasks": 0,
        "completed_stages": 6,
        "failed_stages": 0
      }
    }
  },
  "stage_comparison": {
    "stage_count_ratio": 0.125,
    "duration_ratio": 0.005833474517535078,
    "executor_runtime_ratio": 0.00167234976000695,
    "shuffle_read_ratio": 0.0,
    "shuffle_write_ratio": 0.0,
    "input_ratio": 0.0,
    "output_ratio": 0.0
  },
  "efficiency_analysis": {
    "app1_avg_tasks_per_stage": 2.0,
    "app2_avg_tasks_per_stage": 9.125,
    "app1_input_throughput_bps": 0.0,
    "app1_output_throughput_bps": 0.0,
    "app2_input_throughput_bps": 7640231.336732023,
    "app2_output_throughput_bps": 10407442.824248772
  },
  "efficiency_ratios": {
    "tasks_per_stage_ratio": 4.5625
  },
  "recommendations": [],
  "filtering_summary": {
    "stage_comparison": {
      "total_metrics": 9,
      "significant_metrics": 7,
      "filtering_applied": true
    },
    "efficiency_ratios": {
      "total_metrics": 1,
      "significant_metrics": 1,
      "filtering_applied": false
    },
    "significance_threshold": 0.1
  }
}
```

### `compare_app_resources`
Response format:
- resource ratios and recommendations
Sample (full):
```json
{
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "name": "PythonPi",
      "cores_granted": null,
      "max_cores": null,
      "cores_per_executor": null,
      "memory_per_executor_mb": null,
      "max_executors": null
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "name": "NewYorkTaxiData_2025_06_27_00_24_44",
      "cores_granted": null,
      "max_cores": null,
      "cores_per_executor": null,
      "memory_per_executor_mb": null,
      "max_executors": null
    }
  },
  "resource_comparison": {},
  "recommendations": []
}
```

### `compare_app_environments`
Response format:
- spark/system property diffs, JVM info, summary
Sample (full):
```json
{
  "spark_properties": {
    "different": [
      {
        "property": "spark.app.name",
        "app1_value": "PythonPi",
        "app2_value": "NewYorkTaxiData_2025_06_27_00_24_44"
      },
      {
        "property": "spark.driver.memory",
        "app1_value": "512m",
        "app2_value": "4g"
      },
      {
        "property": "spark.eventLog.dir",
        "app1_value": "s3a://spark-operator-doeks-spark-logs-20250224211454008900000007/spark-event-logs",
        "app2_value": "s3a://sparksense-ai-spark-20250624175205897100000006/spark-event-logs"
      },
      {
        "property": "spark.executor.instances",
        "app1_value": "1",
        "app2_value": "4"
      },
      {
        "property": "spark.executor.memory",
        "app1_value": "512m",
        "app2_value": "4g"
      },
      {
        "property": "spark.kubernetes.driver.label.sparkoperator.k8s.io/app-name",
        "app1_value": "spark-pi-python",
        "app2_value": "taxi-trip"
      },
      {
        "property": "spark.kubernetes.driver.pod.name",
        "app1_value": "spark-pi-python-driver",
        "app2_value": "taxi-trip-driver"
      },
      {
        "property": "spark.kubernetes.executor.label.sparkoperator.k8s.io/app-name",
        "app1_value": "spark-pi-python",
        "app2_value": "taxi-trip"
      },
      {
        "property": "spark.kubernetes.executor.podNamePrefix",
        "app1_value": "pythonpi-009fa697a98f0548",
        "app2_value": "taxi-trip"
      }
    ],
    "total_different": 9,
    "app1_only_count": 1,
    "app2_only_count": 21
  },
  "system_properties": {
    "different": [
      {
        "property": "java.vm.compressedOopsMode",
        "app1_value": "32-bit",
        "app2_value": "Zero based"
      },
      {
        "property": "os.version",
        "app1_value": "6.1.134-150.224.amzn2023.x86_64",
        "app2_value": "6.1.140-154.222.amzn2023.x86_64"
      },
      {
        "property": "sun.java.command",
        "app1_value": "org.apache.spark.deploy.SparkSubmit --deploy-mode client --conf spark.driver.bindAddress=100.64.10.2 --conf spark.executorEnv.SPARK_DRIVER_POD_IP=100.64.10.2 --properties-file /opt/spark/conf/spark.properties --class org.apache.spark.deploy.PythonRunner local:///opt/spark/examples/src/main/python/pi.py",
        "app2_value": "org.apache.spark.deploy.SparkSubmit --deploy-mode client --conf spark.driver.bindAddress=100.64.122.12 --conf spark.executorEnv.SPARK_DRIVER_POD_IP=100.64.122.12 --properties-file /opt/spark/conf/spark.properties --class org.apache.spark.deploy.PythonRunner s3a://sparksense-ai-spark-20250624175205897100000006/taxi-trip/scripts/pyspark-taxi-trip.py s3a://sparksense-ai-spark-20250624175205897100000006/taxi-trip/input/ s3a://sparksense-ai-spark-20250624175205897100000006/taxi-trip/output/"
      }
    ],
    "total_different": 3,
    "app1_only_count": 0,
    "app2_only_count": 0
  },
  "jvm_info": {
    "java_version": {
      "app1": "17.0.12 (Eclipse Adoptium)",
      "app2": "17.0.12 (Eclipse Adoptium)"
    },
    "java_home": {
      "app1": "/opt/java/openjdk",
      "app2": "/opt/java/openjdk"
    },
    "scala_version": {
      "app1": "version 2.12.18",
      "app2": "version 2.12.18"
    }
  },
  "summary": {
    "total_spark_differences": 9,
    "shown_spark_differences": 9,
    "spark_app1_only": 1,
    "spark_app2_only": 21,
    "total_system_differences": 3,
    "shown_system_differences": 3,
    "system_app1_only": 0,
    "system_app2_only": 0,
    "note": "Use get_environment tool for full property details"
  },
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260"
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b"
    }
  }
}
```

### `compare_app_executor_timeline`
Response format:
- executor timeline comparison and efficiency
Sample (full):
```json
{
  "app1_info": {
    "app_id": "spark-bcec39f6201b42b9925124595baad260",
    "name": "PythonPi",
    "start_time": "2025-06-26T00:06:51.168000+00:00",
    "end_time": "2025-06-26T00:06:58.590000+00:00",
    "duration_seconds": 7.422
  },
  "app2_info": {
    "app_id": "spark-110be3a8424d4a2789cb88134418217b",
    "name": "NewYorkTaxiData_2025_06_27_00_24_44",
    "start_time": "2025-06-27T00:24:44.577000+00:00",
    "end_time": "2025-06-27T00:29:48.493000+00:00",
    "duration_seconds": 303.916
  },
  "comparison_config": {
    "interval_minutes": 1,
    "original_intervals_compared": 1,
    "merged_intervals_shown": 1,
    "analysis_type": "App-Level Executor Timeline Comparison"
  },
  "timeline_comparison": [
    {
      "interval": 1,
      "timestamp_range": "2025-06-26T00:06:51.168000+00:00 to 2025-06-26T00:06:58.590000+00:00",
      "app1": {
        "executor_count": 2
      },
      "app2": {
        "executor_count": 1
      },
      "differences": {
        "executor_count_diff": -1
      }
    }
  ],
  "resource_efficiency": {
    "app1": {
      "total_executors": 2,
      "total_stages": 1,
      "peak_executor_count": 2,
      "avg_executor_count": 2.0,
      "peak_cores": 1,
      "peak_memory_mb": 233.92499923706055,
      "avg_utilization": 2.0,
      "efficiency_score": 1.0,
      "resource_waste_intervals": 0
    },
    "app2": {
      "total_executors": 5,
      "total_stages": 8,
      "peak_executor_count": 5,
      "avg_executor_count": 4.333333333333333,
      "peak_cores": 4,
      "peak_memory_mb": 11387.999997138977,
      "avg_utilization": 4.333333333333333,
      "efficiency_score": 0.8666666666666666,
      "resource_waste_intervals": 1
    }
  },
  "summary": {
    "original_intervals": 1,
    "merged_intervals": 1,
    "intervals_with_differences": 1,
    "avg_executor_count_difference": 1.0,
    "max_executor_count_difference": 1,
    "app2_more_efficient": false,
    "performance_improvement": {
      "time_difference_seconds": -296.49399999999997,
      "efficiency_improvement_ratio": 0.8666666666666666
    }
  },
  "recommendations": [
    {
      "type": "resource_allocation",
      "priority": "medium",
      "issue": "App2 uses 117% more executors on average",
      "suggestion": "Consider if App2's higher resource allocation provides proportional performance benefits"
    }
  ],
  "key_differences": {
    "peak_executor_difference": 3,
    "avg_executor_difference": 2.333333333333333,
    "duration_difference_seconds": 296.49399999999997
  }
}
```

### `compare_stages`
Response format:
- stage comparison, significant differences, summary
Sample (full):
```json
{
  "stage_comparison": {
    "stage1": {
      "app_id": "spark-bcec39f6201b42b9925124595baad260",
      "stage_id": 0,
      "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
      "status": "COMPLETE"
    },
    "stage2": {
      "app_id": "spark-110be3a8424d4a2789cb88134418217b",
      "stage_id": 7,
      "name": "parquet at NativeMethodAccessorImpl.java:0",
      "status": "COMPLETE"
    }
  },
  "significant_differences": {
    "stage_metrics": {
      "output_bytes": {
        "stage1": 0,
        "stage2": 2702896161,
        "change": "+270289616100.0%",
        "significance": 1.0
      },
      "output_records": {
        "stage1": 0,
        "stage2": 125660481,
        "change": "+12566048100.0%",
        "significance": 1.0
      },
      "shuffle_remote_blocks_fetched": {
        "stage1": 0,
        "stage2": 25,
        "change": "+2500.0%",
        "significance": 1.0
      },
      "shuffle_local_blocks_fetched": {
        "stage1": 0,
        "stage2": 9,
        "change": "+900.0%",
        "significance": 1.0
      },
      "shuffle_remote_bytes_read": {
        "stage1": 0,
        "stage2": 3961264450,
        "change": "+396126445000.0%",
        "significance": 1.0
      }
    }
  },
  "summary": {
    "significance_threshold": 0.1,
    "total_differences_found": 39,
    "differences_shown": 5
  }
}
```

### `compare_stage_executor_timeline`
Response format:
- stage-level executor timeline comparison
Sample (full):
```json
{
  "app1_info": {
    "app_id": "spark-bcec39f6201b42b9925124595baad260",
    "stage_details": {
      "stage_id": 0,
      "attempt_id": 0,
      "name": "reduce at /opt/spark/examples/src/main/python/pi.py:42",
      "submission_time": "2025-06-26T00:06:57.054000+00:00",
      "completion_time": "2025-06-26T00:06:58.569000+00:00",
      "duration_seconds": 1.515
    }
  },
  "app2_info": {
    "app_id": "spark-110be3a8424d4a2789cb88134418217b",
    "stage_details": {
      "stage_id": 7,
      "attempt_id": 0,
      "name": "parquet at NativeMethodAccessorImpl.java:0",
      "submission_time": "2025-06-27T00:27:26.009000+00:00",
      "completion_time": "2025-06-27T00:29:42.935000+00:00",
      "duration_seconds": 136.926
    }
  },
  "comparison_config": {
    "interval_minutes": 1,
    "original_intervals_compared": 1,
    "merged_intervals_shown": 1
  },
  "timeline_comparison": [
    {
      "interval": 1,
      "timestamp_range": "2025-06-26T00:06:57.054000+00:00 to 2025-06-26T00:06:58.569000+00:00",
      "app1": {
        "executor_count": 2
      },
      "app2": {
        "executor_count": 5
      },
      "differences": {
        "executor_count_diff": 3
      }
    }
  ],
  "summary": {
    "original_intervals": 1,
    "merged_intervals": 1,
    "intervals_with_executor_differences": 1,
    "max_executor_count_difference": 3,
    "stages_overlap": true
  }
}
```

### `find_top_stage_differences`
Response format:
- top stage differences and analysis parameters
Sample (full):
```json
{
  "error": "No matching stages found between applications (similarity threshold: 0.6)",
  "applications": {
    "app1": {
      "id": "spark-bcec39f6201b42b9925124595baad260",
      "name": "PythonPi",
      "stage_count": 1
    },
    "app2": {
      "id": "spark-110be3a8424d4a2789cb88134418217b",
      "name": "NewYorkTaxiData_2025_06_27_00_24_44",
      "stage_count": 8
    }
  },
  "analysis_parameters": {
    "requested_top_n": 3,
    "similarity_threshold": 0.6,
    "available_stages_app1": 1,
    "available_stages_app2": 8,
    "matched_stages": 0
  },
  "stage_summary": {
    "matched_stages": 0,
    "total_time_difference_seconds": 0.0,
    "average_time_difference_seconds": 0.0,
    "max_time_difference_seconds": 0.0
  },
  "suggestion": "Try lowering the similarity_threshold parameter or check that applications are performing similar operations"
}
```
