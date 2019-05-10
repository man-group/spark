/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.nio.file.{Files, Paths}
import java.util

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.executor.CgroupMetricReader.findCgroupPath
import org.apache.spark.internal.Logging
import org.apache.spark.status.api.v1.CgroupMetrics

import scala.io.{BufferedSource, Source}


class CgroupMetricReader extends Logging {
  private var cgroupPathCache: util.TreeMap[String, Option[String]] =
    new util.TreeMap[String, Option[String]]()

  /*
  private var cgroupMemoryUsageInBytes: Long = 0
  private var cgroupMemoryPeakInBytes: Long = 0
  private var cgroupMemoryLimitInBytes: Long = 0
  */

  private[spark] def readCgroupVariable(cgroupType: String,
                                        cgroupVariable: String
                                       ): Option[String] = {
    var cgroupPath = cgroupPathCache.get(cgroupType)
    if (cgroupPath == null) {
      cgroupPath = findCgroupPath(cgroupType)
      cgroupPathCache.put(cgroupType, cgroupPath)
    }

    if (cgroupPath.isEmpty) {
      return None
    }

    val cgroupVariablePath = s"/sys/fs/cgroup/$cgroupType${cgroupPath.get}/$cgroupVariable"
    try {
      val cgroupVariableFile = Source.fromFile(cgroupVariablePath)
      try {
        Some(cgroupVariableFile.mkString.trim())
      } finally {
        cgroupVariableFile.close()
      }
    } catch {
      case e: java.io.FileNotFoundException =>
        None
      case e: Exception =>
        logWarning(s"readCgroupVariable(): Exception: ${ExceptionUtils.getStackTrace(e)}")
        None
    }
  }

  def getCgroupMemoryUsageInBytes(): Option[Long] = {
    readCgroupVariable(
      "memory",
      "memory.memsw.usage_in_bytes"
    ).map(x => {
      x.toLong
    })
  }

  def getCgroupMemoryPeakInBytes(): Option[Long] = {
    readCgroupVariable(
      "memory",
      "memory.memsw.max_usage_in_bytes"
    ).map(x => {
      x.toLong
    })
  }

  def getCgroupMemoryLimitInBytes(): Option[Long] = {
    /*
     * george@spark-1:~$ cat /sys/fs/cgroup/memory/hadoop-yarn/container_1558621002730_0013_01_000002/memory.memsw.limit_in_bytes
     * 9223372036854771712
     * george@spark-1:~$ cat /sys/fs/cgroup/memory/hadoop-yarn/container_1558621002730_0013_01_000002/memory.limit_in_bytes
     * 1610612736
     *
     * See https://github.com/apache/hadoop/blob/rel/release-3.1.2/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/linux/resources/CGroupsHandler.java#L78
     */
    readCgroupVariable(
      "memory",
      "memory.limit_in_bytes"
    ).map(x => {
      x.toLong
    })
  }

  private[spark] def getCgroupMetrics(): CgroupMetrics = {
    new CgroupMetrics(
      getCgroupMemoryUsageInBytes().getOrElse(0),
      getCgroupMemoryPeakInBytes().getOrElse(0),
      getCgroupMemoryLimitInBytes().getOrElse(0)
    )
  }
}

object CgroupMetricReader extends Logging {
  /**
    * On Kubernetes 1.13, /proc/self/cgroup will look something like this:
    *
    * <pre>
    * 1:name=systemd:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 2:cpuacct,cpu:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 3:cpuset:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 4:memory:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 5:perf_event:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 6:net_prio,net_cls:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 7:blkio:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 8:pids:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 9:freezer:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 10:devices:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * 11:hugetlb:/kubepods/burstable/pod5b[...]5d/8eaf[...]c8
    * </pre>
    *
    * However, due to https://github.com/moby/moby/issues/34584 ,
    * we should use "/" instead of the path in /proc/self/cgroup.
    *
    * On YARN, /proc/self/cgroup will look something like this:
    *
    * <pre>
    * 1:name=systemd:/system.slice/hadoop-nodemanager.service
    * 2:cpuacct,cpu:/hadoop-yarn/container_1555684705367_0008_01_000001
    * 3:devices:/system.slice/hadoop-nodemanager.service
    * 4:net_prio,net_cls:/
    * 5:hugetlb:/
    * 6:cpuset:/
    * 7:pids:/system.slice/hadoop-nodemanager.service
    * 8:memory:/hadoop-yarn/container_1555684705367_0008_01_000001
    * 9:perf_event:/
    * 10:freezer:/
    * 11:blkio:/system.slice/hadoop-nodemanager.service
    * </pre>
    *
    * @param cgroupType "memory", "cpuacct,cpu", etc.
    * @return
    */
  private[spark] def findCgroupPath(cgroupType: String): Option[String] = {
    try {
      val procSelfCgroup: BufferedSource = Source.fromFile("/proc/self/cgroup")
      try {
        var potentialCgroupPath: String = null

        for (line <- procSelfCgroup.getLines()) {
          s"^[0-9]+:$cgroupType:(/.*)$$".r.findAllIn(line).matchData foreach {
            m => potentialCgroupPath = m.group(1)
          }
        }

        if (potentialCgroupPath == null) {
          None
        } else if (Files.exists(Paths.get(s"/sys/fs/cgroup/$cgroupType$potentialCgroupPath"))) {
          Some(potentialCgroupPath)
        } else if (potentialCgroupPath.contains("/kubepods/")) {
          // Workaround for https://github.com/moby/moby/issues/34584
          Some("/")
        } else {
          None
        }
      } finally {
        procSelfCgroup.close()
      }
    } catch {
      case e: java.io.FileNotFoundException =>
        None
      case e: Exception =>
        logWarning(s"findCgroupPath(): Exception: ${ExceptionUtils.getStackTrace(e)}")
        None
    }
  }
}