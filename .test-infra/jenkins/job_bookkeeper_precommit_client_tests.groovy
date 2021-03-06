/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import common_job_properties

// test `org.apache.bookkeeper.client.**`
freeStyleJob('bookkeeper_precommit_client_tests') {
  description('Run bookkeeper client tests in Java 8.')

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    'JDK 1.8 (latest)',
    200,
    'ubuntu',
    '${ghprbActualCommit}')

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(
    delegate,
    'Client Tests',
    '.*(re)?run bookkeeper-server (client )?tests.*',
    '.*\\[x\\] \\[skip bookkeeper-server (client )?tests\\].*',
    true)

  steps {
    // Temporary information gathering to see if full disks are causing the builds to flake
    shell("id")
    shell("ulimit -a")
    shell("pwd")
    shell("df -h")
    shell("ps aux")

    // Build everything
    maven {
      // Set Maven parameters.
      common_job_properties.setMavenConfig(delegate)

      goals('-B -am -pl bookkeeper-server clean install -DskipTests')
    }

    // Test the package
    maven {
      // Set Maven parameters.
      common_job_properties.setMavenConfig(delegate)

      goals('-pl bookkeeper-server test -Dtest="org.apache.bookkeeper.client.**"')
    }
  }

}
