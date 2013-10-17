/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.stats;

/**
 * An operation that takes an amount of time and then
 * finishes with a success or failure status.
 *
 * An example would be the servicing of a request on a server.
 * The OpTimer.Ctx object would be created before the request is serviced,
 * and when servicing is complete, either #failure() or #success() would
 * be called. The stats provider would then update the relevant statistic.
 */
public interface OpTimer {
    Ctx create();
    
    public interface Ctx {
        void success();
        void failure();
    }
}
