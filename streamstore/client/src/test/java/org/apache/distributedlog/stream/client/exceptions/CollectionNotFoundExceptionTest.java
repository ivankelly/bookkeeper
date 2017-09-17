/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.client.exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test Case of {@link CollectionNotFoundException}.
 */
public class CollectionNotFoundExceptionTest {

  @Rule
  public TestName name = new TestName();

  @Test
  public void testGetMessage() {
    CollectionNotFoundException cnfe = new CollectionNotFoundException(name.getMethodName());
    assertEquals("Collection '" + name.getMethodName() + "' is not found", cnfe.getMessage());
    assertNull(cnfe.getCause());
  }

}
