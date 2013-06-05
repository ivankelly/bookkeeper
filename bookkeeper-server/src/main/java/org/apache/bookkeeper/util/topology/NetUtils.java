/**
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
// This code has been copied from hadoop-common 2.0.4-alpha
package org.apache.bookkeeper.util.topology;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.NoRouteToHostException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class NetUtils {

  private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);

  /**
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   *
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  public static String normalizeHostName(String name) {
    try {
      return InetAddress.getByName(name).getHostAddress();
    } catch (UnknownHostException e) {
      return name;
    }
  }

  /**
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   *
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  public static List<String> normalizeHostNames(Collection<String> names) {
    List<String> hostNames = new ArrayList<String>(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }
}
