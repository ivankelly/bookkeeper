/*
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
package org.apache.bookkeeper.benchmark.loadgen;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;


import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadGenerator {
    static final Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
    static final String THREADS_OPTION_NAME = "loadgen_threads";
    static final String PARTITIONS_OPTION_NAME = "loadgen_partitions";
    static final String RATE_OPTION_NAME = "loadgen_rate";
    static final byte[] MESSAGE = new byte[100];

    final ClusterState clusterState;
    List<LoadThread> loadThreads = new ArrayList<LoadThread>();
    final ClientConfiguration conf;

    LoadGenerator(ClientConfiguration conf) {
        this.conf = conf;
        clusterState = new ClusterState(conf.getInteger(PARTITIONS_OPTION_NAME, 10));
    }

    int getNumThreads() {
        return loadThreads.size();
    }

    void runThreads() throws InterruptedException, BKException, IOException, KeeperException {
        int numThreads = conf.getInteger(THREADS_OPTION_NAME, 10);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            LoadThread t = new LoadThread(conf);
            loadThreads.add(t);
            executor.submit(t);
        }
        rebalance();
        
        while (true) {
            Thread.sleep(1000);
        }
    }
    
    void rebalance() {
        int partitionsPerThread = (int)Math.ceil(clusterState.getNumPartitions()/getNumThreads());

        SortedSet<LoadThread> loadSet = new TreeSet<LoadThread>(new Comparator<LoadThread>() {
                @Override
                public int compare(LoadThread o1, LoadThread o2) {
                    return o2.getNumPartitions() - o1.getNumPartitions();
                }
            });

        SortedSet<Integer> allPartitions = new TreeSet<Integer>();
        for (int i = 0; i < clusterState.getNumPartitions(); i++) {
            allPartitions.add(i);
        }

        for (LoadThread t : loadThreads) {
            List<Integer> partitions = t.getPartitions();

            int overmax = partitions.size() - partitionsPerThread;
            for (int i = 0; i < overmax; i++) {
                t.dropPartition(partitions.remove(0));
            }

            allPartitions.removeAll(partitions);
        }

        while (allPartitions.size() > 0) {
            if (loadSet.size() == 0
                    || loadSet.first().getNumPartitions() >= partitionsPerThread) {
                LOG.error("Unable to fully balance, some partitions unassigned {}", allPartitions);
                return;
            }
            while (loadSet.first().getNumPartitions() < partitionsPerThread
                   && allPartitions.size() > 0) {
                loadSet.first().acquirePartition(allPartitions.first());
                allPartitions.remove(allPartitions.first());
            }
        }    
    }

    class LoadThread implements Runnable {
        Map<Integer, Partition> partitions = new HashMap<Integer, Partition>();
        List<Integer> partitionIds = new ArrayList<Integer>();
        IntegerDistribution integerGenerator;
        BookKeeper bk;
        AtomicBoolean killed = new AtomicBoolean(false);
        
        LoadThread(ClientConfiguration conf)
                throws IOException, BKException, InterruptedException, KeeperException {
            integerGenerator = new UniformIntegerDistribution(0, 0);
            bk = new BookKeeper(conf);
        }

        int getNumPartitions() {
            return partitionIds.size();
        }

        List<Integer> getPartitions() {
            return partitionIds;
        }

        synchronized void acquirePartition(int partition) {
            if (!partitionIds.contains(partition)) {
                partitionIds.add(partition);
            }
            integerGenerator = new UniformIntegerDistribution(0, 
                    Math.max(0, partitionIds.size()-1));
        }

        synchronized void dropPartition(int partition) {
            partitionIds.remove(partition);
            integerGenerator = new UniformIntegerDistribution(0, 
                    Math.max(0, partitionIds.size()-1));
            Partition p = partitions.get(partition);
            if (p != null) {
                p.kill();
            }
        }

        public void kill() throws InterruptedException, BKException {
            killed.set(true);
            bk.close();
        }

        @Override
        public void run() {
            while (!killed.get()) {
                if (partitionIds.size() == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                synchronized (this) {
                    if (partitionIds.size() == 0) {
                        continue;
                    }
                    int i = integerGenerator.sample();
                    int pid;

                    i = i % partitionIds.size();
                    pid = partitionIds.get(i);

                    Partition p = partitions.get(pid);
                    if (p == null) {
                        p = new Partition(bk, clusterState, pid);
                        partitions.put(pid, p);
                    }
                    try {
                        p.writeMessage(MESSAGE);
                    } catch (Exception e) {
                        partitions.remove(pid);
                    }
                }
            }
        }
    }

    public static void main(String[] args)
            throws ParseException, InterruptedException, BKException, IOException, KeeperException {
        Options options = new Options();
        options.addOption("threads", true, "Number of load threads to run. Each threads gets it's own bookkeeper client. Default 1.");
        options.addOption("partitions", true, "Number of partitions in the system. The partitions will be balanced between threads. Default 10.");
        options.addOption("zookeeper", true, "Zookeeper ensemble, Default \"localhost:2181\"");
        options.addOption("rate", true, "Rate that entries are added. This is per thread, so a rate of 1000 with 10 threads, will be 10,000 in total. Default is 1000RPS.");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help") || !cmd.hasOption("host")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("LoadGenerator <options>", options);
            System.exit(-1);
        }

        String zookeeper = cmd.getOptionValue("zookeeper", "localhost:2181");
        int threads = Integer.valueOf(cmd.getOptionValue("threads", "10"));
        int rate = Integer.valueOf(cmd.getOptionValue("rate", "1000"));
        int partitions = Integer.valueOf(cmd.getOptionValue("rate", "10"));

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zookeeper);
        conf.setProperty(THREADS_OPTION_NAME, threads);
        conf.setProperty(RATE_OPTION_NAME, rate);
        conf.setProperty(PARTITIONS_OPTION_NAME, partitions);

        new LoadGenerator(conf).runThreads();
    }
}
