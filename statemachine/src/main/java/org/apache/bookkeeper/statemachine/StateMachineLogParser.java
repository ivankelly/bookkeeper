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
package org.apache.bookkeeper.statemachine;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StateMachineLogParser {

    static Pattern fsmPattern = Pattern.compile("FSM-(\\d+):");
    static Map<String, List<String>> getFsmEventMap(BufferedReader f) throws IOException {
        String s = f.readLine();
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        while (s != null) {
            Matcher m = fsmPattern.matcher(s);
            if (m.find()) {
                String key = m.group(1);
                if (!map.containsKey(key)) {
                    map.put(key, new ArrayList<String>());
                }
                map.get(key).add(s);
            }
            s = f.readLine();
        }
        return map;
    }

    static class Tuple {
        final String state1;
        final String state2;
        final String event;

        Tuple(String state1, String event, String state2) {
            this.state1 = state1;
            this.state2 = state2;
            this.event = event;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Tuple) {
                Tuple t = (Tuple)o;
                return state1.equals(t.state1)
                    && state2.equals(t.state2)
                    && event.equals(t.event);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (state1 + event + state2).hashCode();
        }

        @Override
        public String toString() {
            return state1 + " + " + event + " = " + state2;
        }
    }

    static Pattern hashcodePattern = Pattern.compile("(.+)@(.+)");
    static Pattern subclassPattern = Pattern.compile("(.+)\\$(.+)");
    static String cleanupState(String state) {
        Matcher m = hashcodePattern.matcher(state);
        if (m.find()) {
            state = m.group(1);
        }
        m = subclassPattern.matcher(state);
        if (m.find()) {
            state = m.group(2);
        }
        return state;
    }

    static Pattern eventPattern = Pattern.compile("Received event (.+)@\\d+ in state (.+)@\\d+");
    static Pattern transitionPattern = Pattern.compile("State transition (.+) -> (.+)");
    static Set<Tuple> getStateTuples(Map<String, List<String>> fsms) {
        Set<Tuple> tuples = new HashSet<Tuple>();
        for (List<String> transitions : fsms.values()) {
            String currentEvent = null;
            for (String s : transitions) {
                Matcher m = eventPattern.matcher(s);
                if (m.find()) {
                    currentEvent = m.group(1);
                    continue;
                }
                m = transitionPattern.matcher(s);
                if (m.find()) {
                    if (currentEvent == null) {
                        System.err.println("event is null");
                    }
                    String state1 = m.group(1);
                    String state2 = m.group(2);
                    tuples.add(new Tuple(cleanupState(state1),
                                         currentEvent,
                                         cleanupState(state2)));
                    continue;
                }
                if (s.contains("deferred")) {
                    currentEvent = currentEvent + "[deferred]";
                }
            }
        }
        return tuples;
    }

    static Set<Tuple> reachableFromInitState(Set<Tuple> tuples, String initState) {
        Set<Tuple> newtuples = new HashSet<Tuple>();
        Set<String> processed = new HashSet<String>();

        Map<String, List<Tuple>> graph = new HashMap<>();
        for (Tuple t : tuples) {
            if (!graph.containsKey(t.state1)) {
                graph.put(t.state1, new ArrayList<Tuple>());
            }
            graph.get(t.state1).add(t);
        }
        Queue<String> next = new ArrayDeque<String>();
        next.add(initState);
        while (next.size() > 0) {
            String node = next.remove();
            List<Tuple> outEdges = graph.get(node);
            if (!processed.contains(node) && outEdges != null) {
                for (Tuple t : outEdges) {
                    newtuples.add(t);
                    next.add(t.state2);
                }
                processed.add(node);
            }
        }
        return newtuples;
    }

    static void drawDotGraph(Set<Tuple> tuples) {
        Set<String> states = new HashSet<String>();
        for (Tuple t : tuples) {
            states.add(t.state1);
            states.add(t.state2);
        }

        System.out.println("digraph finite_state_machine {");
        for (String s : states) {
            System.out.println("\tnode [ shape = box ] " + s + ";");
        }
        for (Tuple t : tuples) {
            System.out.println("\t" + t.state1 + " -> " + t.state2 + " [ label = \"" + t.event + "\" ];");
        }
        System.out.println("}");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: StateMachineLogParser <logfile> [initstate] | dot -Tsvg ");
            System.exit(-1);
        }
        BufferedReader f = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        Map<String,List<String>> fsms = getFsmEventMap(f);
        Set<Tuple> tuples = getStateTuples(fsms);
        if (args.length == 2) {
            tuples = reachableFromInitState(tuples, args[1]);
        }
        drawDotGraph(tuples);
    }
}
