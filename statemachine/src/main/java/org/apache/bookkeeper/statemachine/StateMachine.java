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

import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Queue;
import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateMachine {
    static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);
    static final String HANDLER_METHOD_NAME = "handleEvent";

    private static ConcurrentHashMap<Class<?>, ConcurrentHashMap<Class<?>, Method>> stateCaches
        = new ConcurrentHashMap<Class<?>, ConcurrentHashMap<Class<?>, Method>>();

    public static abstract class State {
        protected final Fsm fsm;
        private final ConcurrentHashMap<Class<?>, Method> handlerCache;

        public State(Fsm fsm) {
            this.fsm = fsm;

            ConcurrentHashMap<Class<?>, Method> handlerCache = stateCaches.get(getClass());
            if (handlerCache == null) {
                handlerCache = new ConcurrentHashMap<Class<?>, Method>();
                ConcurrentHashMap<Class<?>, Method> old = stateCaches.putIfAbsent(getClass(),
                                                                                  handlerCache);
                if (old != null) {
                    handlerCache = old;
                }
            }
            this.handlerCache = handlerCache;
        }

        private Method findHandlerInternal(Class<?> state, Class<?> e)
                throws NoSuchMethodException {
            Method[] methods = state.getMethods();
            List<Method> candidates = new ArrayList<Method>();
            for (Method m : methods) {
                if (m.getName().equals(HANDLER_METHOD_NAME)
                    && State.class.isAssignableFrom(m.getReturnType())
                    && m.getGenericParameterTypes().length == 1) {
                    candidates.add(m);
                }
            }

            Class<?> eventType = e;
            Method best = null;
            for (Method m : candidates) {
                if (m.getParameterTypes()[0].isAssignableFrom(eventType)) {
                    if (best == null) {
                        best = m;
                    } else if (best.getParameterTypes()[0]
                               .isAssignableFrom(m.getParameterTypes()[0])) {
                        best = m;
                    }
                }
            }
            if (best != null) {
                best.setAccessible(true);
                return best;
            }
            throw new NoSuchMethodException("Handler doesn't exist");
        }

        private Method findHandler(Class<?> event)
                throws NoSuchMethodException {
            Method m = handlerCache.get(event);
            if (m == null) {
                m = findHandlerInternal(getClass(), event);
                Method m2 = handlerCache.putIfAbsent(event, m);
                if (m2 != null) {
                    m = m2;
                }
            }
            return m;
        }

        State dispatch(Event e)
                throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            return (State)findHandler(e.getClass()).invoke(this, e);
        }
    }

    public interface Event { }

    public interface Fsm {
        Fsm newChildFsm();
        void setInitState(State initState);
        void sendEvent(Event e);
        Future<?> sendEvent(Event e, long delay, TimeUnit unit);
        void deferEvent(DeferrableEvent e);
    }

    public static class FsmImpl implements Fsm {
        ScheduledExecutorService executor;
        private State state;
        private Queue<DeferrableEvent> deferred;

        public FsmImpl(ScheduledExecutorService executor) {
            this.executor = executor;
            state = null;
            deferred = new ArrayDeque<DeferrableEvent>();
        }

        private void errorDeferredEvents(Throwable t) {
            Queue<DeferrableEvent> oldDeferred = deferred;
            deferred = new ArrayDeque<DeferrableEvent>();

            for (DeferrableEvent e : oldDeferred) {
                e.error(new IllegalStateException(t));
            }
        }

        @Override
        public Fsm newChildFsm() {
            return new FsmImpl(executor);
        }

        @Override
        public void setInitState(State initState) {
            assert (state == null);
            state = initState;
        }

        public State getState() {
            return state;
        }

        protected void setState(final State curState, final State newState) {
            if (curState != state) {
                LOG.error("FSM-{}: Tried to transition from {} to {}, but current state is {}",
                        new Object[] { getFsmId(), state, newState, curState });
                throw new IllegalArgumentException();
            }
            state = newState;

            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: State transition {} -> {}",
                        new Object[] { getFsmId(), curState, newState });
            }
        }

        protected boolean processEvent(Event e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: Received event {}@{} in state {}@{}",
                          new Object[] { getFsmId(), e.getClass().getSimpleName(),
                                         System.identityHashCode(e),
                                         state.getClass().getSimpleName(),
                                         System.identityHashCode(state) });
            }
            try {
                State newState = state.dispatch(e);

                if (newState != state) {
                    setState(state, newState);
                    return true;
                }
            } catch (Throwable t) {
                LOG.error("Caught throwable while handling event", t);
                errorDeferredEvents(t);
            }
            return false;
        }

        class FSMRunnable implements Runnable {
            final Event e;

            FSMRunnable(Event e) {
                this.e = e;
            }

            @Override
            public void run() {
                boolean stateChanged = processEvent(e);
                while (stateChanged) {
                    stateChanged = false;
                    Queue<DeferrableEvent> prevDeferred = deferred;
                    deferred = new ArrayDeque<DeferrableEvent>();
                    for (DeferrableEvent d : prevDeferred) {
                        if (stateChanged) {
                            deferred.add(d);
                        } else if (processEvent(d)) {
                            stateChanged = true;
                        }
                    }
                }
            }
        }

        @Override
        public void sendEvent(final Event e) {
            executor.submit(new FSMRunnable(e));
        }

        @Override
        public Future<?> sendEvent(final Event e, final long delay, final TimeUnit unit) {
            return executor.schedule(new FSMRunnable(e), delay, unit);
        }

        @Override
        public void deferEvent(DeferrableEvent e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("FSM-{}: deferred {}@{}",
                          new Object[] { getFsmId(), e.getClass().getSimpleName(),
                                         System.identityHashCode(e) });
            }
            deferred.add(e);
        }

        int getFsmId() {
            return System.identityHashCode(this);
        }

        @Override
        public void finalize() {
            LOG.debug("FSM-{}: Finalizing", getFsmId());
        }
    }

    public static interface DeferrableEvent extends Event {
        public void error(Throwable exception);
    }
}
