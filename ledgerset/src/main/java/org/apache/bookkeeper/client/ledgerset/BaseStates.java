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
package org.apache.bookkeeper.client.ledgerset;

import org.apache.bookkeeper.statemachine.StateMachine.Event;
import org.apache.bookkeeper.statemachine.StateMachine.DeferrableEvent;
import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.apache.bookkeeper.statemachine.StateMachine.State;

import org.apache.bookkeeper.client.ledgerset.Contexts.*;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseStates {
    static final Logger LOG = LoggerFactory.getLogger(BaseStates.class);

    static class BaseState<T extends Context> extends State {
        final T ctx;

        BaseState(Fsm fsm, T ctx) {
            super(fsm);
            this.ctx = ctx;
        }

        public State handleEvent(UserEvent<?> e) {
            e.error(new IllegalStateException("Unhandled event " + e.getClass().getSimpleName()
                                               + " in state " + this));
            return this;
        }

        public final State handleEvent(Event e) {
            LOG.error("Unhandled event {} for state {}, ignoring", e, this);
            return this;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName().replace("State", "");
        }
    }

    static class ErrorState extends State {
        final Throwable exception;

        ErrorState(Fsm fsm, Throwable exception) {
            super(fsm);
            this.exception = exception;
        }

        public State handleEvent(DeferrableEvent e) {
            e.error(exception);
            return this;
        }

        public State handleEvent(Event e) {
            LOG.debug("Received event {} in state {}, ignoring", e, this);
            return this;
        }
    }

    static class DeferringState<T extends Context> extends BaseState<T> {
        DeferringState(Fsm fsm, T ctx) {
            super(fsm, ctx);
        }

        // Think harder about how we could not have this class
        public State handleEvent(UserEvent<?> e) {
            fsm.deferEvent(e);
            return this;
        }

        public State handleEvent(DeferrableEvent e) {
            fsm.deferEvent(e);
            return this;
        }
    }

    static abstract class ReaderBaseState<T extends Context> extends BaseState<T> {
        ReaderBaseState(Fsm fsm, T ctx) {
            super(fsm, ctx);
        }

        public abstract State handleEvent(SkipEvent e);
        public abstract State handleEvent(HasMoreEvent e);
        public abstract State handleEvent(ReadEntryEvent e);
        public abstract State handleEvent(WaitMoreEvent e);
        public abstract State handleEvent(CloseEvent e);
    }

    static abstract class WriterBaseState<T extends Context> extends BaseState<T> {
        WriterBaseState(Fsm fsm, T ctx) {
            super(fsm, ctx);
        }
        public abstract State handleEvent(ResumeEvent e);
        public abstract State handleEvent(WriteEntryEvent e);
        public abstract State handleEvent(CloseEvent e);
    }

    static abstract class TrimmerBaseState<T extends Context> extends BaseState<T> {
        TrimmerBaseState(Fsm fsm, T ctx) {
            super(fsm, ctx);
        }
        public abstract State handleEvent(TrimEvent e);
        public abstract State handleEvent(CloseEvent e);
    }
}
