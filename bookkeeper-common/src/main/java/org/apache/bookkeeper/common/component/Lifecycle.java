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

package org.apache.bookkeeper.common.component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Lifecycle state. Allows the following transitions:
 *
 * <ul>
 * <li>INITIALIZED -&gt; STARTED, STOPPED, CLOSED</li>
 * <li>STARTED     -&gt; STOPPED</li>
 * <li>STOPPED     -&gt; STARTED, CLOSED</li>
 * <li>CLOSED      -&gt; </li>
 * </ul>
 *
 * <p>Also allows to stay in the same state. For example, when calling stop on a component, the
 * following logic can be applied:
 *
 * <pre>
 * public void stop() {
 *  if (!lifecycleState.moveToStopped()) {
 *      return;
 *  }
 * // continue with stop logic
 * }
 * </pre>
 *
 * <p>Note, closed is only allowed to be called when stopped, so make sure to stop the component first.
 * Here is how the logic can be applied:
 *
 * <pre>
 * public void close() {
 *  if (lifecycleState.started()) {
 *      stop();
 *  }
 *  if (!lifecycleState.moveToClosed()) {
 *      return;
 *  }
 *  // perform close logic here
 * }
 * </pre>
 */
public class Lifecycle {

    /**
     * Lifecycle State.
     */
    public enum State {
        INITIALIZED,
        STOPPED,
        STARTED,
        CLOSED
    }

    private AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);

    public State state() {
        return this.state.get();
    }

    /**
     * Returns <tt>true</tt> if the state is initialized.
     */
    public boolean initialized() {
        return state.get() == State.INITIALIZED;
    }

    /**
     * Returns <tt>true</tt> if the state is started.
     */
    public boolean started() {
        return state.get() == State.STARTED;
    }

    /**
     * Returns <tt>true</tt> if the state is stopped.
     */
    public boolean stopped() {
        return state.get() == State.STOPPED;
    }

    /**
     * Returns <tt>true</tt> if the state is closed.
     */
    public boolean closed() {
        return state.get() == State.CLOSED;
    }

    public boolean stoppedOrClosed() {
        Lifecycle.State state = this.state.get();
        return state == State.STOPPED || state == State.CLOSED;
    }

    public boolean moveToStarted() throws IllegalStateException {
        State localState = this.state.get();
        while (localState == State.INITIALIZED || localState == State.STOPPED) {
            if (state.compareAndSet(localState, State.STARTED)) {
                return true;
            } else {
                localState = this.state.get();
            }
        }
        if (localState == State.STARTED) {
            return false;
        }
        if (localState == State.CLOSED) {
            throw new IllegalStateException("Can't move to started state when closed");
        }
        throw new IllegalStateException("Can't move to started with unknown state");
    }

    public boolean moveToStopped() throws IllegalStateException {
        State localState = state.get();
        while (localState == State.STARTED) {
            if (state.compareAndSet(localState, State.STOPPED)) {
                return true;
            } else {
                localState = state.get();
            }
        }
        if (localState == State.INITIALIZED
            || localState == State.STOPPED
            || localState == State.CLOSED) {
            return false;
        }
        throw new IllegalStateException("Can't move to stopped with unknown state");
    }

    public boolean moveToClosed() throws IllegalStateException {
        State localState = state.get();
        while (true) {
            if (localState == State.CLOSED) {
                return false;
            } else if (localState == State.STARTED) {
                throw new IllegalStateException("Can't move to closed before moving to stopped mode");
            }
            if (state.compareAndSet(localState, State.CLOSED)) {
                return true;
            } else {
                localState = state.get();
            }
        }
    }

    @Override
    public String toString() {
        return state.toString();
    }

}
