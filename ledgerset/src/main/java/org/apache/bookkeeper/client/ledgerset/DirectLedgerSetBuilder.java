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
package org.apache.bookkeeper.client.ledgerset;

import org.apache.bookkeeper.statemachine.StateMachine;
import org.apache.bookkeeper.statemachine.StateMachine.Fsm;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.LedgerFuture;
import static org.apache.bookkeeper.client.HackAroundProtectionLevel.*;
import static org.apache.bookkeeper.client.ledgerset.Constants.*;
import org.apache.bookkeeper.client.LedgerSet.TailingReader;
import org.apache.bookkeeper.client.LedgerSet.Reader;
import org.apache.bookkeeper.client.LedgerSet.Writer;
import org.apache.bookkeeper.client.LedgerSet.Trimmer;
import org.apache.bookkeeper.client.LedgerSet.Builder;

import com.google.common.util.concurrent.SettableFuture;

public class DirectLedgerSetBuilder implements Builder {
    private ClientConfiguration conf = null;
    private BookKeeper bookkeeper = null;
    private MetadataStorage metadataStorage = null;
    private Fsm fsm = null;
    private BookKeeper.DigestType digestType = LEDGERSET_DIGEST_TYPE_DEFAULT;
    private String passwd = LEDGERSET_PASSWD_DEFAULT;
    private int ensemble = LEDGERSET_ENSEMBLE_SIZE_DEFAULT;
    private int writeQuorum = LEDGERSET_WRITE_QUORUM_DEFAULT;
    private int ackQuorum = LEDGERSET_ACK_QUORUM_DEFAULT;
    private int readBatchSize = LEDGERSET_READ_BATCH_SIZE_DEFAULT;

    @Override
    public Builder setDigestType(BookKeeper.DigestType t) {
        this.digestType = t;
        return this;
    }

    @Override
    public Builder setPassword(String password) {
        this.passwd = password;
        return this;
    }

    @Override
    public Builder setEnsemble(int ensemble) {
        this.ensemble = ensemble;
        return this;
    }

    @Override
    public Builder setWriteQuorum(int quorum) {
        this.writeQuorum = quorum;
        return this;
    }

    @Override
    public Builder setAckQuorum(int quorum) {
        this.ackQuorum = quorum;
        return this;
    }

    @Override
    public Builder setReadBatchSize(int readBatchSize) {
        this.readBatchSize = readBatchSize;
        return this;
    }

    @Override
    public Builder setConfiguration(ClientConfiguration conf) {
        this.conf = new ClientConfiguration(conf);
        return this;
    }

    @Override
    public Builder setClient(BookKeeper bookkeeper) {
        this.bookkeeper = bookkeeper;
        return this;
    }

    @Override
    public Builder setMetadataStorage(MetadataStorage metadataStorage) {
        this.metadataStorage = metadataStorage;
        return this;
    }

    public Builder setFsm(Fsm fsm) {
        this.fsm = fsm;
        return this;
    }

    private void addSettingsFromBuilder(ClientConfiguration conf) {
        if (!conf.containsKey(LEDGERSET_DIGEST_TYPE)) {
            conf.setProperty(LEDGERSET_DIGEST_TYPE, digestType);
        }
        if (!conf.containsKey(LEDGERSET_PASSWD)) {
            conf.setProperty(LEDGERSET_PASSWD, passwd);
        }
        if (!conf.containsKey(LEDGERSET_ENSEMBLE_SIZE)) {
            conf.setProperty(LEDGERSET_ENSEMBLE_SIZE, ensemble);
        }
        if (!conf.containsKey(LEDGERSET_ACK_QUORUM)) {
            conf.setProperty(LEDGERSET_ACK_QUORUM, ackQuorum);
        }
        if (!conf.containsKey(LEDGERSET_WRITE_QUORUM)) {
            conf.setProperty(LEDGERSET_WRITE_QUORUM, writeQuorum);
        }
        if (!conf.containsKey(LEDGERSET_READ_BATCH_SIZE)) {
            conf.setProperty(LEDGERSET_READ_BATCH_SIZE, readBatchSize);
        }
    }

    @Override
    public LedgerFuture<TailingReader> buildTailingReader(String setName) {
        if (bookkeeper == null) {
            throw new IllegalArgumentException("Must specify bookkeeper client");
        }
        ClientConfiguration conf = this.conf;
        if (conf == null) {
            conf = new ClientConfiguration(getConfiguration(bookkeeper));
        }
        addSettingsFromBuilder(conf);

        if (fsm == null) {
            fsm = new StateMachine.FsmImpl(getScheduler(bookkeeper));
        }

        DirectLedgerSet.TailingReader reader = new DirectLedgerSet.TailingReader(setName, conf,
                bookkeeper, getScheduler(bookkeeper), metadataStorage, fsm);
        final LedgerFuture<Void> openFuture = reader.open();
        return new ChainingLedgerFuture<TailingReader>(openFuture, reader);
    }

    @Override
    public LedgerFuture<Reader> buildFencingReader(String setName) {
        if (bookkeeper == null) {
            throw new IllegalArgumentException("Must specify bookkeeper client");
        }

        ClientConfiguration conf = this.conf;
        if (conf == null) {
            conf = new ClientConfiguration(getConfiguration(bookkeeper));
        }
        addSettingsFromBuilder(conf);

        if (fsm == null) {
            fsm = new StateMachine.FsmImpl(getScheduler(bookkeeper));
        }

        DirectLedgerSet.Reader reader
            = new DirectLedgerSet.Reader(setName, conf,
                    bookkeeper, getScheduler(bookkeeper), metadataStorage, fsm);
        final LedgerFuture<Void> openFuture = reader.open();
        return new ChainingLedgerFuture<Reader>(openFuture, reader);
    }

    @Override
    public LedgerFuture<Writer> buildWriter(String setName) {
        if (bookkeeper == null) {
            throw new IllegalArgumentException("Must specify bookkeeper client");
        }

        ClientConfiguration conf = this.conf;
        if (conf == null) {
            conf = new ClientConfiguration(getConfiguration(bookkeeper));
        }
        addSettingsFromBuilder(conf);

        if (fsm == null) {
            fsm = new StateMachine.FsmImpl(getScheduler(bookkeeper));
        }

        DirectLedgerSet.Writer writer
            = new DirectLedgerSet.Writer(setName, conf,
                    bookkeeper, getScheduler(bookkeeper), metadataStorage, fsm);
        SettableFuture<Writer> f = SettableFuture.<Writer>create();
        f.set(writer);
        return new ForwardingLedgerFuture<Writer>(f);
    }

    @Override
    public LedgerFuture<Trimmer> buildTrimmer(String setName) {
        if (bookkeeper == null) {
            throw new IllegalArgumentException("Must specify bookkeeper client");
        }

        ClientConfiguration conf = this.conf;
        if (conf == null) {
            conf = new ClientConfiguration(getConfiguration(bookkeeper));
        }
        addSettingsFromBuilder(conf);

        if (fsm == null) {
            fsm = new StateMachine.FsmImpl(getScheduler(bookkeeper));
        }

        DirectLedgerSet.Trimmer trimmer
            = new DirectLedgerSet.Trimmer(setName, conf,
                    bookkeeper, getScheduler(bookkeeper), metadataStorage, fsm);
        SettableFuture<Trimmer> f = SettableFuture.<Trimmer>create();
        f.set(trimmer);
        return new ForwardingLedgerFuture<Trimmer>(f);
    }
}
