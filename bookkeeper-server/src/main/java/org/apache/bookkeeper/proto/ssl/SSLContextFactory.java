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
package org.apache.bookkeeper.proto.ssl;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.KeyManagementException;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;

import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class SSLContextFactory {
    private final SSLContext ctx;
    private final boolean isClient;

    public SSLContextFactory(ServerConfiguration conf) throws FileNotFoundException, IOException {
        isClient = false;

        try {
            // Load our Java key store.
            KeyStore ks = KeyStore.getInstance("pkcs12");
            String sslKeyStore = conf.getSSLKeyStore();
            InputStream certStream = getClass().getResourceAsStream(sslKeyStore);
            if (certStream == null) {
                certStream = new FileInputStream(sslKeyStore);
            }
            ks.load(certStream, conf.getSSLKeyStorePassword().toCharArray());

            // Like ssh-agent.
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, conf.getSSLKeyStorePassword().toCharArray());

            // Create the SSL context.
            ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), getTrustManagers(), null);
        } catch (KeyStoreException kse) {
            throw new RuntimeException("Standard keystore type missing", kse);
        } catch(NoSuchAlgorithmException nsae) {
            throw new RuntimeException("Standard algorithm missing", nsae);
        } catch (CertificateException ce) {
            throw new IOException("Unable to load keystore", ce);
        } catch (UnrecoverableKeyException uke) {
            throw new IOException("Unable to load key manager, possibly wrong password given", uke);
        } catch (KeyManagementException kme) {
            throw new IOException("Error initializing SSLContext", kme);
        }
    }

    public SSLContextFactory(ClientConfiguration conf) throws IOException {
        isClient = true;
        // Create the SSL context.
        try {
            ctx = SSLContext.getInstance("TLS");
            ctx.init(null, getTrustManagers(), null);
        } catch(NoSuchAlgorithmException nsae) {
            throw new RuntimeException("Standard algorithm missing", nsae);
        } catch (KeyManagementException kme) {
            throw new IOException("Error initializing SSLContext", kme);
        }
    }

    public SSLContext getContext() {
        return ctx;
    }

    public SSLEngine getEngine() {
        SSLEngine engine = ctx.createSSLEngine();
        engine.setUseClientMode(isClient);
        return engine;
    }

    protected TrustManager[] getTrustManagers() {
        return new TrustManager[] { new X509TrustManager() {
                // Always trust, even if invalid.

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                    // Always trust.
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                    // Always trust.
                }
            }
        };
    }

}
