package org.apache.bookkeeper.tests

import java.util.concurrent.CompletableFuture

import org.junit.Assert
import org.junit.Test
import org.junit.AfterClass
import org.junit.runner.RunWith

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.Container
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.async.ResultCallback

import org.jboss.arquillian.test.api.ArquillianResource
import org.jboss.arquillian.junit.Arquillian

import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.Watcher.Event.KeeperState

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container as Container2
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Network
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerNetwork

import org.jboss.arquillian.core.api.Instance;

import org.jboss.arquillian.core.api.annotation.Inject;
import org.jboss.arquillian.core.api.Injector;

import org.arquillian.cube.docker.impl.client.config.Await

import java.io.File


@RunWith(Arquillian.class)
class TestCompatUpgrade {
    private static final Logger LOG = LoggerFactory.getLogger(TestCompatUpgrade.class)
    private static byte[] PASSWD = "foobar".getBytes()

    @ArquillianResource
    DockerClient docker

    private void testUpgrade(String zookeeper, String currentlyRunning, String upgradeTo) {
        LOG.info("Upgrading from {} to {}", currentlyRunning, upgradeTo)
        int numEntries = 10
        def currentRunningCL = MavenClassLoader.forBookKeeperVersion(currentlyRunning)

        def currentRunningBK = currentRunningCL.newBookKeeper(zookeeper)

        def ledger0 = currentRunningBK.createLedger(3, 2,
                                                    currentRunningCL.digestType("CRC32"),
                                                    PASSWD)
        for (int i = 0; i < numEntries; i++) {
            ledger0.addEntry(("foobar" + i).getBytes())
        }
        ledger0.close()

        // Check that current client can not write to old server
        /*def upgradedCL = MavenClassLoader.forBookKeeperVersion(upgradeTo)
        def upgradedBK = upgradedCL.newBookKeeper(zookeeper)
        def ledger1 = upgradedBK.createLedger(3, 2, upgradedCL.digestType("CRC32"), PASSWD)
        try {
            ledger1.addEntry("foobar".getBytes())
            Assert.fail("Shouldn't have been able to write")
        } catch (Exception e) {
            // correct behaviour
        }*/

        /*if (!BookKeeperClusterUtils.stopAllBookies(docker)) {
            File f = new File("/tmp/pause")
            f.createNewFile()
            while (f.exists()) { Thread.sleep(1000) }
        }*/
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, upgradeTo));

        // check that old client can read its old ledgers on new server
        def ledger2 = currentRunningBK.openLedger(ledger0.getId(), currentRunningCL.digestType("CRC32"),
                                                  PASSWD)
        Assert.assertEquals(numEntries, ledger2.getLastAddConfirmed() + 1 /* counts from 0 */)
        def entries = ledger2.readEntries(0, ledger2.getLastAddConfirmed())
        int j = 0
        while (entries.hasMoreElements()) {
            def e = entries.nextElement()
            Assert.assertEquals(new String(e.getEntry()), "foobar"+ j)
            j++
        }
        ledger2.close()

        currentRunningBK.close()
    }

    @Test
    public void testCompat410() throws Exception {
        BookKeeperClusterUtils.legacyMetadataFormat(docker)
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker)

        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.1.0"))
        testUpgrade(zookeeper, "4.1.0", "4.2.0")
        testUpgrade(zookeeper, "4.2.0", "4.2.1")
        testUpgrade(zookeeper, "4.2.1", "4.2.2")
        testUpgrade(zookeeper, "4.2.2", "4.2.3")
        testUpgrade(zookeeper, "4.2.3", "4.2.4")
        testUpgrade(zookeeper, "4.2.4", "4.3.0")
        testUpgrade(zookeeper, "4.3.0", "4.3.1")
        testUpgrade(zookeeper, "4.3.1", "4.3.2")
        testUpgrade(zookeeper, "4.3.2", "4.4.0")
        testUpgrade(zookeeper, "4.4.0", "4.5.0")
        testUpgrade(zookeeper, "4.5.0", "4.5.1")
        testUpgrade(zookeeper, "4.5.1", "4.6.0")
        testUpgrade(zookeeper, "4.6.0", System.getProperty("currentVersion"))
    }

}
