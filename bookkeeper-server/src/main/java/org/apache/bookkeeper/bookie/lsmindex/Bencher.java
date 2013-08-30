package org.apache.bookkeeper.bookie.lsmindex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import com.google.protobuf.ByteString;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import java.util.Comparator;

import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.OpTimer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;

public class Bencher {
    private final static Logger LOG = LoggerFactory.getLogger(Bencher.class);

    static Comparator<ByteString> keyComp = new Comparator<ByteString>() {
        Comparator<byte[]> byteComp = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(ByteString k1, ByteString k2) {
            return byteComp.compare(k1.toByteArray(), k2.toByteArray());
        }
    };

    public static void main(String args[]) throws Exception {
        Configuration conf = new BaseConfiguration();
        conf.setProperty("statsProviderClass","org.apache.bookkeeper.stats.CodahaleMetricsProvider");
        conf.setProperty("codahale_stats_name","bench");

        Stats.init(conf);

        DB db = new DB(keyComp, new File(args[0]));
        db.start();
        int i = 0;
        OpTimer op = Stats.get().getOpTimer(Bencher.class.getName(), "put");
        while (true) {
            i++;
            ByteString key = ByteString.copyFrom(Ints.toByteArray(i));
            ByteString value = ByteString.copyFromUtf8("ValueFor"+i);
            OpTimer.Ctx ctx = op.create();
            db.put(key, value);
            ctx.success();
        }
    }
}
