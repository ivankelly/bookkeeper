package org.apache.bookkeeper.stats;

import org.apache.bookkeeper.conf.AbstractConfiguration;

public class Stats {
    static TwitterStatsProvider prov = new TwitterStatsProvider();

    public static StatsProvider get() {
        prov.init();
        return prov;
    }
}
