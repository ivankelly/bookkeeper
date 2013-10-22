package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.stats.Meter;
import org.apache.bookkeeper.stats.OpTimer;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import com.google.common.io.Closeables;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtil {
    private final static Logger LOG
        = LoggerFactory.getLogger(DBUtil.class);

    public static void main(String[] args) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setProperty("statsProviderClass","org.apache.bookkeeper.stats.CodahaleMetricsProvider");
        conf.setProperty("codahale_stats_name", "lsmstats");
        conf.setProperty("codahale_stats_frequency_s", 5);
        //codahale_stats_graphite_host={{ graphite }}

        Options options = new Options();
        options.addOption("directory", true, "Database directory");
        options.addOption("dumpManifest", false, "Dump the manifest");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchSSTable <options>", options);
            System.exit(-1);
        }

        Stats.init(conf);

        if (cmd.hasOption("dumpManifest")) {
            Comparator<ByteString> keyComp = KeyComparators.unsignedLexicographical();
            Manifest manifest = new Manifest(keyComp, new File(cmd.getOptionValue("directory")));
            manifest.dumpManifest();
        }
    }
}
