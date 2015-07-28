/*
 * Software Copyright BAE Systems plc 2015. All Rights Reserved.
 * BAE SYSTEMS, DETICA and CYBERREVEAL are trademarks of BAE Systems
 * plc and may be registered in certain jurisdictions.
 */
package com.baesystems;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    @Parameter(names = { "-h", "--host" }, description = "Elasticsearch host.", required = true)
    private String host;

    @Parameter(names = { "-sd", "--start-date" }, description = "Start date from start reindexing in yyyyMMdd format (20150701)(Inclusive)", required = true)
    private String startDateString;

    @Parameter(names = { "-ed", "--end-date" }, description = "End date for reindexing in yyyyMMdd format (20150711)(Exclusive)")
    private String endDateString;

    @Parameter(names = { "-i", "--index" }, description = "Name of the index", required = true)
    private String index;

    @Parameter(names = { "-d", "--destination" }, description = "Name of the destination index", required = true)
    private String newIndex;

    @Parameter(names = { "-t", "--type" }, description = "Name of the type", required = true)
    private String type;

    @Parameter(names = { "-f", "--field" }, description = "Name of the field which is used for reindexing. Field has to be date type.", required = true)
    private String field;

    @Parameter(names = { "--batch-size" }, description = "Batch size of how many document will be pulled from elasticsearch at given time")
    private final int batchSize = 500;

    @Parameter(names = { "-cn", "--cluster-name" }, description = "Name of the Elasticsearch cluster", required = true)
    private String clusterName;

    /**
     * Run re-indexing operation.
     */
    public void run() {
        LOG.info("Started re-indexing process");

        ClientManager manager = new ClientManager(this.clusterName, this.host);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");

        DateTime startDate = fmt.parseDateTime(this.startDateString);
        DateTime endTime;
        if (StringUtils.isBlank(this.endDateString)) {
            endTime = new DateTime().withTime(0, 0, 0, 0).plusDays(1);
        } else {
            endTime = fmt.parseDateTime(this.endDateString);
        }

        Reindexer reindexer = new Reindexer(this.index, this.type, this.field, this.batchSize, manager.getClient());

        long numberOfDocumentInPeriod = reindexer.getDocumentNumberInPeriod(startDate, endTime);
        LOG.info("Founded {} items to reindex.", numberOfDocumentInPeriod);

        if (numberOfDocumentInPeriod == 0) {
            LOG.info("Re-index terminated due to lack of document to reindex.");
        }

        while (true) {
            DateTime beforeDate = endTime.minusDays(1);

            reindexer.reindex(beforeDate, endTime, this.newIndex);

            endTime = beforeDate;
            if (beforeDate.equals(startDate)) {
                break;
            }
        }

    }

    /**
     * Main method run application and parse the args.
     *
     * @param args the arguments
     */
    public static void main(final String[] args) {

        Main main = new Main();

        if (args.length > 0) {
            LOG.info("Parsing arguments");
            new JCommander(main, args);
            main.run();
            LOG.info("Reindexing finished");
        } else {
            LOG.error("There are missing arguments. Please look at the usage.");
            new JCommander(main).usage();
        }

    }
}
