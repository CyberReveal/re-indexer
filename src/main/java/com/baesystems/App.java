/*
 * Software Copyright BAE Systems plc 2015. All Rights Reserved.
 * BAE SYSTEMS, DETICA and CYBERREVEAL are trademarks of BAE Systems
 * plc and may be registered in certain jurisdictions.
 */
package com.baesystems;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * The main class which run reindexing.
 */
public class App {

	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	@Parameter(names = { "-sh", "--src-host" }, description = "Coma separated host list of Elasticsearch src hosts.", required = true)
	private String srcHost;

	@Parameter(names = { "-dh", "--dst-host" }, description = "Coma separated host list of Elasticsearch dst hosts.", required = false)
	private String dstHost;

	@Parameter(names = { "-sd", "--start-date" }, description = "Start date from start reindexing in yyyyMMdd format (20150701)(Inclusive)")
	private String startDateString;

	@Parameter(names = { "-ed", "--end-date" }, description = "End date for reindexing in yyyyMMdd format (20150711)(Exclusive)")
	private String endDateString;

	@Parameter(names = { "-i", "--index" }, description = "Name of the index", required = true)
	private String index;

	@Parameter(names = { "-d", "--destination" }, description = "Name of the destination index", required = false)
	private String newIndex;

	@Parameter(names = { "-t", "--type" }, description = "Name of the type", required = true)
	private String type;

	@Parameter(names = { "-f", "--field" }, description = "Name of the field which is used for reindexing. Field has to be date type.", required = false)
	private String field;

	@Parameter(names = { "-bs", "--batch-size" }, description = "Batch size of how many document will be pulled from elasticsearch at given time")
	private int batchSize = 500;

	@Parameter(names = { "-tbs", "--temp-batch-size" }, description = "Temporal batch size in days")
	private int temporalBatchSize = 1;

	@Parameter(names = { "-cn", "--cluster-name" }, description = "Name of the Elasticsearch cluster", required = false)
	private String clusterName;

	/**
	 * Run re-indexing operation.
	 */
	public void run() {
		LOG.info("Started re-indexing process");

		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");

		DateTime startDate = null;
		DateTime endTime = null;

		if (this.field != null) {
			startDate = fmt.parseDateTime(this.startDateString);
			if (StringUtils.isBlank(this.endDateString)) {
				endTime = new DateTime().withTime(0, 0, 0, 0).plusDays(1);
			} else {
				endTime = fmt.parseDateTime(this.endDateString);
			}
		}

		Reindexer reindexer = null;
		if (dstHost != null) {
			reindexer = new InterClusterReindexer(this.index, this.type,
					this.field, this.batchSize, this.srcHost, this.dstHost);
		} else {
			String[] hosts = StringUtils.split(this.srcHost, ",");
			ClientManager manager = new ClientManager(this.clusterName, hosts);

			reindexer = new IntraClusterReindexer(this.index, this.type,
					this.field, this.batchSize, manager.getClient(),
					this.newIndex);
		}

		long numberOfDocumentInPeriod = reindexer.getDocumentNumberInPeriod(
				startDate, endTime);
		LOG.info("Founded {} items to reindex.", numberOfDocumentInPeriod);

		if (numberOfDocumentInPeriod == 0) {
			LOG.info("Re-index finished - no more documents.");
		}

		long startTime = System.currentTimeMillis();
		while (true) {
			DateTime beforeDate = null;

			if (field != null) {
				beforeDate = endTime.minusDays(temporalBatchSize);

				if (beforeDate.isBefore(startDate)) {
					beforeDate = startDate;
				}
			}

			reindexer.reindex(beforeDate, endTime);

			if (field != null) {
				endTime = beforeDate;
				if (beforeDate.equals(startDate)) {
					break;
				}
			} else {
				break;
			}
		}
		LOG.info("Completed in {}", new LocalTime(System.currentTimeMillis()
				- startTime));
	}

	/**
	 * Main method run application and parse the args.
	 * 
	 * @param args
	 *            the arguments
	 */
	public static void main(final String[] args) {

		App main = new App();

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
