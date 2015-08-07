package com.baesystems;

import org.joda.time.DateTime;

public interface Reindexer {
	static final String TIMESTAMP_FIELD = "_timestamp";
	static final String SOURCE_FIELD = "_source";
	static final String PARENT_FIELD = "_parent";
	static final int TIMEOUT = 60000;

	long getDocumentNumberInPeriod(final DateTime from, final DateTime to);

	void reindex(final DateTime from, final DateTime to);
}
