package com.baesystems

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.HttpResponseDecorator
import groovyx.net.http.Method

import org.apache.http.util.EntityUtils
import org.elasticsearch.index.query.BaseQueryBuilder
import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.QueryBuilders
import org.joda.time.DateTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Similar to {@link IntraClusterReindexer} but works between clusters - useful when performing an ES upgrade from a pre 1.3 index
 */
class InterClusterReindexer implements Reindexer {
	private static final String TIMESTAMP_FIELD = '_timestamp'

	private static final String SOURCE_FIELD = '_source'

	private static final String PARENT_FIELD = '_parent'

	private static final String TIMEOUT = '10m'

	private static final Logger LOG = LoggerFactory.getLogger(InterClusterReindexer.class)

	private final HTTPBuilder srcHttp
	private final HTTPBuilder dstHttp

	private final String index
	private final String type
	private final String field
	private final int batchSize

	private final JsonSlurper jsonSlurper
	private final JsonBuilder jsonBuilder

	/**
	 * Instantiates a new reindexer.
	 *
	 * @param index Elasticsearch source index
	 * @param type document type
	 * @param field the field which is used for re-index
	 * @param batchSize number of document pulled from ES in batch
	 * @param srcNode - Source node address (http://localhost:9200)
	 * @param dstNode - Destination node address (http://localhost:9201)
	 */
	public InterClusterReindexer(final String index, final String type, final String field, final int batchSize, final String srcNode, String dstNode) {
		this.srcHttp = new HTTPBuilder(srcNode)
		this.dstHttp = new HTTPBuilder(dstNode)
		this.index = index
		this.type = type
		this.field = field
		this.batchSize = batchSize

		this.jsonSlurper = new JsonSlurper()
		this.jsonBuilder = new JsonBuilder()
	}

	/**
	 * Returns number of documents in given time. NOTE 'to' date is exclusive.
	 *
	 * @param from the from
	 * @param to the to
	 * @return the document number in period
	 */
	public long getDocumentNumberInPeriod(final DateTime from, final DateTime to) {
		BaseQueryBuilder queryBuilder = null

		if (field) {
			queryBuilder = QueryBuilders.rangeQuery(this.field).gte(from).lt(to)
		} else {
			queryBuilder = QueryBuilders.matchAllQuery()
		}

		this.srcHttp.request( Method.POST, ContentType.JSON ) { req ->
			uri.path = "$index/$type/_count"
			body = [ query : jsonSlurper.parseText(queryBuilder.toString()) ]

			response.success = { resp, json ->
				return json['count']
			}
		}
	}

	/**
	 * Copying data in batches from old index to new index.
	 *
	 * @param from Starting date
	 * @param to End date
	 */
	public void reindex(final DateTime from, final DateTime to) {

		LOG.info("Start re-indexing for data between {} and {}", from, to)

		BaseQueryBuilder queryBuilder = null
		if (field) {
			queryBuilder = QueryBuilders.filteredQuery(
					QueryBuilders.matchAllQuery(),
					FilterBuilders.andFilter(
					FilterBuilders.typeFilter(this.type),
					FilterBuilders.rangeFilter(this.field).gte(from).lt(to)))
		} else {
			queryBuilder = QueryBuilders.matchAllQuery()
		}

		String scrollId = null
		boolean scroll = true

		long totalDocs = 0
		long errorDocs = 0
		long start = System.currentTimeMillis()
		while (scroll) {
			srcHttp.request( Method.POST, ContentType.JSON ) { req ->
				uri.path = scrollId ? "/_search/scroll" : "/$index/$type/_search"
				uri.query = [ 'scroll' : TIMEOUT ]
				if (!scrollId) {
					uri.query['search_type'] = 'scan'
				}

				if (!scrollId) {
					body = [
						fields : [PARENT_FIELD, SOURCE_FIELD, TIMESTAMP_FIELD],
						query : jsonSlurper.parseText(queryBuilder.toString()),
						size : this.batchSize
					]
				} else {
					body = scrollId
				}

				response.success = { resp, json ->
					if (scrollId) {
						scroll = json.hits.hits
					} else {
						scroll = json.hits.total > 0
					}

					scrollId = json._scroll_id

					// process hits
					if (json.hits.hits) {
						StringBuilder bulkActions = new StringBuilder(10*1024*1024)
						json.hits.hits.each { hit ->
							Map action = [ create : [ _index : this.index, _type : this.type, _id : hit._id ]]
							if (hit.fields) {
								action.create << hit.fields
							}
							jsonBuilder.call(action)
							bulkActions.append(jsonBuilder.toString())
							bulkActions.append('\n')

							jsonBuilder.call(hit._source)
							bulkActions.append(jsonBuilder.toString())
							bulkActions.append('\n')
						}

						int batchDocs = 0
						int batchErrs = 0
						dstHttp.request( Method.POST, ContentType.JSON ) { dstReq ->
							uri.path = '_bulk'
							body = bulkActions.toString()

							response.success = { dstResp, dstJson ->
								dstJson.items.each { item ->
									if (item.create.error) {
										LOG.error('Error for document:{} : {}', item.create._id, item.create.error)
										batchErrs++
									} else {
										batchDocs++
									}
								}
							}

							response.failure = { dstResp ->
								LOG.error('Error while pushing current document batch: {}', EntityUtils.toString(dstResp.entity))
								throw new IOException('Error while pushing current documents')
							}
						}

						LOG.info("This batch inserted {} documents and had {} failures.", batchDocs, batchErrs)
						totalDocs += batchDocs
						errorDocs += batchErrs
					}
				}

				response.failure = { HttpResponseDecorator resp ->
					LOG.error('Error while pulling current documents: {}', EntityUtils.toString(resp.entity))
					throw new IOException('Error while pulling current documents')
				}
			}
		}
		long end = System.currentTimeMillis()
		long totalTime = ((end - start) / 1000)+1

		LOG.info("Inserted {} documents in {} secs ({} docs/s)", totalDocs, totalTime, (long) (totalDocs / totalTime))
		if (errorDocs) {
			LOG.error("{} documents failed inserion", errorDocs)
		}
	}
}
