package bskyscraper;


import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.ingest.GetPipelineResponse;
import co.elastic.clients.elasticsearch.ingest.PutPipelineResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ElasticSearchManager {

    private ElasticsearchClient esClient;
    private final static Logger logger = LoggerFactory.getLogger(ElasticSearchManager.class);
    private final static String indexName = "events";
    private final static String ingestPipelineName = "add_timestamp";

    public ElasticSearchManager() {
        var restClient = RestClient
            .builder(HttpHost.create(Constants.esServerUrl))
            .setDefaultHeaders(new Header[]{
                new BasicHeader("Authorization", "ApiKey " + Constants.esApiKey)
            })
            .build();
        var esTransport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(esTransport);
    }

    public void createIndex() {
        try {
            CreateIndexResponse response = this.esClient.indices().create(c -> c
                .index(indexName)
                .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                .mappings(m -> m
                    .properties("did", p -> p.keyword(k -> k))
                    .properties("time_us", p -> p.long_(l -> l))
                    .properties("kind", p -> p.keyword(k -> k))
                    .properties("commit", p -> p.object(o -> o
                        .properties("rev", rev -> rev.keyword(k -> k))
                        .properties("operation", op -> op.keyword(k -> k))
                        .properties("collection", col -> col.keyword(k -> k))
                        .properties("rkey", rkey -> rkey.keyword(k -> k))
                        .properties("record", record -> record.object(r -> r
                            .properties("$type", t -> t.keyword(k -> k))
                            .properties("createdAt", createdAt -> createdAt.date(d -> d))
                            .properties("langs", langs -> langs.keyword(k -> k))
                            .properties("reply", reply -> reply.object(rep -> rep
                                .properties("parent", parent -> parent.object(par -> par
                                    .properties("cid", cid -> cid.keyword(k -> k))
                                    .properties("uri", uri -> uri.keyword(k -> k))
                                ))
                                .properties("root", root -> root.object(ro -> ro
                                    .properties("cid", cid -> cid.keyword(k -> k))
                                    .properties("uri", uri -> uri.keyword(k -> k))
                                ))
                            ))
                            .properties("text", text -> text.text(t -> t))
                            .properties("subject", subject -> subject.object(sub -> sub
                                .properties("cid", cid -> cid.keyword(k -> k))
                                .properties("uri", uri -> uri.keyword(k -> k))
                            ))
                        ))
                        .properties("cid", cid -> cid.keyword(k -> k))
                    ))
                )
            );

            if (response.acknowledged()) {
                logger.info("Created 'events' index");
            } else {
                logger.error("Events index creation not acknowledged.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createTimestampIngestionPipeline() {
        try {
            PutPipelineResponse response = this.esClient.ingest().putPipeline(put -> put
                .id(ingestPipelineName)
                .description("Create timestamp field from time_us")
                .processors(p -> p
                    .script(s -> s
                        .lang("painless")
                        .source("ctx.timestamp = Instant.ofEpochMilli((ctx.time_us + 500) / 1000).toString()")
                    )
                )
            );

            if (response.acknowledged()) {
                logger.info("Created 'add_timestamp' ingestion pipeline");
            } else {
                logger.error("'add_timestamp' ingest pipeline creation not acknowledged.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void performSetup() throws IOException {
        GetPipelineResponse pipelineResponse = this.esClient.ingest().getPipeline(p -> p.id(ingestPipelineName));

        if (!pipelineResponse.result().containsKey(ingestPipelineName)) {
            this.createTimestampIngestionPipeline();
        }

        boolean exists = this.esClient.indices().exists(e -> e.index(indexName)).value();

        if(!exists) {
            this.createIndex();
        }

    }

    
}
