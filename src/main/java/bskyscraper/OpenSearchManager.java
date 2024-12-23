package bskyscraper;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.ingest.GetPipelineResponse;
import org.opensearch.client.opensearch.ingest.PutPipelineResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

public class OpenSearchManager {

    private final static Logger logger = LoggerFactory.getLogger(OpenSearchManager.class);
    private final static String indexName = "events";
    private final static String ingestPipelineName = "add_timestamp";

    private final OpenSearchClient osClient;

    public OpenSearchManager() {
        System.setProperty("javax.net.ssl.trustStore", Constants.trustStore);
        System.setProperty("javax.net.ssl.trustStorePassword", Constants.trustStorePassword);

        final HttpHost host = HttpHost.create(Constants.osServerUrl);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(host), new UsernamePasswordCredentials(Constants.osUser, Constants.osPassword));

        final RestClient restClient = RestClient.builder(host).
        setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        }).build();

        final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.osClient = new OpenSearchClient(transport);

    }

    public void createIndex() throws Exception {
        CreateIndexResponse response = this.osClient.indices().create(c -> c
            .index(indexName)
            .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
            .mappings(m -> m
                .properties("did", p -> p.keyword(k -> k))
                .properties("timestamp", p -> p.date(k -> k))
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
    }

    public void createTimestampIngestionPipeline() throws Exception {
        PutPipelineResponse response = this.osClient.ingest().putPipeline(put -> put
            .id(ingestPipelineName)
            .description("Create timestamp field from time_us")
            .processors(p -> p
                .script(s -> s
                    .inline(i -> i
                        .lang("painless")
                        .source("ctx.timestamp = Instant.ofEpochMilli((ctx.time_us + 500) / 1000).toString()"))
                )
            )
        );

        if (response.acknowledged()) {
            logger.info("Created 'add_timestamp' ingestion pipeline");
        } else {
            logger.error("'add_timestamp' ingest pipeline creation not acknowledged.");
        }

    }

    public void performSetup() throws Exception {
        GetPipelineResponse pipelineResponse = this.osClient.ingest().getPipeline(p -> p.id(ingestPipelineName));

        if (!pipelineResponse.result().containsKey(ingestPipelineName)) {
            this.createTimestampIngestionPipeline();
        }

        boolean exists = this.osClient.indices().exists(e -> e.index(indexName)).value();

        if(!exists) {
            this.createIndex();
        }

    }

    public <T> void insertPayloads(List<T> payloads) throws Exception {
        var payloadsArr = payloads.toArray();
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (var payload : payloadsArr) {
            br.operations(op -> op
                .index(idx -> idx
                    .index(indexName)
                    .pipeline(ingestPipelineName)
                    .document(payload)
                )
            );
        }
        BulkResponse response = this.osClient.bulk(br.build());

        StringBuilder stringBuilder = new StringBuilder();

        if (response.errors()) {
            stringBuilder.append("Bulk insert had errors: [ ");
            int i = 0;
            for (BulkResponseItem item: response.items()) {
                if (item.error() != null) {
                    stringBuilder.append(String.format("\"%s\" ", item.error().reason()));
                    logger.error("Failed message: {}", payloadsArr[i].toString());
                }
                i++;
            }
            stringBuilder.append("]");
            logger.error(stringBuilder.toString());
            throw new Exception("Error inserting payloads.");
        }

        logger.info("Inserted {} payloads", payloads.size());
    }

    
}
