package bskyscraper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class AppTest {

    @Test
    public void shouldMakeSubscribeUrl() {
        var url = Util.makeJetstreamSubUrl(new String[] {"app.bsky.feed.post"});
        var expectedUrl = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
        assertEquals(url, expectedUrl);

    }

    @Test
    public void shouldKeepCommitEvent() {
        String messageJson = """
            {
                "did":"blah",
                "time_us":1734847430089564,
                "kind":"commit",
                "commit": {
                    "rev":"blah",
                    "operation":"create",
                    "collection":"app.bsky.feed.post",
                    "rkey":"blah",
                    "record": {
                        "$type":"app.bsky.feed.post",
                        "createdAt":"2024-12-22T06:03:49.468Z",
                        "langs":["en"],
                        "text":"blah"
                    },
                    "cid":"blah"
                }
            }
            """;

            var keep = Util.keepMessage(messageJson);
            assertTrue(keep);
    }

    @Test
    public void shouldNotKeepAccountEvent() {
        String messageJson = """
            {
                "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
                "time_us": 1725516665333808,
                "kind": "account",
                "account": {
                    "active": true,
                    "did": "did:plc:ufbl4k27gp6kzas5glhz7fim",
                    "seq": 1409753013,
                    "time": "2024-09-05T06:11:04.870Z"
                }
            }
            """;

            var keep = Util.keepMessage(messageJson);
            assertFalse(keep);
    }
}
