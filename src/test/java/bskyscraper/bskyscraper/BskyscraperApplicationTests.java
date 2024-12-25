package bskyscraper.bskyscraper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.profiles.active=test")
class BskyscraperApplicationTests {

	@Test
	void contextLoads() {
	}

    @Test
    public void shouldMakeSubscribeUrl() {
        var url = Util.makeJetstreamSubUrl(new String[] {"app.bsky.feed.post"}, null);
        var expectedUrl = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
        assertEquals(url, expectedUrl);

    }

}
