package com.mentor.cachingproxy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(properties = {"proxy.admin-token=s3cret", "proxy.backend-url=http://localhost:1"})
@AutoConfigureWebTestClient
class AdminAuthTest {

    @Autowired
    private WebTestClient client;

    @Test
    void purgeWithoutTokenIsUnauthorized() {
        client.delete().uri("/cache").exchange().expectStatus().isUnauthorized();
        client.delete().uri("/cache/GET:/x").header("X-Admin-Token", "wrong").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void purgeWithTokenIsAllowed() {
        client.delete().uri("/cache").header("X-Admin-Token", "s3cret").exchange().expectStatus().isOk();
    }

    @Test
    void statsAndHealthStayPublic() {
        client.get().uri("/cache/stats").exchange().expectStatus().isOk();
        client.get().uri("/actuator/health").exchange().expectStatus().isOk();
    }
}
