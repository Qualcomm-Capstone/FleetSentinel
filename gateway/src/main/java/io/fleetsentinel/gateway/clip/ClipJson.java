package io.fleetsentinel.gateway.clip;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

final class ClipJson {
    static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();

    String write(Object value) { return MAPPER.writeValueAsString(value); }
    ClipApi.Create read(String value) { return MAPPER.readValue(value, ClipApi.Create.class); }
    ClipApi.Complete parts(String value) { return MAPPER.readValue(value, ClipApi.Complete.class); }

    String fingerprint(ClipApi.Create value) {
        return HexFormat.of().formatHex(digest().digest(write(value).getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
