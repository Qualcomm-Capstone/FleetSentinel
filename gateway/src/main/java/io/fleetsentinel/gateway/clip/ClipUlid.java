package io.fleetsentinel.gateway.clip;

import java.security.SecureRandom;

final class ClipUlid {
    private static final char[] ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();

    static String next() {
        byte[] bytes = new byte[10];
        RANDOM.nextBytes(bytes);
        long time = System.currentTimeMillis();
        StringBuilder id = new StringBuilder(26);
        for (int n = 9; n >= 0; n--) id.append(ALPHABET[(int) ((time >>> (n * 5)) & 31)]);
        int buffer = 0, bits = 0;
        for (byte value : bytes) {
            buffer = (buffer << 8) | (value & 255);
            bits += 8;
            while (bits >= 5) { bits -= 5; id.append(ALPHABET[(buffer >>> bits) & 31]); }
        }
        return id.toString();
    }
}
