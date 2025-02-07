package org.meeuw.util.kafka;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class KafkaDumpReaderTest {

    @Test
    public void test() {
        Stream<KafkaDumpReader.Record> read = KafkaDumpReader.read(getClass().getResourceAsStream("/availability/availability-messages.table"));
        read
            .limit(5)
            .forEach(r -> {
            System.out.println(r.toString() + " " + r.key() + "=" + r.offset());
        });
    }

}
