package org.meeuw.util.kafka;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaDumpReaderTest {

    @Test
    void readParsesLegacyDump() {
        try (Stream<KafkaDumpReader.Record> read = KafkaDumpReader.read(getClass().getResourceAsStream("/availability/availability-messages.table"))) {
            KafkaDumpReader.Record first = read.findFirst().orElseThrow();

            assertEquals("POW_00751358", first.key());
            assertEquals(2, first.partition());
            assertEquals(264667, first.offset());
            assertNotNull(first.timeStamp());
        }
    }

    @Test
    void readTSVMapsIteratorToStream() throws IOException {
        try (Stream<KafkaDumpReader.Record> read = KafkaDumpReader.readTSV(getClass().getResourceAsStream("/output.tsv"))) {
            List<KafkaDumpReader.Record> records = read.toList();

            assertEquals(2, records.size());

            KafkaDumpReader.Record first = records.get(0);
            assertEquals("won-vpp-catalog-epg", first.topic());
            assertEquals(Instant.parse("2026-04-14T05:44:08Z"), first.timeStamp());
            assertEquals("20260414074323-CatalogEPG-AT_300022362-npo-svod.json", first.key());
            assertEquals(0, first.partition());
            assertEquals(5L, first.offset());
            assertNotNull(first.value());
        }
    }

}
