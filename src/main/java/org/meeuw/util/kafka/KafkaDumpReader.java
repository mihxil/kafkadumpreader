package org.meeuw.util.kafka;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;


/**
 * Support for format that Intellij kafka plugin that produces dumps in this odd format.
 *<p>
 * Tab separated (with a header?). No escaping. The last field has no newlines, so it can be split on newline then.
 */
public class KafkaDumpReader {

    private KafkaDumpReader() {
        // no instances!
    }

    // pretty much absurd
    public static DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    public static Stream<Record> readTSV(InputStream inputStream) throws IOException {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.builder()
            .addColumn("Topic")
            .addColumn("Timestamp")
            .addColumn("Key")
            .addColumn("Value")
            .addColumn("Partition")
            .addColumn("Offset")
            .setColumnSeparator('\t')
            .setUseHeader(true)
            .setQuoteChar('"')
            .build();
        final MappingIterator<Map<String, String>> read = mapper
            .readerFor(Map.class)
            .with(schema)
            .readValues(inputStream);
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(read, Spliterator.ORDERED | Spliterator.NONNULL),
            false).map(KafkaDumpReader::toRecord)
            .onClose(() -> close(read));
    }

    public static Stream<Record> read(InputStream inputStream) {
        var  reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        var  scanner = new Scanner(reader);

        // skip header
        //log.debug(Arrays.asList(readRecord(scanner, NUMBER_OF_FIELDS_IN_RECORD)));

        Stream<Record> stream = StreamSupport.stream(new Spliterator<>() {
            @Override
            public int characteristics() {
                return Spliterator.DISTINCT;
            }
            @Override
            public long estimateSize() {
                return Integer.MAX_VALUE;
            }
            @Override
            public boolean tryAdvance(Consumer<? super Record> tr) {
                if (scanner.hasNext()) {
                    String[] record = readRecord(scanner, LEGACY_NUMBER_OF_FIELDS_IN_RECORD);
                    tr.accept(new Record(
                        null,
                        parseTimestamp(record[0]),
                        record[1],
                        record[2],
                        Integer.parseInt(record[3]),
                        Long.parseLong(record[4])
                    ));
                    return true;
                } else {
                    return false;
                }

            }

            @Override
            public Spliterator<Record> trySplit() {
                return null;
            }
        }, false);
        return stream.onClose(scanner::close);
    }

    private static Record toRecord(Map<String, String> record) {
        return new Record(
            record.get("Topic"),
            parseTimestamp(record.get("Timestamp")),
            record.get("Key"),
            record.get("Value"),
            Integer.parseInt(record.get("Partition")),
            Long.parseLong(record.get("Offset"))
        );
    }

    private static Instant parseTimestamp(String timestamp) {
        try {
            return ZonedDateTime.parse(timestamp.trim(), TIMESTAMP_FORMAT).toInstant();
        } catch (DateTimeParseException wtf) {
            return null;
        }
    }
    private static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    public static String[] readRecord(Scanner scanner, int fields) {
        var result = new String[fields];
        scanner.useDelimiter("\t");
        for (int i = 0 ; i < fields -1; i++) {
            result[i] = scanner.next().trim();
        }
        scanner.useDelimiter("[\n\r]+");
        result[fields - 1] = scanner.next().trim();
        return result;
    }
    static final int LEGACY_NUMBER_OF_FIELDS_IN_RECORD = 5;

    public record Record(String topic, Instant timeStamp, String key, String value, int partition, long offset) {


        public byte[] bytes() {
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }


}
