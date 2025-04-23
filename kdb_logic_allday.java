import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGeneratorStreamed {

    private static final DateTimeFormatter KDB_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter KDB_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws IOException {
        String csvFile = "interest_list.csv";
        String outputFile = "kdb_queries_streamed.txt";
        int maxRicsPerQuery = 20;
        LocalDate queryDate = LocalDate.of(2025, 3, 7);
        Duration interval = Duration.ofMinutes(5);

        // Step 1: Read distinct RICs and sort
        List<String> rics = Files.readAllLines(Paths.get(csvFile)).stream()
                .skip(1)
                .map(line -> line.split(",", -1)[2].trim().replaceAll("\"", ""))
                .filter(ric -> !ric.isEmpty())
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        // Step 2: Chunk RICs into groups of max 20
        List<List<String>> ricBatches = new ArrayList<>();
        for (int i = 0; i < rics.size(); i += maxRicsPerQuery) {
            ricBatches.add(rics.subList(i, Math.min(i + maxRicsPerQuery, rics.size())));
        }

        // Step 3: Stream queries directly to file (no heap buildup)
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            for (List<String> batch : ricBatches) {
                LocalTime current = LocalTime.MIDNIGHT;
                LocalTime dayEnd = LocalTime.MIDNIGHT.plusDays(1); // 24:00

                while (current.isBefore(dayEnd)) {
                    LocalTime next = current.plus(interval);
                    if (next.isAfter(dayEnd)) next = dayEnd;

                    ZonedDateTime startDateTime = ZonedDateTime.of(queryDate, current, ZoneOffset.UTC);
                    ZonedDateTime endDateTime = ZonedDateTime.of(queryDate, next, ZoneOffset.UTC);

                    String query = generateKdbQuery(startDateTime, endDateTime, batch);
                    writer.write(query);
                    writer.newLine();

                    current = next;
                }
            }
        }

        System.out.println("✅ KDB Queries written successfully to: " + outputFile);
    }

    private static String generateKdbQuery(ZonedDateTime start, ZonedDateTime end, List<String> rics) {
        String ricList = rics.stream()
                .map(r -> "\"" + r + "\"")
                .collect(Collectors.joining("; ", "`$(", ")"));

        return String.format(
            "futurePeriodTick[(`src`columns`symType`format`filters`applyTz`sDate`sTime`eDate`eTime`tz`syms)!"
          + "(`reuters;`date`sym`time`exchDate`exchTime`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5"
          + "`bidSize1`bidSize2`bidSize3`bidSize4`bidSize5`askPrice1`askPrice2`askPrice3`askPrice4`askPrice5"
          + "`askSize1`askSize2`askSize3`askSize4`askSize5`bidNo1`bidNo2`bidNo3`bidNo4`bidNo5`askNo1`askNo2`askNo3`askNo4`askNo5;"
          + "`ric;`depth;`;0b;%s;%s;%s;%s;`$\"\";%s)]",
            KDB_DATE.format(start.toInstant()),
            KDB_TIME.format(start.toInstant()),
            KDB_DATE.format(end.toInstant()),
            KDB_TIME.format(end.toInstant()),
            ricList
        );
    }
}
