import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGenerator {

    static class RicWindow {
        String ric;
        ZonedDateTime start;
        ZonedDateTime end;

        public RicWindow(String ric, ZonedDateTime start, ZonedDateTime end) {
            this.ric = ric;
            this.start = start;
            this.end = end;
        }
    }

    private static final DateTimeFormatter KDB_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter KDB_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws IOException {
        String csvFile = "interest_list.csv";
        String outputFile = "kdb_queries.txt";
        int maxRicsPerQuery = 20;
        Duration intervalSize = Duration.ofMinutes(5);

        List<RicWindow> ricWindows = loadAndExplodeInterestList(csvFile, intervalSize);

        // Group by interval start time
        Map<ZonedDateTime, List<RicWindow>> intervalBuckets = ricWindows.stream()
                .collect(Collectors.groupingBy(w -> getIntervalStart(w.start, intervalSize)));

        List<String> queries = new ArrayList<>();

        // Sort intervals
        List<ZonedDateTime> sortedIntervals = new ArrayList<>(intervalBuckets.keySet());
        sortedIntervals.sort(Comparator.naturalOrder());

        for (ZonedDateTime intervalStart : sortedIntervals) {
            List<RicWindow> ricsInWindow = intervalBuckets.get(intervalStart);

            // Sort RICs inside interval
            ricsInWindow.sort(Comparator.comparing(r -> r.ric));

            for (int i = 0; i < ricsInWindow.size(); i += maxRicsPerQuery) {
                List<RicWindow> batch = ricsInWindow.subList(i, Math.min(i + maxRicsPerQuery, ricsInWindow.size()));
                queries.add(generateKdbQuery(intervalStart, intervalStart.plus(intervalSize), batch));
            }
        }

        Files.write(Paths.get(outputFile), queries);
        System.out.println("✅ KDB queries written to " + outputFile);
    }

    private static List<RicWindow> loadAndExplodeInterestList(String file, Duration intervalSize) throws IOException {
        List<RicWindow> explodedWindows = new ArrayList<>();
        List<String> allLines = Files.readAllLines(Paths.get(file));
        List<String> lines = allLines.subList(1, allLines.size()); // skip header

        for (String line : lines) {
            String[] parts = line.split(",", -1);
            if (parts.length < 7) continue;

            String ric = parts[2].trim().replaceAll("\"", "");
            if (ric.isEmpty()) continue;

            try {
                ZonedDateTime start = ZonedDateTime.parse(parts[5]).withZoneSameInstant(ZoneOffset.UTC);
                ZonedDateTime end = ZonedDateTime.parse(parts[6]).withZoneSameInstant(ZoneOffset.UTC);

                ZonedDateTime windowStart = getIntervalStart(start, intervalSize);
                while (windowStart.isBefore(end)) {
                    ZonedDateTime windowEnd = windowStart.plus(intervalSize);
                    ZonedDateTime ricStart = start.isAfter(windowStart) ? start : windowStart;
                    ZonedDateTime ricEnd = end.isBefore(windowEnd) ? end : windowEnd;

                    explodedWindows.add(new RicWindow(ric, ricStart, ricEnd));
                    windowStart = windowEnd;
                }

            } catch (DateTimeException e) {
                System.err.println("Skipping row with invalid date: " + line);
            }
        }

        return explodedWindows;
    }

    private static ZonedDateTime getIntervalStart(ZonedDateTime time, Duration interval) {
        long seconds = time.toEpochSecond();
        long intervalSeconds = interval.getSeconds();
        long floored = (seconds / intervalSeconds) * intervalSeconds;
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(floored), ZoneOffset.UTC);
    }

    private static String generateKdbQuery(ZonedDateTime start, ZonedDateTime end, List<RicWindow> group) {
        if (group.isEmpty()) return "// Skipped empty group\n";

        String ricList = group.stream()
                .map(r -> "\"" + r.ric + "\"")
                .distinct()
                .collect(Collectors.joining("; ", "`$(", ")"));

        return String.format(
            "futurePeriodTick[(`src`columns`symType`format`filters`applyTz`sDate`sTime`eDate`eTime`tz`syms)!"
          + "(`reuters;`date`sym`time`exchDate`exchTime`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5"
          + "`bidSize1`bidSize2`bidSize3`bidSize4`bidSize5`askPrice1`askPrice2`askPrice3`askPrice4`askPrice5"
          + "`askSize1`askSize2`askSize3`askSize4`askSize5`bidNo1`bidNo2`bidNo3`bidNo4`bidNo5`askNo1`askNo2`askNo3`askNo4`askNo5;"
          + "`ric;`depth;`;0b;%s;%s;%s;%s;`$\"\";%s)]",
            KDB_DATE.format(start),
            KDB_TIME.format(start),
            KDB_DATE.format(end),
            KDB_TIME.format(end),
            ricList
        );
    }
}
