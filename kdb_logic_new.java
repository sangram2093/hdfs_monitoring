import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGeneratorOverlapping {

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
        Duration maxGroupDuration = Duration.ofMinutes(5);

        List<RicWindow> ricWindows = loadInterestList(csvFile);

        // Sort by start time
        ricWindows.sort(Comparator.comparing(r -> r.start));

        List<List<RicWindow>> groupedQueries = new ArrayList<>();
        List<RicWindow> currentGroup = new ArrayList<>();

        for (RicWindow ric : ricWindows) {
            currentGroup.add(ric);
            ZonedDateTime groupStart = currentGroup.stream().map(r -> r.start).min(Comparator.naturalOrder()).get();
            ZonedDateTime groupEnd = currentGroup.stream().map(r -> r.end).max(Comparator.naturalOrder()).get();
            Duration groupDuration = Duration.between(groupStart, groupEnd);

            boolean exceedsWindow = groupDuration.compareTo(maxGroupDuration) > 0;
            boolean exceedsCount = currentGroup.size() > maxRicsPerQuery;

            if (exceedsWindow || exceedsCount) {
                RicWindow last = currentGroup.remove(currentGroup.size() - 1);
                groupedQueries.add(new ArrayList<>(currentGroup));
                currentGroup.clear();
                currentGroup.add(last);
            }
        }

        if (!currentGroup.isEmpty()) {
            groupedQueries.add(currentGroup);
        }

        // Optional: sort queries by minStart and then ric name
        groupedQueries.sort(Comparator
            .comparing((List<RicWindow> g) -> g.stream().map(r -> r.start).min(Comparator.naturalOrder()).orElse(ZonedDateTime.now()))
            .thenComparing(g -> g.stream().map(r -> r.ric).min(String::compareTo).orElse("")));

        List<String> queries = groupedQueries.stream()
                .map(KdbQueryGeneratorOverlapping::generateKdbQuery)
                .collect(Collectors.toList());

        Files.write(Paths.get(outputFile), queries);
        System.out.println("✅ KDB Queries written to: " + outputFile);
    }

    private static List<RicWindow> loadInterestList(String file) throws IOException {
        List<RicWindow> list = new ArrayList<>();
        List<String> allLines = Files.readAllLines(Paths.get(file));
        List<String> lines = allLines.subList(1, allLines.size()); // Skip header

        for (String line : lines) {
            String[] parts = line.split(",", -1);
            if (parts.length < 7) continue;

            String ric = parts[2].trim().replaceAll("\"", "");
            if (ric.isEmpty()) continue;

            try {
                ZonedDateTime start = ZonedDateTime.parse(parts[5]).withZoneSameInstant(ZoneOffset.UTC);
                ZonedDateTime end = ZonedDateTime.parse(parts[6]).withZoneSameInstant(ZoneOffset.UTC);
                list.add(new RicWindow(ric, start, end));
            } catch (DateTimeException e) {
                System.err.println("Skipping invalid row: " + line);
            }
        }

        return list;
    }

    private static String generateKdbQuery(List<RicWindow> group) {
        if (group.isEmpty()) return "";

        ZonedDateTime minStart = group.stream().map(r -> r.start).min(Comparator.naturalOrder()).get();
        ZonedDateTime maxEnd = group.stream().map(r -> r.end).max(Comparator.naturalOrder()).get();

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
            KDB_DATE.format(minStart),
            KDB_TIME.format(minStart),
            KDB_DATE.format(maxEnd),
            KDB_TIME.format(maxEnd),
            ricList
        );
    }
}
