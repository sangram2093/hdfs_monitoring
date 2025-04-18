import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGenerator {

    static class Interest {
        String ric;
        ZonedDateTime startTime;
        ZonedDateTime endTime;

        public Interest(String ric, ZonedDateTime startTime, ZonedDateTime endTime) {
            this.ric = ric;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    static class RicWindow {
        String ric;
        ZonedDateTime minStart;
        ZonedDateTime maxEnd;

        public RicWindow(String ric, ZonedDateTime minStart, ZonedDateTime maxEnd) {
            this.ric = ric;
            this.minStart = minStart;
            this.maxEnd = maxEnd;
        }
    }

    private static final DateTimeFormatter KDB_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private static final DateTimeFormatter KDB_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws IOException {
        String csvFile = "interest_list.csv";
        String outputFile = "kdb_queries.txt";
        int maxRicsPerChunk = 20;
        Duration overlapWindow = Duration.ofMinutes(5);

        List<Interest> interests = loadInterestList(csvFile);
        Map<String, RicWindow> ricTimeMap = calculateMinMaxTimes(interests);
        List<List<RicWindow>> chunks = groupOverlappingRics(ricTimeMap.values(), overlapWindow, maxRicsPerChunk);

        List<String> kdbQueries = chunks.stream()
                .map(KdbQueryGenerator::generateKdbQuery)
                .collect(Collectors.toList());

        Files.write(Paths.get(outputFile), kdbQueries);
        System.out.println("KDB Queries written to: " + outputFile);
    }

    private static List<Interest> loadInterestList(String file) throws IOException {
        List<Interest> list = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(file)).subList(1, Integer.MAX_VALUE); // Skip header
        for (String line : lines) {
            String[] parts = line.split(",", -1);
            String ric = parts[2].trim().replaceAll("\"", "");
            if (ric.isEmpty()) continue;

            ZonedDateTime start = ZonedDateTime.parse(parts[5]);
            ZonedDateTime end = ZonedDateTime.parse(parts[6]);

            list.add(new Interest(ric, start, end));
        }
        return list;
    }

    private static Map<String, RicWindow> calculateMinMaxTimes(List<Interest> list) {
        return list.stream()
                .collect(Collectors.groupingBy(i -> i.ric))
                .entrySet()
                .stream()
                .map(e -> {
                    String ric = e.getKey();
                    List<Interest> ricEntries = e.getValue();
                    ZonedDateTime minStart = ricEntries.stream().map(i -> i.startTime).min(Comparator.naturalOrder()).get();
                    ZonedDateTime maxEnd = ricEntries.stream().map(i -> i.endTime).max(Comparator.naturalOrder()).get();
                    return new AbstractMap.SimpleEntry<>(ric, new RicWindow(ric, minStart, maxEnd));
                })
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
    }

    private static List<List<RicWindow>> groupOverlappingRics(Collection<RicWindow> rics, Duration window, int maxChunkSize) {
        List<RicWindow> sortedRics = new ArrayList<>(rics);
        sortedRics.sort(Comparator.comparing(r -> r.ric));

        List<List<RicWindow>> groups = new ArrayList<>();
        List<RicWindow> currentGroup = new ArrayList<>();

        for (RicWindow ric : sortedRics) {
            if (currentGroup.isEmpty()) {
                currentGroup.add(ric);
                continue;
            }

            boolean overlaps = currentGroup.stream().anyMatch(r ->
                    !ric.minStart.isAfter(r.maxEnd.plus(window)) &&
                    !ric.maxEnd.isBefore(r.minStart.minus(window)));

            if (overlaps && currentGroup.size() < maxChunkSize) {
                currentGroup.add(ric);
            } else {
                groups.add(new ArrayList<>(currentGroup));
                currentGroup.clear();
                currentGroup.add(ric);
            }
        }

        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }

    private static String generateKdbQuery(List<RicWindow> group) {
        ZonedDateTime minStart = group.stream().map(r -> r.minStart).min(Comparator.naturalOrder()).get();
        ZonedDateTime maxEnd = group.stream().map(r -> r.maxEnd).max(Comparator.naturalOrder()).get();

        String ricList = group.stream()
                .map(r -> "\"" + r.ric + "\"")
                .collect(Collectors.joining("; ", "`$(", ")"));

        return String.format(
            "futurePeriodTick[(`src`columns`symType`format`filters`applyTz`sDate`sTime`eDate`eTime`tz`syms)!"
          + "(`reuters;`date`sym`time`exchDate`exchTime`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5"
          + "`bidSize1`bidSize2`bidSize3`bidSize4`bidSize5`askPrice1`askPrice2`askPrice3`askPrice4`askPrice5"
          + "`askSize1`askSize2`askSize3`askSize4`askSize5`bidNo1`bidNo2`bidNo3`bidNo4`bidNo5`askNo1`askNo2`askNo3`askNo4`askNo5;"
          + "`ric;`depth;`;0b;%s;%s;%s;%s;`$\"\";%s)]",
            minStart.format(KDB_DATE),
            minStart.format(KDB_TIME),
            maxEnd.format(KDB_DATE),
            maxEnd.format(KDB_TIME),
            ricList
        );
    }
}
