import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGeneratorFlexibleInterval {

    private static final DateTimeFormatter KDB_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter KDB_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws IOException {
        String csvFile = "interest_list.csv";
        String outputFile = "kdb_queries_flexible.txt";
        int maxRicsPerQuery = 20;

        // ✅ Get interval from command-line args or fallback to default
        int intervalMinutes = 5; // default
        if (args.length > 0) {
            try {
                intervalMinutes = Integer.parseInt(args[0]);
                if (intervalMinutes <= 0 || intervalMinutes > 1440) {
                    throw new IllegalArgumentException("Invalid interval. Use a value between 1 and 1440.");
                }
            } catch (Exception e) {
                System.out.println("Invalid argument for interval. Using default 5 minutes.");
                intervalMinutes = 5;
            }
        }

        System.out.println("⏱️  Generating KDB queries using interval: " + intervalMinutes + " minutes");

        List<String> allLines = Files.readAllLines(Paths.get(csvFile));
        if (allLines.size() <= 1) {
            System.out.println("Input file is empty or contains only header.");
            return;
        }

        // Step 1: Extract execution date from the first valid Execution_Start_Time (col 5)
        LocalDate queryDate = allLines.stream()
                .skip(1)
                .map(line -> line.split(",", -1))
                .filter(parts -> parts.length > 5 && !parts[5].trim().isEmpty())
                .map(parts -> parts[5].trim().substring(0, 10))
                .map(LocalDate::parse)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No valid Execution_Start_Time found."));

        System.out.println("📅 Using query date: " + queryDate);

        // Step 2: Extract distinct sorted RICs from column 3
        List<String> rics = allLines.stream()
                .skip(1)
                .map(line -> line.split(",", -1))
                .filter(parts -> parts.length > 2 && !parts[2].trim().isEmpty())
                .map(parts -> parts[2].trim().replaceAll("\"", ""))
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        System.out.println("🔍 Unique RICs: " + rics.size());

        // Step 3: Chunk RICs into batches of max 20
        List<List<String>> ricBatches = new ArrayList<>();
        for (int i = 0; i < rics.size(); i += maxRicsPerQuery) {
            ricBatches.add(rics.subList(i, Math.min(i + maxRicsPerQuery, rics.size())));
        }

        System.out.println("📦 Total batches: " + ricBatches.size());

        // Step 4: Stream queries to file with dynamic interval
        Duration interval = Duration.ofMinutes(intervalMinutes);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            for (List<String> batch : ricBatches) {
                for (int minutes = 0; minutes < 1440; minutes += intervalMinutes) {
                    LocalTime current = LocalTime.ofSecondOfDay(minutes * 60L);
                    ZonedDateTime startDateTime = ZonedDateTime.of(queryDate, current, ZoneOffset.UTC);
                    ZonedDateTime endDateTime = startDateTime.plus(interval).minusNanos(1_000_000);  // end time - 1ms

                    String query = generateKdbQuery(startDateTime, endDateTime, batch);
                    writer.write(query);
                    writer.newLine();
                }
            }
        }

        System.out.println("✅ All queries generated in " + outputFile);
    }

    private static String generateKdbQuery(ZonedDateTime start, ZonedDateTime end, List<String> rics) {
        String ricList = rics.stream()
                .map(r -> "\"" + r + "\"")
                .collect(Collectors.joining("; ", "`$(", ")"));

        return String.format(
            "futurePeriodTick[(`src`columns`symType`format`filters`applyTz`sDate`sTime`eDate`sTime`tz`syms)!"
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
