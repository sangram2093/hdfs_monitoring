import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryGeneratorFixedDays {

    private static final DateTimeFormatter KDB_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter KDB_TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws IOException {
        String csvFile = "interest_list.csv";
        String outputFile = "kdb_queries_days_only.txt";
        int maxRicsPerQuery = 10;

        // ✅ Optional interval in minutes
        int intervalMinutes = 1440; // default 24h
        if (args.length > 0) {
            try {
                intervalMinutes = Integer.parseInt(args[0]);
                if (intervalMinutes <= 0 || intervalMinutes > 1440)
                    throw new IllegalArgumentException("Invalid interval");
            } catch (Exception e) {
                System.out.println("Invalid interval provided. Using default: 1440 mins.");
            }
        }

        List<String> allLines = Files.readAllLines(Paths.get(csvFile));
        if (allLines.size() <= 1) {
            System.out.println("❌ Empty or header-only file.");
            return;
        }

        // ✅ Get start and end dates
        LocalDate startDate = allLines.stream()
                .skip(1)
                .map(line -> line.split(",", -1))
                .filter(parts -> parts.length > 5 && !parts[5].trim().isEmpty())
                .map(parts -> parts[5].trim().substring(0, 10))
                .map(LocalDate::parse)
                .min(LocalDate::compareTo)
                .orElseThrow();

        LocalDate endDate = allLines.stream()
                .skip(1)
                .map(line -> line.split(",", -1))
                .filter(parts -> parts.length > 6 && !parts[6].trim().isEmpty())
                .map(parts -> parts[6].trim().substring(0, 10))
                .map(LocalDate::parse)
                .max(LocalDate::compareTo)
                .orElse(startDate);

        System.out.println("📅 Start Date: " + startDate + " | End Date: " + endDate);
        System.out.println("🕒 Interval: " + intervalMinutes + " minutes");

        // ✅ Collect RICs
        List<String> rics = allLines.stream()
                .skip(1)
                .map(line -> line.split(",", -1))
                .filter(parts -> parts.length > 2 && !parts[2].trim().isEmpty())
                .map(parts -> parts[2].trim().replaceAll("\"", ""))
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<List<String>> ricBatches = new ArrayList<>();
        for (int i = 0; i < rics.size(); i += maxRicsPerQuery) {
            ricBatches.add(rics.subList(i, Math.min(i + maxRicsPerQuery, rics.size())));
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            for (LocalDate date : Arrays.asList(startDate, endDate)) {
                for (List<String> batch : ricBatches) {
                    LocalTime startTime = LocalTime.MIDNIGHT;
                    ZonedDateTime startDateTime = ZonedDateTime.of(date, startTime, ZoneOffset.UTC);
                    ZonedDateTime endDateTime = startDateTime.plusMinutes(intervalMinutes).minusNanos(1_000_000);

                    String query = generateKdbQuery(startDateTime, endDateTime, batch);
                    writer.write(query);
                    writer.newLine();
                }
            }
        }

        System.out.println("✅ KDB queries written to " + outputFile);
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
