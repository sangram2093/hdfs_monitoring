import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

public class KdbQueryFromColonProps {

    private static final String PROPERTIES_FILE = "kdb_config.properties";
    private static final String INPUT_CSV = "interest_list.csv";
    private static final String OUTPUT_FILE = "kdb_generated_queries.txt";

    public static void main(String[] args) throws IOException {
        Map<String, String> props = loadCustomProperties(PROPERTIES_FILE);

        String baseQueryTemplate = props.get("KDB_SERVER_URL_PARAMS_1");
        String functionName = props.get("KDB_SERBER_URL_FUNCTION");
        int paramCount = Integer.parseInt(props.get("KDB_SERVER_URL_PARAMS_DYN_CNT"));
        boolean groupRics = "Y".equalsIgnoreCase(props.get("KDB_SERVER_MULTI_SYM_REQD"));
        int maxRics = Integer.parseInt(props.get("KDB_SERVER_SYM_CNT_PER_LOOP"));
        int intervalMinutes = Integer.parseInt(props.get("KDB_SERVER_MULTI_SYM_INTERVAL_PERIOD"));

        List<String> allLines = Files.readAllLines(Paths.get(INPUT_CSV));
        List<String[]> data = allLines.stream().skip(1)
                .map(line -> line.split(",", -1))
                .collect(Collectors.toList());

        LocalDate startDate = data.stream()
                .filter(row -> row.length > 5 && !row[5].trim().isEmpty())
                .map(row -> row[5].substring(0, 10))
                .map(LocalDate::parse)
                .min(LocalDate::compareTo)
                .orElseThrow();

        LocalDate endDate = data.stream()
                .filter(row -> row.length > 6 && !row[6].trim().isEmpty())
                .map(row -> row[6].trim().substring(0, 10))
                .map(LocalDate::parse)
                .max(LocalDate::compareTo)
                .orElse(startDate);

        List<String> rics = data.stream()
                .filter(row -> row.length > 2 && !row[2].trim().isEmpty())
                .map(row -> row[2].trim().replaceAll("\"", ""))
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<List<String>> ricBatches = groupRics ?
                IntStream.range(0, (rics.size() + maxRics - 1) / maxRics)
                        .mapToObj(i -> rics.subList(i * maxRics, Math.min(rics.size(), (i + 1) * maxRics)))
                        .collect(Collectors.toList())
                : rics.stream().map(Collections::singletonList).collect(Collectors.toList());

        Duration interval = Duration.ofMinutes(intervalMinutes);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(OUTPUT_FILE))) {
            for (LocalDate date : Arrays.asList(startDate, endDate)) {
                for (List<String> batch : ricBatches) {
                    LocalTime startTime = LocalTime.MIDNIGHT;
                    ZonedDateTime start = ZonedDateTime.of(date, startTime, ZoneOffset.UTC);
                    ZonedDateTime end = start.plus(interval).minusNanos(1_000_000);  // -1ms

                    Map<String, String> paramMap = buildParamMap(start, end, batch, props, paramCount);
                    String filledQuery = applyTemplate(baseQueryTemplate, paramMap);
                    writer.write(functionName + filledQuery);
                    writer.newLine();
                }
            }
        }

        System.out.println("✅ Queries written to: " + OUTPUT_FILE);
    }

    private static Map<String, String> loadCustomProperties(String file) throws IOException {
        Map<String, String> map = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty() || line.trim().startsWith("#")) continue;
                String[] parts = line.split("::", 2);
                if (parts.length == 2) {
                    map.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        return map;
    }

    private static Map<String, String> buildParamMap(ZonedDateTime start, ZonedDateTime end, List<String> rics, Map<String, String> props, int count) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String key = "KDB_SERVER_URL_PARAMS_VAL_" + i;
            String type = props.get(key + "_TYPE");

            if ("SDATE".equalsIgnoreCase(type)) {
                String format = props.get(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(start.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("STIME".equalsIgnoreCase(type)) {
                String format = props.get(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(start.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("EDATE".equalsIgnoreCase(type)) {
                String format = props.get(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(end.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("ETIME".equalsIgnoreCase(type)) {
                String format = props.get(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(end.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("STRING".equalsIgnoreCase(type)) {
                map.put("$" + key, "`$(" + rics.stream().map(r -> "\"" + r + "\"").collect(Collectors.joining("; ")) + ")");
            }
        }
        return map;
    }

    private static String applyTemplate(String template, Map<String, String> values) {
        String result = template;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
