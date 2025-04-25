import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.stream.*;

public class KdbQueryFromTemplate {

    private static final String PROPERTIES_FILE = "kdb_config.properties";
    private static final String INPUT_CSV = "interest_list.csv";
    private static final String OUTPUT_FILE = "kdb_generated_queries.txt";

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(PROPERTIES_FILE)) {
            props.load(input);
        }

        // ✅ Step 1: Read key config
        String baseQueryTemplate = props.getProperty("KDB_SERVER_URL_PARAMS_1");
        String functionName = props.getProperty("KDB_SERBER_URL_FUNCTION");
        int paramCount = Integer.parseInt(props.getProperty("KDB_SERVER_URL_PARAMS_DYN_CNT"));
        boolean groupRics = "Y".equalsIgnoreCase(props.getProperty("KDB_SERVER_MULTI_SYM_REQD"));
        int maxRics = Integer.parseInt(props.getProperty("KDB_SERVER_SYM_CNT_PER_LOOP"));
        int intervalMinutes = Integer.parseInt(props.getProperty("KDB_SERVER_MULTI_SYM_INTERVAL_PERIOD"));

        // ✅ Step 2: Load CSV and determine start & end date
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
                .map(row -> row[6].substring(0, 10))
                .map(LocalDate::parse)
                .max(LocalDate::compareTo)
                .orElse(startDate);

        // ✅ Step 3: Extract RICs
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

        // ✅ Step 4: Generate queries
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

        System.out.println("✅ KDB queries generated in " + OUTPUT_FILE);
    }

    private static Map<String, String> buildParamMap(ZonedDateTime start, ZonedDateTime end, List<String> rics, Properties props, int count) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String key = "KDB_SERVER_URL_PARAMS_VAL_" + i;
            String type = props.getProperty(key + "_TYPE");

            if ("SDATE".equalsIgnoreCase(type)) {
                String format = props.getProperty(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(start.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("STIME".equalsIgnoreCase(type)) {
                String format = props.getProperty(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(start.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("EDATE".equalsIgnoreCase(type)) {
                String format = props.getProperty(key + "_FORMAT");
                map.put("$" + key, DateTimeFormatter.ofPattern(format).format(end.toInstant().atZone(ZoneOffset.UTC)));
            } else if ("ETIME".equalsIgnoreCase(type)) {
                String format = props.getProperty(key + "_FORMAT");
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
