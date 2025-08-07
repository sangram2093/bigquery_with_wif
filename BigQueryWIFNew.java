package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.*;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.*;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.*;
import java.util.List;

public class BigQueryWIFPdfExporter {

    private static final String WIF_ENDPOINT = "https://frtrasdsa10.cd.ab.com:9001/";
    private static final String WIF_HOME = System.getenv("WIF_HOME");
    private static final String CLIENT_PEM_PATH = WIF_HOME + "/client.pem";
    private static final String CA_CERT_PATH = WIF_HOME + "/ca_chain.crt";
    private static final String CONFIG_YAML_PATH = "resources/config.yaml";

    public static void main(String[] args) throws Exception {
        String accessTokenString = getTokenFromSecureEndpoint();
        AccessToken token = new AccessToken(accessTokenString, null);

        GoogleCredentials credentials = GoogleCredentials.create(token)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId(System.getenv("PROJECT_NAME"))
                .setLocation(System.getenv("LOCATION"))
                .setCredentials(credentials)
                .build()
                .getService();

        String query = "SELECT exchange, client_id, order_id, symbol, side, price, quantity, status, trader, desk, account, notes FROM `your-project.your_dataset.your_table` LIMIT 100";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        JobId jobId = JobId.of("WIF_QUERY_" + System.currentTimeMillis());
        Job job = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();

        if (job == null || job.getStatus().getError() != null)
            throw new RuntimeException("BigQuery error: " + (job == null ? "Job not found" : job.getStatus().getError()));

        TableResult result = job.getQueryResults();

        // Group rows by exchange for pagination
        Map<String, List<FieldValueList>> groupedData = new LinkedHashMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String exchange = row.get("exchange").getStringValue();
            groupedData.computeIfAbsent(exchange, k -> new ArrayList<>()).add(row);
        }

        // Column widths from config
        Map<String, Integer> colWidths = loadColumnWidthsFromYaml(CONFIG_YAML_PATH);

        generatePdf(groupedData, result.getSchema().getFields(), colWidths);
    }

    private static String getTokenFromSecureEndpoint() throws Exception {
    String clientPem = Files.readString(Paths.get(CLIENT_PEM_PATH));
    String caCert = Files.readString(Paths.get(CA_CERT_PATH));

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
            PemUtils.createKeyManagerFactory(clientPem).getKeyManagers(),
            PemUtils.createTrustManagerFactory(caCert).getTrustManagers(),
            new SecureRandom()
    );

    URL url = new URL(WIF_ENDPOINT);
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    conn.setSSLSocketFactory(sslContext.getSocketFactory()); // ✅ Local to this request only
    conn.setRequestMethod("GET");

    if (conn.getResponseCode() == 200) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(reader);
            return node.get("access_token").asText();
        }
    } else {
        throw new RuntimeException("Failed to retrieve token: " + conn.getResponseCode());
    }
}

    private static Map<String, Integer> loadColumnWidthsFromYaml(String yamlPath) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream input = Files.newInputStream(Paths.get(yamlPath))) {
            Map<String, Object> config = yaml.load(input);
            return (Map<String, Integer>) config.get("column_widths");
        }
    }

    private static void generatePdf(Map<String, List<FieldValueList>> data,
                                    Schema schema,
                                    Map<String, Integer> colWidths) throws IOException {
        try (PDDocument doc = new PDDocument()) {
            PDFont font = PDType1Font.HELVETICA;
            PDFont boldFont = PDType1Font.HELVETICA_BOLD;
            float margin = 40;
            float fontSize = 10;
            float rowSpacing = 4;

            for (Map.Entry<String, List<FieldValueList>> entry : data.entrySet()) {
                PDPage page = new PDPage(PDRectangle.LETTER.rotate());
                doc.addPage(page);
                PDPageContentStream stream = new PDPageContentStream(doc, page);

                float yStart = page.getMediaBox().getHeight() - margin;
                float tableTopY = yStart - 40;

                // Header: exchange and title
                stream.beginText();
                stream.setFont(font, 12);
                stream.newLineAtOffset(margin, yStart);
                stream.showText("Exchange: " + entry.getKey());
                stream.endText();

                stream.beginText();
                stream.setFont(boldFont, 14);
                stream.newLineAtOffset(page.getMediaBox().getWidth() / 2 - 60, yStart);
                stream.showText("BigQuery Export");
                stream.endText();

                // Table
                float tableX = margin;
                float y = tableTopY;
                float cellHeight;
                List<String> headers = new ArrayList<>();
                for (Field field : schema.getFields()) headers.add(field.getName());

                // Draw header row
                y -= 20;
                float x = tableX;
                float maxHeaderHeight = 0;
                for (String col : headers) {
                    float colWidth = colWidths.getOrDefault(col, 60);
                    List<String> lines = wrapText(col, font, fontSize, colWidth);
                    maxHeaderHeight = Math.max(maxHeaderHeight, lines.size());
                    drawCell(stream, x, y, colWidth, lines.size() * (fontSize + rowSpacing), lines, boldFont, fontSize);
                    x += colWidth;
                }
                y -= maxHeaderHeight * (fontSize + rowSpacing);

                // Draw rows
                for (FieldValueList row : entry.getValue()) {
                    x = tableX;
                    int maxLines = 1;
                    Map<String, List<String>> wrappedMap = new HashMap<>();

                    for (String col : headers) {
                        String text = row.get(col).isNull() ? "" : row.get(col).getValue().toString();
                        float width = colWidths.getOrDefault(col, 60);
                        List<String> wrapped = wrapText(text, font, fontSize, width);
                        wrappedMap.put(col, wrapped);
                        maxLines = Math.max(maxLines, wrapped.size());
                    }

                    float rowHeight = maxLines * (fontSize + rowSpacing);
                    for (String col : headers) {
                        float width = colWidths.getOrDefault(col, 60);
                        drawCell(stream, x, y, width, rowHeight, wrappedMap.get(col), font, fontSize);
                        x += width;
                    }
                    y -= rowHeight;
                }

                // Footer
                stream.beginText();
                stream.setFont(font, 10);
                stream.newLineAtOffset(page.getMediaBox().getWidth() - 80, 20);
                stream.showText("Page " + doc.getNumberOfPages());
                stream.endText();

                stream.close();
            }

            String outputFile = "BigQueryExport_" + LocalDate.now() + ".pdf";
            doc.save(outputFile);
            System.out.println("✅ PDF generated: " + outputFile);
        }
    }

    private static void drawCell(PDPageContentStream stream, float x, float y, float width, float height,
                                 List<String> lines, PDFont font, float fontSize) throws IOException {
        stream.setStrokingColor(0, 0, 0);
        stream.addRect(x, y, width, -height);
        stream.stroke();

        float textY = y - fontSize;
        for (String line : lines) {
            stream.beginText();
            stream.setFont(font, fontSize);
            stream.newLineAtOffset(x + 2, textY);
            stream.showText(line);
            stream.endText();
            textY -= fontSize + 2;
        }
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float width) throws IOException {
        List<String> lines = new ArrayList<>();
        StringBuilder line = new StringBuilder();
        for (String word : text.split(" ")) {
            if (font.getStringWidth(line + word) / 1000 * fontSize > width) {
                lines.add(line.toString());
                line = new StringBuilder();
            }
            line.append(word).append(" ");
        }
        lines.add(line.toString().trim());
        return lines;
    }
}
