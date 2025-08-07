package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.util.*;

public class BigQueryWIFPDFBoxJp {

    private static final String WIF_ENDPOINT = "https://frtrasdsa10.cd.ab.com:9001/";
    private static final String CLIENT_PEM_PATH = "resources/client.pem";
    private static final String CA_CERT_PATH = "resources/ca_chain.crt";
    private static final String FONT_PATH = "resources/fonts/NotoSansJP-Regular.ttf";
    private static final String CONFIG_YAML = "resources/config.yaml";
    private static final float PAGE_WIDTH = PDRectangle.LETTER.getHeight();  // Landscape
    private static final float PAGE_HEIGHT = PDRectangle.LETTER.getWidth();
    private static final float MARGIN = 40;
    private static final float FONT_SIZE = 10;
    private static final float CELL_PADDING = 5;
    private static final float LINE_HEIGHT = FONT_SIZE + 2;

    public static void main(String[] args) throws Exception {

        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");

        String accessTokenString = getTokenFromSecureEndpoint();

        AccessToken accessToken = new AccessToken(accessTokenString, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectName)
                .setLocation(location)
                .build()
                .getService();

        String query = "SELECT exchange, client_order_id, trader, status, message, instruction, updated_at, strategy, symbol, quantity, price, venue FROM `your-project.dataset.table`";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        JobId jobId = JobId.of(projectName, "WIF_QUERY_JOB_" + System.currentTimeMillis());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        TableResult result = queryJob.getQueryResults();
        FieldList fields = result.getSchema().getFields();

        Map<String, List<FieldValueList>> groupedData = new TreeMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN" : row.get("exchange").getStringValue();
            groupedData.computeIfAbsent(exchange, k -> new ArrayList<>()).add(row);
        }

        Map<String, Integer> colWidths = loadColumnWidths();
        generatePdf(groupedData, fields, colWidths);
    }

    private static String getTokenFromSecureEndpoint() throws Exception {
        URL url = new URL(WIF_ENDPOINT);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        SSLContextBuilder sslContextBuilder = new SSLContextBuilder(CLIENT_PEM_PATH, CA_CERT_PATH);
        conn.setSSLSocketFactory(sslContextBuilder.getSSLContext().getSocketFactory());
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

    private static Map<String, Integer> loadColumnWidths() throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(CONFIG_YAML))) {
            Map<String, Object> config = yaml.load(in);
            Map<String, Object> widthMap = (Map<String, Object>) config.get("columnWidths");
            Map<String, Integer> colWidths = new HashMap<>();
            for (Map.Entry<String, Object> entry : widthMap.entrySet()) {
                colWidths.put(entry.getKey(), (Integer) entry.getValue());
            }
            return colWidths;
        }
    }

    private static void generatePdf(Map<String, List<FieldValueList>> data,
                                    FieldList fields,
                                    Map<String, Integer> colWidths) throws IOException {

        try (PDDocument doc = new PDDocument()) {

            PDFont font = PDType0Font.load(doc, new File(FONT_PATH));  // ✅ Japanese font
            PDFont boldFont = font;

            int pageNumber = 1;
            int totalPages = data.size();

            for (Map.Entry<String, List<FieldValueList>> entry : data.entrySet()) {
                String exchange = entry.getKey();
                List<FieldValueList> rows = entry.getValue();

                PDPage page = new PDPage(new PDRectangle(PAGE_WIDTH, PAGE_HEIGHT));
                doc.addPage(page);

                try (PDPageContentStream contentStream = new PDPageContentStream(doc, page)) {

                    contentStream.setFont(font, FONT_SIZE);
                    contentStream.beginText();
                    contentStream.setFont(boldFont, FONT_SIZE + 2);
                    contentStream.newLineAtOffset(PAGE_WIDTH / 2 - 60, PAGE_HEIGHT - MARGIN + 5);
                    contentStream.showText("Exchange: " + exchange);
                    contentStream.endText();

                    contentStream.beginText();
                    contentStream.setFont(font, FONT_SIZE);
                    contentStream.newLineAtOffset(MARGIN, PAGE_HEIGHT - MARGIN + 5);
                    contentStream.showText("Report");
                    contentStream.endText();

                    contentStream.beginText();
                    contentStream.setFont(font, FONT_SIZE);
                    contentStream.newLineAtOffset(PAGE_WIDTH - 100, MARGIN - 15);
                    contentStream.showText("Page " + pageNumber + " of " + totalPages);
                    contentStream.endText();

                    float startY = PAGE_HEIGHT - MARGIN - 20;
                    float tableTopY = startY;
                    float tableX = MARGIN;
                    float rowY = tableTopY;

                    List<String> headers = new ArrayList<>();
                    for (Field field : fields) {
                        headers.add(field.getName());
                    }

                    float rowHeight = computeMaxRowHeight(headers, font, colWidths);

                    // Draw header row
                    float x = tableX;
                    for (String col : headers) {
                        float colWidth = colWidths.getOrDefault(col, 60);
                        drawWrappedCell(contentStream, boldFont, FONT_SIZE, col, x, rowY, colWidth, rowHeight);
                        x += colWidth;
                    }

                    rowY -= rowHeight;

                    // Draw data rows
                    for (FieldValueList row : rows) {
                        float maxRowHeight = computeMaxRowHeightForRow(row, headers, font, colWidths);

                        x = tableX;
                        for (String col : headers) {
                            float colWidth = colWidths.getOrDefault(col, 60);
                            String text = row.get(col).isNull() ? "" : row.get(col).getValue().toString();
                            drawWrappedCell(contentStream, font, FONT_SIZE, text, x, rowY, colWidth, maxRowHeight);
                            x += colWidth;
                        }
                        rowY -= maxRowHeight;
                    }
                }

                pageNumber++;
            }

            doc.save("BigQuery_Report_" + LocalDate.now() + ".pdf");
            System.out.println("✅ PDF exported successfully.");
        }
    }

    private static void drawWrappedCell(PDPageContentStream contentStream, PDFont font, float fontSize,
                                        String text, float x, float y, float width, float height) throws IOException {

        List<String> lines = wrapText(text, font, fontSize, width - 2 * CELL_PADDING);
        float leading = LINE_HEIGHT;
        float startY = y - CELL_PADDING - fontSize;

        contentStream.setFont(font, fontSize);
        for (String line : lines) {
            contentStream.beginText();
            contentStream.newLineAtOffset(x + CELL_PADDING, startY);
            contentStream.showText(line);
            contentStream.endText();
            startY -= leading;
        }
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        StringBuilder line = new StringBuilder();

        for (String word : text.split(" ")) {
            String testLine = line + (line.length() == 0 ? "" : " ") + word;
            float width = font.getStringWidth(testLine) / 1000 * fontSize;
            if (width > maxWidth) {
                if (!line.isEmpty()) lines.add(line.toString());
                line = new StringBuilder(word);
            } else {
                line = new StringBuilder(testLine);
            }
        }

        if (!line.isEmpty()) lines.add(line.toString());
        return lines;
    }

    private static float computeMaxRowHeight(List<String> texts, PDFont font, Map<String, Integer> colWidths) throws IOException {
        float maxHeight = 0;
        for (String col : texts) {
            float colWidth = colWidths.getOrDefault(col, 60);
            List<String> lines = wrapText(col, font, FONT_SIZE, colWidth - 2 * CELL_PADDING);
            maxHeight = Math.max(maxHeight, lines.size() * LINE_HEIGHT + 2 * CELL_PADDING);
        }
        return maxHeight;
    }

    private static float computeMaxRowHeightForRow(FieldValueList row, List<String> headers, PDFont font, Map<String, Integer> colWidths) throws IOException {
        float maxHeight = 0;
        for (String col : headers) {
            float colWidth = colWidths.getOrDefault(col, 60);
            String text = row.get(col).isNull() ? "" : row.get(col).getValue().toString();
            List<String> lines = wrapText(text, font, FONT_SIZE, colWidth - 2 * CELL_PADDING);
            maxHeight = Math.max(maxHeight, lines.size() * LINE_HEIGHT + 2 * CELL_PADDING);
        }
        return maxHeight;
    }
}
