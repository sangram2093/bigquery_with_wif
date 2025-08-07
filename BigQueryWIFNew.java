package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPage;

public class BigQueryWIFPdfExporter {

    static final String WIF_ENDPOINT = "https://frtrasdsa10.cd.ab.com:9001/";
    static final String WIF_HOME = System.getenv("WIF_HOME");
    static final String CLIENT_PEM_PATH = WIF_HOME + "/client.pem";
    static final String CA_CERT_PATH = WIF_HOME + "/ca_chain.crt";
    static final String CONFIG_YAML_PATH = "resources/config.yaml";

    public static void main(String[] args) throws Exception {
        String token = getTokenFromSecureEndpoint();

        AccessToken accessToken = new AccessToken(token, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(System.getenv("PROJECT_NAME"))
                .setLocation(System.getenv("LOCATION"))
                .build()
                .getService();

        String query = "SELECT client_order_id, exchange, trader, status FROM `db-dev-rlvd-cag-001-1.cag_bq.japan_client_order` LIMIT 100";
        TableResult result = bigquery.query(QueryJobConfiguration.newBuilder(query).build());

        Map<String, List<FieldValueList>> grouped = new LinkedHashMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN" : row.get("exchange").getStringValue();
            grouped.computeIfAbsent(exchange, k -> new ArrayList<>()).add(row);
        }

        Map<String, Integer> colWidths = loadColumnWidths(CONFIG_YAML_PATH);
        generatePdf(grouped, result.getSchema().getFields(), colWidths);
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
        conn.setSSLSocketFactory(sslContext.getSocketFactory());
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

    private static Map<String, Integer> loadColumnWidths(String configPath) throws IOException {
        try (InputStream input = new FileInputStream(configPath)) {
            Yaml yaml = new Yaml();
            Map<String, Object> obj = yaml.load(input);
            Map<String, Integer> widths = new HashMap<>();
            Map<String, Object> columnMap = (Map<String, Object>) obj.get("columnWidths");
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                widths.put(entry.getKey(), (Integer) entry.getValue());
            }
            return widths;
        }
    }

    private static void generatePdf(Map<String, List<FieldValueList>> data,
                                    FieldList fields,
                                    Map<String, Integer> colWidths) throws IOException {

        PDFont font = PDType1Font.HELVETICA;
        PDFont boldFont = PDType1Font.HELVETICA_BOLD;
        float fontSize = 8f;
        float leading = 1.5f * fontSize;

        PDDocument doc = new PDDocument();
        int pageNumber = 1;
        int totalPages = data.size();

        for (Map.Entry<String, List<FieldValueList>> entry : data.entrySet()) {
            PDPage page = new PDPage(new PDRectangle(PDRectangle.LETTER.getHeight(), PDRectangle.LETTER.getWidth())); // Landscape
            doc.addPage(page);

            try (PDPageContentStream content = new PDPageContentStream(doc, page)) {

                float margin = 50;
                float yStart = page.getMediaBox().getHeight() - margin;
                float yPosition = yStart;

                // Header Left
                content.beginText();
                content.setFont(font, 10);
                content.newLineAtOffset(margin, yPosition);
                content.showText("Exchange: " + entry.getKey());
                content.endText();

                // Header Center
                content.beginText();
                content.setFont(boldFont, 12);
                float centerX = page.getMediaBox().getWidth() / 2;
                content.newLineAtOffset(centerX - 60, yPosition);
                content.showText("BigQuery Data Export");
                content.endText();

                yPosition -= 30;

                List<String> headers = fields.stream().map(Field::getName).collect(Collectors.toList());
                float maxHeaderHeight = 0;

                // Measure header height first
                for (String header : headers) {
                    float colWidth = colWidths.getOrDefault(header, 60);
                    List<String> headerLines = wrapText(header, boldFont, fontSize, colWidth - 4);
                    float height = headerLines.size() * leading + 4;
                    if (height > maxHeaderHeight) maxHeaderHeight = height;
                }

                // Draw header row
                float xPosition = margin;
                for (String header : headers) {
                    float colWidth = colWidths.getOrDefault(header, 60);
                    drawCell(content, xPosition, yPosition, colWidth, maxHeaderHeight, header, boldFont, fontSize);
                    xPosition += colWidth;
                }

                yPosition -= maxHeaderHeight;

                // Draw data rows
                for (FieldValueList row : entry.getValue()) {
                    float maxRowHeight = 0;
                    for (String col : headers) {
                        float colWidth = colWidths.getOrDefault(col, 60);
                        String text = row.get(col).isNull() ? "" : row.get(col).getStringValue();
                        List<String> lines = wrapText(text, font, fontSize, colWidth - 4);
                        float height = lines.size() * leading + 4;
                        if (height > maxRowHeight) maxRowHeight = height;
                    }

                    xPosition = margin;
                    for (String col : headers) {
                        float colWidth = colWidths.getOrDefault(col, 60);
                        String text = row.get(col).isNull() ? "" : row.get(col).getStringValue();
                        drawCell(content, xPosition, yPosition, colWidth, maxRowHeight, text, font, fontSize);
                        xPosition += colWidth;
                    }

                    yPosition -= maxRowHeight;
                }

                // Footer
                content.beginText();
                content.setFont(font, 10);
                content.newLineAtOffset(page.getMediaBox().getWidth() - 100, 20);
                content.showText("Page " + pageNumber + " of " + totalPages);
                content.endText();
                pageNumber++;
            }
        }

        doc.save("BigQueryExport.pdf");
        doc.close();
        System.out.println("✅ PDF saved as BigQueryExport.pdf");
    }

    private static void drawCell(PDPageContentStream content, float x, float y,
                                 float width, float height, String text,
                                 PDFont font, float fontSize) throws IOException {

        content.setStrokingColor(0, 0, 0);
        content.addRect(x, y - height, width, height);
        content.stroke();

        List<String> lines = wrapText(text, font, fontSize, width - 4);

        content.beginText();
        content.setFont(font, fontSize);
        content.newLineAtOffset(x + 2, y - fontSize - 2);
        for (String line : lines) {
            content.showText(line);
            content.newLineAtOffset(0, -1.5f * fontSize);
        }
        content.endText();
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        String[] words = text.split("\\s+");
        StringBuilder line = new StringBuilder();

        for (String word : words) {
            String candidate = line.length() == 0 ? word : line + " " + word;
            float size = font.getStringWidth(candidate) / 1000 * fontSize;
            if (size > maxWidth) {
                if (!line.toString().isEmpty()) {
                    lines.add(line.toString());
                }
                line = new StringBuilder(word);
            } else {
                line = new StringBuilder(candidate);
            }
        }
        if (!line.toString().isEmpty()) {
            lines.add(line.toString());
        }
        return lines;
    }
}
