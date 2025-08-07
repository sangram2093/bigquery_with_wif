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
import java.util.stream.Collectors;

import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDPage;

public class BigQueryWIFPDFBoxNew {

    // ─────────────────────────────────────────────────────────────────────────────
    //  CONSTANTS & CONFIG
    // ─────────────────────────────────────────────────────────────────────────────
    static final String WIF_ENDPOINT   = "https://frtrasdsa10.cd.ab.com:9001/";
    static final String WIF_HOME       = System.getenv("WIF_HOME");
    static final String CLIENT_PEM_PATH = WIF_HOME + "/client.pem";
    static final String CA_CERT_PATH    = WIF_HOME + "/ca_chain.crt";
    static final String CONFIG_YAML_PATH = "resources/config.yaml";

    // ─────────────────────────────────────────────────────────────────────────────
    //  MAIN
    // ─────────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        // 1) Retrieve WIF access-token via mTLS
        String token = getTokenFromSecureEndpoint();
        AccessToken accessToken = new AccessToken(token, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        // 2) BigQuery client
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(System.getenv("PROJECT_NAME"))
                .setLocation(System.getenv("LOCATION"))
                .build()
                .getService();

        // 3) Query data
        String query =
            "SELECT client_order_id, exchange, trader, status " +
            "FROM `db-dev-rlvd-cag-001-1.cag_bq.japan_client_order` " +
            "LIMIT 100";
        TableResult result = bigquery.query(QueryJobConfiguration.newBuilder(query).build());

        // 4) Group rows by exchange (each exchange ➜ new page)
        Map<String, List<FieldValueList>> grouped = new LinkedHashMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN"
                                                           : row.get("exchange").getStringValue();
            grouped.computeIfAbsent(exchange, k -> new ArrayList<>()).add(row);
        }

        // 5) Column widths (points) from YAML
        Map<String, Integer> colWidths = loadColumnWidths(CONFIG_YAML_PATH);

        // 6) Generate the PDF
        generatePdf(grouped, result.getSchema().getFields(), colWidths);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  SECURE-ENDPOINT TOKEN
    // ─────────────────────────────────────────────────────────────────────────────
    private static String getTokenFromSecureEndpoint() throws Exception {
        String clientPem = Files.readString(Paths.get(CLIENT_PEM_PATH));
        String caCert    = Files.readString(Paths.get(CA_CERT_PATH));

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

        if (conn.getResponseCode() != 200) {
            throw new RuntimeException("Failed to retrieve token: " + conn.getResponseCode());
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return new ObjectMapper().readTree(reader).get("access_token").asText();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  YAML COLUMN WIDTHS
    // ─────────────────────────────────────────────────────────────────────────────
    private static Map<String, Integer> loadColumnWidths(String configPath) throws IOException {
        try (InputStream in = new FileInputStream(configPath)) {
            Yaml yaml = new Yaml();
            Map<String, Integer> widths = new HashMap<>();
            Map<String, Object> root = yaml.load(in);
            Map<String, Object> columns = (Map<String, Object>) root.get("columnWidths");
            columns.forEach((k, v) -> widths.put(k, (Integer) v));
            return widths;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  PDF GENERATION
    // ─────────────────────────────────────────────────────────────────────────────
    private static void generatePdf(Map<String, List<FieldValueList>> data,
                                    FieldList fields,
                                    Map<String, Integer> colWidths) throws IOException {

        PDFont font      = PDType1Font.HELVETICA;
        PDFont boldFont  = PDType1Font.HELVETICA_BOLD;
        float  fontSize  = 8f;
        float  leading   = 1.5f * fontSize;

        PDDocument doc   = new PDDocument();
        int pageNumber   = 1;
        int totalPages   = data.size();

        // Each exchange ➜ own page
        for (Map.Entry<String, List<FieldValueList>> entry : data.entrySet()) {
            PDPage page = new PDPage(
                new PDRectangle(PDRectangle.LETTER.getHeight(), PDRectangle.LETTER.getWidth())); // Landscape
            doc.addPage(page);

            try (PDPageContentStream content = new PDPageContentStream(doc, page)) {

                float margin   = 50;
                float yStart   = page.getMediaBox().getHeight() - margin;
                float yPos     = yStart;

                // ── HEADER ────────────────────────────────────
                content.beginText();
                content.setFont(font, 10);
                content.newLineAtOffset(margin, yPos);
                content.showText("Exchange: " + entry.getKey());
                content.endText();

                content.beginText();
                content.setFont(boldFont, 12);
                float centerX = page.getMediaBox().getWidth() / 2;
                content.newLineAtOffset(centerX - 60, yPos);
                content.showText("BigQuery Data Export");
                content.endText();

                yPos -= 30;

                // Column headers
                List<String> headers = fields.stream()
                                             .map(Field::getName)
                                             .collect(Collectors.toList());

                // Header height (wrapped text)
                float maxHeaderH = 0;
                for (String h : headers) {
                    float w = colWidths.getOrDefault(h, 60);
                    List<String> lines = wrapText(h, boldFont, fontSize, w - 4);
                    maxHeaderH = Math.max(maxHeaderH, lines.size() * leading + 4);
                }

                // Draw header row
                float xPos = margin;
                for (String h : headers) {
                    float w = colWidths.getOrDefault(h, 60);
                    drawCell(content, xPos, yPos, w, maxHeaderH,
                             wrapText(h, boldFont, fontSize, w - 4), boldFont, fontSize);
                    xPos += w;
                }
                yPos -= maxHeaderH;

                // ── DATA ROWS ────────────────────────────────
                for (FieldValueList row : entry.getValue()) {

                    float maxRowH = 0;
                    Map<String, List<String>> lineMap = new HashMap<>();

                    for (String col : headers) {
                        float w   = colWidths.getOrDefault(col, 60);
                        String txt = row.get(col).isNull() ? "" : row.get(col).getStringValue();
                        List<String> wrapped = wrapText(txt, font, fontSize, w - 4);
                        lineMap.put(col, wrapped);
                        maxRowH = Math.max(maxRowH, wrapped.size() * leading + 4);
                    }

                    xPos = margin;
                    for (String col : headers) {
                        float w = colWidths.getOrDefault(col, 60);
                        drawCell(content, xPos, yPos, w, maxRowH,
                                 lineMap.get(col), font, fontSize);
                        xPos += w;
                    }
                    yPos -= maxRowH;
                }

                // ── FOOTER (page #) ──────────────────────────
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

    // ─────────────────────────────────────────────────────────────────────────────
    //  TABLE CELL RENDERING
    // ─────────────────────────────────────────────────────────────────────────────
    private static void drawCell(PDPageContentStream content,
                                 float x, float y,
                                 float width, float height,
                                 List<String> lines,
                                 PDFont font, float fontSize) throws IOException {

        // cell border
        content.setStrokingColor(0, 0, 0);
        content.addRect(x, y - height, width, height);
        content.stroke();

        // text
        content.beginText();
        content.setFont(font, fontSize);
        content.newLineAtOffset(x + 2, y - fontSize - 2);
        for (String l : lines) {
            content.showText(l);
            content.newLineAtOffset(0, -1.5f * fontSize);
        }
        content.endText();
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  TEXT WRAPPING  (★ NEW LOGIC ★)
    // ─────────────────────────────────────────────────────────────────────────────
    private static List<String> wrapText(String text, PDFont font,
                                         float fontSize, float maxWidth) throws IOException {

        List<String> out = new ArrayList<>();
        if (text == null || text.isEmpty()) return out;

        String[] words = text.split("\\s+");
        StringBuilder line = new StringBuilder();

        for (String word : words) {
            // If the word itself is wider than the column, split the word
            if (stringWidth(font, fontSize, word) > maxWidth) {
                // Flush current line before breaking the long word
                if (line.length() > 0) {
                    out.add(line.toString());
                    line.setLength(0);
                }
                StringBuilder segment = new StringBuilder();
                for (char c : word.toCharArray()) {
                    if (stringWidth(font, fontSize, segment.toString() + c) > maxWidth) {
                        out.add(segment.toString());
                        segment.setLength(0);
                    }
                    segment.append(c);
                }
                if (segment.length() > 0) {
                    // Remaining chars of the long word (may become start of next line)
                    line.append(segment);
                }
                continue;
            }

            // Normal word-wrapping
            String test = line.length() == 0 ? word : line + " " + word;
            if (stringWidth(font, fontSize, test) > maxWidth) {
                out.add(line.toString());
                line.setLength(0);
                line.append(word);
            } else {
                line.setLength(0);
                line.append(test);
            }
        }
        if (line.length() > 0) out.add(line.toString());
        return out;
    }

    // Helper: fast string width in points
    private static float stringWidth(PDFont font, float fontSize,
                                     String text) throws IOException {
        return font.getStringWidth(text) / 1000f * fontSize;
    }
}
