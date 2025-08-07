package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.*;
import org.apache.pdfbox.pdmodel.PDPageContentStream;

import javax.net.ssl.*;
import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;

public class BigQueryWIF {

    static final PDFont HEADER_FONT = PDType1Font.HELVETICA_BOLD;
    static final PDFont CELL_FONT = PDType1Font.HELVETICA;
    static final float HEADER_FONT_SIZE = 7f;
    static final float CELL_FONT_SIZE = 6.5f;
    static final float PADDING = 3f;
    static final float LINE_SPACING = CELL_FONT_SIZE + 2;

    public static void main(String[] args) throws Exception {
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");

        String tokenEndpoint = "https://frtrasdsa10.cd.ab.com:9001/";
        String clientPemPath = "src/main/resources/certs/client.pem";
        String caChainPath = "src/main/resources/certs/ca_chain.crt";

        String accessTokenString = fetchTokenUsingMutualTLS(tokenEndpoint, clientPemPath, caChainPath);

        AccessToken accessToken = new AccessToken(accessTokenString, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectName)
                .setLocation(location)
                .build()
                .getService();

        String query = "SELECT exchange, client_name, order_id, price, quantity, symbol, timestamp, status, broker, region, strategy, notes FROM `your_dataset.your_table`";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        JobId jobId = JobId.of(projectName, "QUERY_" + System.currentTimeMillis());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();

        if (queryJob == null || queryJob.getStatus().getError() != null)
            throw new RuntimeException("Query failed: " + (queryJob == null ? "Job is null" : queryJob.getStatus().getError()));

        TableResult results = queryJob.getQueryResults();

        List<String> columnNames = new ArrayList<>();
        for (Field field : results.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        Map<String, List<List<String>>> dataByExchange = new LinkedHashMap<>();
        for (FieldValueList row : results.iterateAll()) {
            String exchange = row.get("exchange").getStringValue();
            List<String> rowData = new ArrayList<>();
            for (String col : columnNames) {
                rowData.add(row.get(col).isNull() ? "" : row.get(col).getValue().toString());
            }
            dataByExchange.computeIfAbsent(exchange, k -> new ArrayList<>()).add(rowData);
        }

        Map<String, Float> colWidths = ColumnConfigLoader.loadColumnWidths();
        generatePdf(dataByExchange, columnNames, colWidths, "bigquery_output.pdf");
    }

    public static String fetchTokenUsingMutualTLS(String url, String clientPemPath, String caChainPath) throws Exception {
        SSLContext sslContext = createSSLContext(clientPemPath, caChainPath);

        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

        URL endpoint = new URL(url);
        HttpsURLConnection conn = (HttpsURLConnection) endpoint.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoInput(true);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            return br.readLine().trim();
        }
    }

    public static SSLContext createSSLContext(String clientPemPath, String caChainPath) throws Exception {
        // Convert PEM to KeyStore
        Path certPath = Paths.get(clientPemPath);
        Path caPath = Paths.get(caChainPath);
        String certContent = Files.readString(certPath);
        String caContent = Files.readString(caPath);

        KeyManagerFactory kmf = PemUtils.createKeyManagerFactory(certContent);
        TrustManagerFactory tmf = PemUtils.createTrustManagerFactory(caContent);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return ctx;
    }

    public static void generatePdf(Map<String, List<List<String>>> dataByExchange,
                                   List<String> columnNames,
                                   Map<String, Float> fixedWidths,
                                   String outputFile) throws IOException {

        PDDocument document = new PDDocument();
        List<String> exchanges = new ArrayList<>(dataByExchange.keySet());
        int totalPages = exchanges.size();
        int currentPage = 1;

        for (String exchange : exchanges) {
            List<List<String>> rows = dataByExchange.get(exchange);
            PDPage page = new PDPage(new PDRectangle(PDRectangle.A4.getHeight(), PDRectangle.A4.getWidth()));
            document.addPage(page);
            PDPageContentStream cs = new PDPageContentStream(document, page);
            PDRectangle mediaBox = page.getMediaBox();

            float margin = 40;
            float yStart = mediaBox.getHeight() - 40;
            float yPosition = yStart;

            drawHeaderAndFooter(cs, mediaBox, exchange, currentPage, totalPages);

            float maxHeaderHeight = 0;
            List<List<String>> wrappedHeaderLines = new ArrayList<>();
            for (String col : columnNames) {
                float colWidth = fixedWidths.get(col);
                List<String> lines = wrapText(col, HEADER_FONT, HEADER_FONT_SIZE, colWidth - 2 * PADDING);
                wrappedHeaderLines.add(lines);
                maxHeaderHeight = Math.max(maxHeaderHeight, lines.size() * LINE_SPACING + 2 * PADDING);
            }

            float xPosition = margin;
            for (int i = 0; i < columnNames.size(); i++) {
                float colWidth = fixedWidths.get(columnNames.get(i));
                drawWrappedCell(cs, wrappedHeaderLines.get(i), xPosition, yPosition - maxHeaderHeight, colWidth, maxHeaderHeight, HEADER_FONT, HEADER_FONT_SIZE);
                xPosition += colWidth;
            }
            yPosition -= maxHeaderHeight;

            for (List<String> row : rows) {
                List<List<String>> wrappedCells = new ArrayList<>();
                float maxRowHeight = 0;

                for (int i = 0; i < columnNames.size(); i++) {
                    float colWidth = fixedWidths.get(columnNames.get(i));
                    String value = i < row.size() ? row.get(i) : "";
                    List<String> lines = wrapText(value, CELL_FONT, CELL_FONT_SIZE, colWidth - 2 * PADDING);
                    wrappedCells.add(lines);
                    maxRowHeight = Math.max(maxRowHeight, lines.size() * LINE_SPACING + 2 * PADDING);
                }

                if (yPosition - maxRowHeight < 60) break;

                xPosition = margin;
                for (int i = 0; i < columnNames.size(); i++) {
                    float colWidth = fixedWidths.get(columnNames.get(i));
                    drawWrappedCell(cs, wrappedCells.get(i), xPosition, yPosition - maxRowHeight, colWidth, maxRowHeight, CELL_FONT, CELL_FONT_SIZE);
                    xPosition += colWidth;
                }

                yPosition -= maxRowHeight;
            }

            cs.close();
            currentPage++;
        }

        document.save(outputFile);
        document.close();
    }

    private static void drawHeaderAndFooter(PDPageContentStream cs, PDRectangle mediaBox, String exchange, int currentPage, int totalPages) throws IOException {
        float margin = 40;

        cs.beginText();
        cs.setFont(HEADER_FONT, 9);
        cs.newLineAtOffset(margin, mediaBox.getHeight() - 25);
        cs.showText("Exchange: " + exchange);
        cs.endText();

        String title = "BigQuery Export - " + exchange;
        float titleWidth = getTextWidth(title, HEADER_FONT, 9);
        cs.beginText();
        cs.setFont(HEADER_FONT, 9);
        cs.newLineAtOffset((mediaBox.getWidth() - titleWidth) / 2, mediaBox.getHeight() - 25);
        cs.showText(title);
        cs.endText();

        String footer = "Page " + currentPage + " of " + totalPages;
        float footerWidth = getTextWidth(footer, CELL_FONT, 8);
        cs.beginText();
        cs.setFont(CELL_FONT, 8);
        cs.newLineAtOffset(mediaBox.getWidth() - footerWidth - margin, 20);
        cs.showText(footer);
        cs.endText();
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        if (text == null || text.trim().isEmpty()) return lines;
        String[] words = text.split("\\s+");
        StringBuilder line = new StringBuilder();
        for (String word : words) {
            String testLine = line.length() == 0 ? word : line + " " + word;
            if (getTextWidth(testLine, font, fontSize) > maxWidth) {
                if (line.length() > 0) lines.add(line.toString());
                line = new StringBuilder(word);
            } else {
                line = new StringBuilder(testLine);
            }
        }
        if (line.length() > 0) lines.add(line.toString());
        return lines;
    }

    private static void drawWrappedCell(PDPageContentStream cs, List<String> lines, float x, float y, float width, float height, PDFont font, float fontSize) throws IOException {
        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();
        float textY = y + height - PADDING - fontSize;
        for (String line : lines) {
            if (textY < y + PADDING) break;
            cs.beginText();
            cs.setFont(font, fontSize);
            cs.newLineAtOffset(x + PADDING, textY);
            cs.showText(line);
            cs.endText();
            textY -= LINE_SPACING;
        }
    }

    private static float getTextWidth(String text, PDFont font, float fontSize) throws IOException {
        return font.getStringWidth(text) / 1000 * fontSize;
    }
}
