package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryWIFPDFBox {

    public static void main(String[] args) throws IOException, InterruptedException {

        // ====== CONFIGURATION ======
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION"); // e.g., "us"
        String outputPdfPath = "bigquery_results.pdf";

        // ====== AUTHENTICATION ======
        String accessTokenString = Files.readString(Paths.get(wifTokenFilename)).trim();
        AccessToken accessToken = new AccessToken(accessTokenString, null);
        GoogleCredentials credentials = GoogleCredentials.create(accessToken)
                .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));

        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(projectName)
                .setLocation(location)
                .build()
                .getService();

        // ====== QUERY ======
        String selectQuery =
                "SELECT exchange, symbol, trade_date, price, volume " +
                "FROM `your_project.your_dataset.your_table` " +
                "ORDER BY exchange, symbol LIMIT 50";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(selectQuery)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(projectName, "WIF_QUERY_JOB_" + System.currentTimeMillis());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        TableResult results = queryJob.getQueryResults();

        // ====== GROUP DATA BY EXCHANGE ======
        List<String> columnNames = new ArrayList<>();
        for (Field field : results.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        Map<String, List<List<String>>> groupedData = new LinkedHashMap<>();

        for (FieldValueList row : results.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN" : row.get("exchange").getStringValue();

            List<String> rowData = new ArrayList<>();
            for (String colName : columnNames) {
                rowData.add(row.get(colName).isNull() ? "" : row.get(colName).getStringValue());
            }

            groupedData.computeIfAbsent(exchange, k -> new ArrayList<>()).add(rowData);
        }

        // ====== CREATE PDF ======
        createPdf(outputPdfPath, groupedData, columnNames);

        System.out.println("PDF created successfully: " + outputPdfPath);
    }

    private static void createPdf(String outputPath, Map<String, List<List<String>>> groupedData,
                                  List<String> columnNames) throws IOException {

        try (PDDocument document = new PDDocument()) {

            for (Map.Entry<String, List<List<String>>> entry : groupedData.entrySet()) {
                String exchange = entry.getKey();
                List<List<String>> rows = entry.getValue();

                PDPage page = new PDPage(PDRectangle.A4);
                document.addPage(page);

                try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                    contentStream.setFont(PDType1Font.HELVETICA_BOLD, 14);
                    contentStream.beginText();
                    contentStream.newLineAtOffset(50, 770);
                    contentStream.showText("Exchange: " + exchange);
                    contentStream.endText();

                    float margin = 50;
                    float yStart = 740;
                    float tableWidth = page.getMediaBox().getWidth() - (2 * margin);
                    float yPosition = yStart;
                    float rowHeight = 20;
                    float tableHeight = rowHeight * (rows.size() + 1); // +1 for header
                    float colWidth = tableWidth / columnNames.size();

                    // ===== Draw Header =====
                    contentStream.setFont(PDType1Font.HELVETICA_BOLD, 10);
                    for (int i = 0; i < columnNames.size(); i++) {
                        drawCell(contentStream, columnNames.get(i), margin + i * colWidth, yPosition, colWidth, rowHeight);
                    }
                    yPosition -= rowHeight;

                    // ===== Draw Rows =====
                    contentStream.setFont(PDType1Font.HELVETICA, 10);
                    for (List<String> row : rows) {
                        for (int i = 0; i < row.size(); i++) {
                            drawCell(contentStream, row.get(i), margin + i * colWidth, yPosition, colWidth, rowHeight);
                        }
                        yPosition -= rowHeight;
                    }
                }
            }

            document.save(outputPath);
        }
    }

    private static void drawCell(PDPageContentStream contentStream, String text,
                                 float x, float y, float width, float height) throws IOException {

        // Draw cell border
        contentStream.setLineWidth(0.5f);
        contentStream.addRect(x, y, width, height);
        contentStream.stroke();

        // Add text
        contentStream.beginText();
        contentStream.setFont(PDType1Font.HELVETICA, 9);
        contentStream.newLineAtOffset(x + 2, y + 5); // Small padding
        contentStream.showText(text == null ? "" : text);
        contentStream.endText();
    }
}
