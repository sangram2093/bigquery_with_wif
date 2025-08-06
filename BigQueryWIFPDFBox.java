package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.util.Matrix;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryWIFPDFRotatedEnglish {

    public static void main(String[] args) throws IOException, InterruptedException {

        // ====== CONFIG ======
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");
        String outputPdfPath = "bigquery_results_rotated_english.pdf";

        // ====== AUTH ======
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
                "SELECT exchange, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 " +
                "FROM `your_project.your_dataset.your_table` " +
                "ORDER BY exchange LIMIT 50";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(selectQuery)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(projectName, "WIF_QUERY_JOB_" + System.currentTimeMillis());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build()).waitFor();

        if (queryJob == null) throw new RuntimeException("Job no longer exists");
        if (queryJob.getStatus().getError() != null) throw new RuntimeException(queryJob.getStatus().getError().toString());

        TableResult results = queryJob.getQueryResults();

        // ====== GET COLUMN NAMES ======
        List<String> columnNames = new ArrayList<>();
        for (Field field : results.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        // ====== GROUP DATA BY EXCHANGE ======
        Map<String, List<List<String>>> groupedData = new LinkedHashMap<>();
        for (FieldValueList row : results.iterateAll()) {
            String exchange = row.get("exchange").isNull() ? "UNKNOWN" : row.get("exchange").getStringValue();
            List<String> rowData = new ArrayList<>();
            for (String colName : columnNames) {
                rowData.add(row.get(colName).isNull() ? "" : row.get(colName).getStringValue());
            }
            groupedData.computeIfAbsent(exchange, k -> new ArrayList<>()).add(rowData);
        }

        // ====== CREATE ROTATED PDF ======
        createRotatedPdf(outputPdfPath, groupedData, columnNames);
        System.out.println("Rotated English PDF created successfully: " + outputPdfPath);
    }

    private static void createRotatedPdf(String outputPath, Map<String, List<List<String>>> groupedData,
                                         List<String> columnNames) throws IOException {

        try (PDDocument document = new PDDocument()) {
            int pageCount = groupedData.size();
            int pageNum = 1;

            for (Map.Entry<String, List<List<String>>> entry : groupedData.entrySet()) {
                String exchange = entry.getKey();
                List<List<String>> rows = entry.getValue();

                PDPage page = new PDPage(PDRectangle.A4);
                document.addPage(page);

                try (PDPageContentStream cs = new PDPageContentStream(document, page)) {

                    // Translate to bottom left, then rotate
                    cs.transform(Matrix.getRotateInstance(Math.toRadians(90), 0, 0));
                    cs.transform(Matrix.getTranslateInstance(0, -page.getMediaBox().getWidth()));

                    float pageWidth = page.getMediaBox().getHeight(); // swapped after rotation
                    float pageHeight = page.getMediaBox().getWidth();

                    // ===== LEFT HEADER =====
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA_BOLD, 10);
                    cs.newLineAtOffset(20, pageWidth - 20);
                    cs.showText("Exchange: " + exchange);
                    cs.endText();

                    // ===== CENTER HEADER =====
                    String centerTitle = "BigQuery Report";
                    float titleWidth = PDType1Font.HELVETICA_BOLD.getStringWidth(centerTitle) / 1000 * 12;
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA_BOLD, 12);
                    cs.newLineAtOffset((pageHeight / 2) - (titleWidth / 2), pageWidth - 20);
                    cs.showText(centerTitle);
                    cs.endText();

                    // ===== FOOTER =====
                    String footer = "Page " + pageNum + " of " + pageCount;
                    float footerWidth = PDType1Font.HELVETICA.getStringWidth(footer) / 1000 * 8;
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA, 8);
                    cs.newLineAtOffset(pageHeight - footerWidth - 20, 10);
                    cs.showText(footer);
                    cs.endText();

                    // ===== TABLE =====
                    float margin = 20;
                    float yStart = pageWidth - 50;
                    float tableWidth = pageHeight - (2 * margin);
                    float rowHeight = 18;
                    float colWidth = tableWidth / columnNames.size();
                    float yPosition = yStart;

                    // Header row
                    cs.setFont(PDType1Font.HELVETICA_BOLD, 7);
                    for (int i = 0; i < columnNames.size(); i++) {
                        drawCell(cs, columnNames.get(i), margin + i * colWidth, yPosition, colWidth, rowHeight);
                    }
                    yPosition -= rowHeight;

                    // Data rows
                    cs.setFont(PDType1Font.HELVETICA, 7);
                    for (List<String> row : rows) {
                        for (int i = 0; i < row.size(); i++) {
                            drawCell(cs, row.get(i), margin + i * colWidth, yPosition, colWidth, rowHeight);
                        }
                        yPosition -= rowHeight;
                    }
                }
                pageNum++;
            }
            document.save(new File(outputPath));
        }
    }

    private static void drawCell(PDPageContentStream cs, String text,
                                 float x, float y, float width, float height) throws IOException {
        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();

        cs.beginText();
        cs.setFont(PDType1Font.HELVETICA, 6.5f);
        cs.newLineAtOffset(x + 2, y + 5);
        cs.showText(text == null ? "" : text);
        cs.endText();
    }
}
