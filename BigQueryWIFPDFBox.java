package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryPDFLandscapeFixed {

    public static void main(String[] args) throws IOException, InterruptedException {

        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");
        String outputPdfPath = "bigquery_results_landscape_fixed.pdf";

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

        generateLandscapePdf(outputPdfPath, groupedData, columnNames);
        System.out.println("✅ Landscape PDF with fixed column overlap generated: " + outputPdfPath);
    }

    private static void generateLandscapePdf(String outputPath, Map<String, List<List<String>>> groupedData,
                                             List<String> columnNames) throws IOException {

        try (PDDocument document = new PDDocument()) {
            int totalPages = groupedData.size();
            int currentPage = 1;

            for (Map.Entry<String, List<List<String>>> entry : groupedData.entrySet()) {
                String exchange = entry.getKey();
                List<List<String>> rows = entry.getValue();

                PDRectangle landscape = new PDRectangle(PDRectangle.A4.getHeight(), PDRectangle.A4.getWidth());
                PDPage page = new PDPage(landscape);
                document.addPage(page);

                try (PDPageContentStream cs = new PDPageContentStream(document, page)) {
                    float pageWidth = landscape.getWidth();
                    float pageHeight = landscape.getHeight();

                    // ===== HEADERS =====
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA_BOLD, 10);
                    cs.newLineAtOffset(40, pageHeight - 30);
                    cs.showText("Exchange: " + exchange);
                    cs.endText();

                    String title = "BigQuery Report";
                    float titleWidth = PDType1Font.HELVETICA_BOLD.getStringWidth(title) / 1000 * 12;
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA_BOLD, 12);
                    cs.newLineAtOffset((pageWidth / 2) - (titleWidth / 2), pageHeight - 30);
                    cs.showText(title);
                    cs.endText();

                    // ===== FOOTER =====
                    String footer = "Page " + currentPage + " of " + totalPages;
                    float footerWidth = PDType1Font.HELVETICA.getStringWidth(footer) / 1000 * 8;
                    cs.beginText();
                    cs.setFont(PDType1Font.HELVETICA, 8);
                    cs.newLineAtOffset(pageWidth - footerWidth - 20, 20);
                    cs.showText(footer);
                    cs.endText();

                    // ===== TABLE =====
                    float margin = 50; // Increased margin for better spacing
                    float yStart = pageHeight - 60;
                    float tableWidth = pageWidth - 2 * margin;
                    float rowHeight = 18;
                    float padding = 4; // Extra space inside cells
                    float colWidth = tableWidth / columnNames.size();
                    float yPosition = yStart;

                    // Header Row
                    for (int i = 0; i < columnNames.size(); i++) {
                        drawCell(cs, columnNames.get(i), margin + i * colWidth, yPosition,
                                colWidth, rowHeight, PDType1Font.HELVETICA_BOLD, 7, padding);
                    }
                    yPosition -= rowHeight;

                    // Data Rows
                    for (List<String> row : rows) {
                        for (int i = 0; i < row.size(); i++) {
                            drawCell(cs, row.get(i), margin + i * colWidth, yPosition,
                                    colWidth, rowHeight, PDType1Font.HELVETICA, 6.5f, padding);
                        }
                        yPosition -= rowHeight;
                    }
                }
                currentPage++;
            }

            document.save(outputPath);
        }
    }

    private static void drawCell(PDPageContentStream cs, String text,
                                 float x, float y, float width, float height,
                                 PDFont font, float fontSize, float padding) throws IOException {

        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();

        // Truncate text if too long
        String displayText = fitTextToWidth(text == null ? "" : text, font, fontSize, width - 2 * padding);

        cs.beginText();
        cs.setFont(font, fontSize);
        cs.newLineAtOffset(x + padding, y + 5);
        cs.showText(displayText);
        cs.endText();
    }

    private static String fitTextToWidth(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        if (font.getStringWidth(text) / 1000 * fontSize <= maxWidth) {
            return text;
        }

        String ellipsis = "...";
        float ellipsisWidth = font.getStringWidth(ellipsis) / 1000 * fontSize;
        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray()) {
            sb.append(c);
            float width = font.getStringWidth(sb.toString()) / 1000 * fontSize;
            if (width + ellipsisWidth > maxWidth) {
                return sb.substring(0, sb.length() - 1) + ellipsis;
            }
        }
        return sb.toString();
    }
}
