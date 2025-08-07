package org.example;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryPDFFinal {

    public static void main(String[] args) throws IOException, InterruptedException {
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");
        String outputPdfPath = "bigquery_final.pdf";

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

        generatePdfWithWrapping(outputPdfPath, groupedData, columnNames);
        System.out.println("✅ Final PDF generated with full wrapping and no overlaps.");
    }

    private static void generatePdfWithWrapping(String outputPath,
                                                Map<String, List<List<String>>> groupedData,
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
                    float margin = 50;
                    float usableWidth = pageWidth - 2 * margin;
                    float yStart = pageHeight - 60;
                    float yPosition = yStart;
                    float padding = 4;

                    PDFont headerFont = PDType1Font.HELVETICA_BOLD;
                    PDFont cellFont = PDType1Font.HELVETICA;
                    float headerFontSize = 7f;
                    float cellFontSize = 6.5f;

                    // Draw header title
                    cs.beginText();
                    cs.setFont(headerFont, 10);
                    cs.newLineAtOffset(40, pageHeight - 30);
                    cs.showText("Exchange: " + exchange);
                    cs.endText();

                    String title = "BigQuery Report";
                    float titleWidth = headerFont.getStringWidth(title) / 1000 * 12;
                    cs.beginText();
                    cs.setFont(headerFont, 12);
                    cs.newLineAtOffset((pageWidth / 2) - (titleWidth / 2), pageHeight - 30);
                    cs.showText(title);
                    cs.endText();

                    String footer = "Page " + currentPage + " of " + totalPages;
                    float footerWidth = cellFont.getStringWidth(footer) / 1000 * 8;
                    cs.beginText();
                    cs.setFont(cellFont, 8);
                    cs.newLineAtOffset(pageWidth - footerWidth - 20, 20);
                    cs.showText(footer);
                    cs.endText();

                    // Calculate max content width per column
                    Map<Integer, Float> columnWidths = new HashMap<>();
                    float totalContentWidth = 0;
                    for (int i = 0; i < columnNames.size(); i++) {
                        float maxWidth = getTextWidth(columnNames.get(i), headerFont, headerFontSize);
                        for (List<String> row : rows) {
                            maxWidth = Math.max(maxWidth,
                                    getTextWidth(row.get(i), cellFont, cellFontSize));
                        }
                        float finalWidth = maxWidth + 2 * padding;
                        columnWidths.put(i, finalWidth);
                        totalContentWidth += finalWidth;
                    }

                    // Scale if needed
                    float scale = totalContentWidth > usableWidth ? usableWidth / totalContentWidth : 1.0f;
                    for (int i : columnWidths.keySet()) {
                        columnWidths.put(i, columnWidths.get(i) * scale);
                    }

                    // ====== HEADER ROW ======
                    float maxHeaderHeight = 0;
                    float xPos = margin;
                    List<Float> colXPositions = new ArrayList<>();
                    for (int i = 0; i < columnNames.size(); i++) {
                        float colWidth = columnWidths.get(i);
                        List<String> wrappedHeader = wrapText(columnNames.get(i), headerFont, headerFontSize, colWidth - 2 * padding);
                        float headerHeight = wrappedHeader.size() * (headerFontSize + 2);
                        maxHeaderHeight = Math.max(maxHeaderHeight, headerHeight);
                        colXPositions.add(xPos);
                        drawWrappedCell(cs, columnNames.get(i), xPos, yPosition, colWidth, headerHeight,
                                headerFont, headerFontSize, padding);
                        xPos += colWidth;
                    }
                    yPosition -= maxHeaderHeight;

                    // ====== DATA ROWS ======
                    for (List<String> row : rows) {
                        float maxRowHeight = 0;
                        for (int i = 0; i < row.size(); i++) {
                            float colWidth = columnWidths.get(i);
                            List<String> wrapped = wrapText(row.get(i), cellFont, cellFontSize, colWidth - 2 * padding);
                            maxRowHeight = Math.max(maxRowHeight, wrapped.size() * (cellFontSize + 2));
                        }

                        for (int i = 0; i < row.size(); i++) {
                            float colWidth = columnWidths.get(i);
                            float x = colXPositions.get(i);
                            drawWrappedCell(cs, row.get(i), x, yPosition, colWidth, maxRowHeight,
                                    cellFont, cellFontSize, padding);
                        }

                        yPosition -= maxRowHeight;
                    }
                }

                currentPage++;
            }

            document.save(outputPath);
        }
    }

    private static float getTextWidth(String text, PDFont font, float fontSize) throws IOException {
        return font.getStringWidth(text == null ? "" : text) / 1000 * fontSize;
    }

    private static void drawWrappedCell(PDPageContentStream cs, String text, float x, float y, float width,
                                        float height, PDFont font, float fontSize, float padding) throws IOException {
        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();

        if (text == null) return;
        List<String> lines = wrapText(text, font, fontSize, width - 2 * padding);

        float lineHeight = fontSize + 2;
        float textY = y + height - lineHeight;
        cs.beginText();
        cs.setFont(font, fontSize);
        for (String line : lines) {
            cs.newLineAtOffset(x + padding, textY);
            cs.showText(line);
            cs.endText();
            textY -= lineHeight;
            if (!line.equals(lines.get(lines.size() - 1))) {
                cs.beginText();
                cs.setFont(font, fontSize);
            }
        }
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        if (text == null) return lines;

        StringBuilder line = new StringBuilder();
        for (String word : text.split(" ")) {
            String testLine = (line.length() == 0 ? "" : line + " ") + word;
            float lineWidth = font.getStringWidth(testLine) / 1000 * fontSize;
            if (lineWidth > maxWidth && line.length() > 0) {
                lines.add(line.toString());
                line = new StringBuilder(word);
            } else {
                if (line.length() > 0) line.append(" ");
                line.append(word);
            }
        }
        if (line.length() > 0) lines.add(line.toString());
        return lines;
    }
}
