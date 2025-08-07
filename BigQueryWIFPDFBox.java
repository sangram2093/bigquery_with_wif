import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.*;
import org.apache.pdfbox.pdmodel.PDPageContentStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BigQueryPdfExporter {

    public static void main(String[] args) throws Exception {
        String wifHome = System.getenv("WIF_HOME");
        String wifTokenFilename = wifHome + "/wif_token.txt";
        String projectName = System.getenv("PROJECT_NAME");
        String location = System.getenv("LOCATION");

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

        String query = "SELECT exchange, client_name, order_id, price, quantity, symbol, timestamp, status, broker, region, strategy, notes " +
                "FROM `your_project.your_dataset.your_table` LIMIT 100";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        TableResult result = bigquery.query(queryConfig);

        List<String> columnNames = new ArrayList<>();
        for (Field field : result.getSchema().getFields()) {
            columnNames.add(field.getName());
        }

        Map<String, List<List<String>>> groupedData = new TreeMap<>();
        for (FieldValueList row : result.iterateAll()) {
            String key = row.get("exchange").getStringValue();
            groupedData.putIfAbsent(key, new ArrayList<>());

            List<String> rowData = new ArrayList<>();
            for (String col : columnNames) {
                rowData.add(row.get(col).isNull() ? "" : row.get(col).getValue().toString());
            }
            groupedData.get(key).add(rowData);
        }

        Map<String, Float> fixedColumnWidths = ColumnConfigLoader.loadColumnWidths();
        generatePdfWithFixedWidths(groupedData, columnNames, fixedColumnWidths, "BigQuery_Output.pdf");

        System.out.println("✅ PDF generated successfully.");
    }

    private static void generatePdfWithFixedWidths(Map<String, List<List<String>>> dataByExchange,
                                                   List<String> columnNames,
                                                   Map<String, Float> fixedWidths,
                                                   String outputFile) throws IOException {

        PDFont headerFont = PDType1Font.HELVETICA_BOLD;
        PDFont cellFont = PDType1Font.HELVETICA;
        float headerFontSize = 7;
        float cellFontSize = 6.5f;
        float padding = 4f;

        PDDocument document = new PDDocument();
        List<String> exchanges = new ArrayList<>(dataByExchange.keySet());
        int totalPages = exchanges.size();
        int currentPage = 1;

        for (String exchange : exchanges) {
            List<List<String>> rows = dataByExchange.get(exchange);
            PDPage page = new PDPage(new PDRectangle(PDRectangle.A4.getHeight(), PDRectangle.A4.getWidth())); // landscape
            document.addPage(page);

            PDPageContentStream cs = new PDPageContentStream(document, page);
            PDRectangle mediaBox = page.getMediaBox();

            float margin = 40;
            float yStart = mediaBox.getHeight() - 40;
            float yPosition = yStart;

            // Top-left header
            cs.beginText();
            cs.setFont(headerFont, 9);
            cs.newLineAtOffset(margin, mediaBox.getHeight() - 25);
            cs.showText("Exchange: " + exchange);
            cs.endText();

            // Center header
            String centerHeader = "BigQuery Export - " + exchange;
            float centerTextWidth = getTextWidth(centerHeader, headerFont, 9);
            cs.beginText();
            cs.setFont(headerFont, 9);
            cs.newLineAtOffset((mediaBox.getWidth() - centerTextWidth) / 2, mediaBox.getHeight() - 25);
            cs.showText(centerHeader);
            cs.endText();

            // Footer
            String footer = "Page " + currentPage + " of " + totalPages;
            float footerWidth = getTextWidth(footer, cellFont, 8);
            cs.beginText();
            cs.setFont(cellFont, 8);
            cs.newLineAtOffset(mediaBox.getWidth() - footerWidth - 40, 20);
            cs.showText(footer);
            cs.endText();

            // Draw headers
            float xPosition = margin;
            float maxHeaderHeight = 0;
            for (String col : columnNames) {
                float colWidth = fixedWidths.get(col);
                List<String> lines = wrapText(col, headerFont, headerFontSize, colWidth - 2 * padding);
                float cellHeight = lines.size() * (headerFontSize + 2) + 2 * padding;
                if (cellHeight > maxHeaderHeight) maxHeaderHeight = cellHeight;
            }

            for (String col : columnNames) {
                float colWidth = fixedWidths.get(col);
                drawWrappedCell(cs, col, xPosition, yPosition - maxHeaderHeight, colWidth, maxHeaderHeight,
                        headerFont, headerFontSize, padding);
                xPosition += colWidth;
            }

            yPosition -= maxHeaderHeight;

            // Draw rows
            for (List<String> row : rows) {
                xPosition = margin;
                float maxRowHeight = 0;
                List<List<String>> wrappedLinesPerColumn = new ArrayList<>();

                for (int i = 0; i < columnNames.size(); i++) {
                    String col = columnNames.get(i);
                    float colWidth = fixedWidths.get(col);
                    String value = i < row.size() ? row.get(i) : "";
                    List<String> lines = wrapText(value, cellFont, cellFontSize, colWidth - 2 * padding);
                    wrappedLinesPerColumn.add(lines);

                    float cellHeight = lines.size() * (cellFontSize + 2) + 2 * padding;
                    if (cellHeight > maxRowHeight) maxRowHeight = cellHeight;
                }

                for (int i = 0; i < columnNames.size(); i++) {
                    String col = columnNames.get(i);
                    float colWidth = fixedWidths.get(col);
                    drawWrappedCell(cs, wrappedLinesPerColumn.get(i), xPosition, yPosition - maxRowHeight,
                            colWidth, maxRowHeight, cellFont, cellFontSize, padding);
                    xPosition += colWidth;
                }

                yPosition -= maxRowHeight;
                if (yPosition < 60) break;
            }

            cs.close();
            currentPage++;
        }

        document.save(outputFile);
        document.close();
    }

    private static void drawWrappedCell(PDPageContentStream cs, List<String> lines,
                                        float x, float y, float width, float height,
                                        PDFont font, float fontSize, float padding) throws IOException {
        cs.setLineWidth(0.5f);
        cs.addRect(x, y, width, height);
        cs.stroke();

        if (lines == null || lines.isEmpty()) return;

        float lineHeight = fontSize + 2;
        float textY = y + height - padding - fontSize;

        for (String line : lines) {
            if (textY < y + padding) break;
            cs.beginText();
            cs.setFont(font, fontSize);
            cs.newLineAtOffset(x + padding, textY);
            cs.showText(line);
            cs.endText();
            textY -= lineHeight;
        }
    }

    private static void drawWrappedCell(PDPageContentStream cs, String text,
                                        float x, float y, float width, float height,
                                        PDFont font, float fontSize, float padding) throws IOException {
        List<String> lines = wrapText(text, font, fontSize, width - 2 * padding);
        drawWrappedCell(cs, lines, x, y, width, height, font, fontSize, padding);
    }

    private static List<String> wrapText(String text, PDFont font, float fontSize, float maxWidth) throws IOException {
        List<String> lines = new ArrayList<>();
        String[] words = text.split("\\s+");
        StringBuilder line = new StringBuilder();

        for (String word : words) {
            String testLine = line.length() == 0 ? word : line + " " + word;
            float width = font.getStringWidth(testLine) / 1000 * fontSize;
            if (width > maxWidth) {
                if (line.length() > 0) lines.add(line.toString());
                line = new StringBuilder(word);
            } else {
                line = new StringBuilder(testLine);
            }
        }

        if (line.length() > 0) lines.add(line.toString());
        return lines;
    }

    private static float getTextWidth(String text, PDFont font, float fontSize) throws IOException {
        return font.getStringWidth(text) / 1000 * fontSize;
    }
}
