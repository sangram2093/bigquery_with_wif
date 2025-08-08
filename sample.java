Gotcha. Here are small, surgical changes to your existing BigQueryWIFPDFBoxNew—no renaming, no refactors.


---

1) Use a BigQuery field as the right-top “Creation date”

At the top (with other constants), add the column name you want to use (example: updated_at):

static final String CREATION_DATE_COLUMN = "updated_at"; // <-- set to your BQ column name

Inside the page loop (right before you draw headers), add this to compute the right-top header text from the first row of that exchange:

// derive creation date from the first row of this exchange
String creation;
if (!entry.getValue().isEmpty() && entry.getValue().get(0).get(CREATION_DATE_COLUMN) != null
        && !entry.getValue().get(0).get(CREATION_DATE_COLUMN).isNull()) {
    creation = "Creation date: " + entry.getValue().get(0).get(CREATION_DATE_COLUMN).getStringValue();
} else {
    creation = "Creation date: -";
}

Then replace your previous right-top header block with this (uses that creation string and keeps the smaller header size you added earlier):

float headerFontSize = 9f; // or whatever you picked earlier

String centerTitle = "BigQuery Data Export";
float centerX = page.getMediaBox().getWidth() / 2;
float centerTextWidth = boldFont.getStringWidth(centerTitle) / 1000f * headerFontSize;

// Left
content.beginText();
content.setFont(font, headerFontSize);
content.newLineAtOffset(margin, yPos);
content.showText("Exchange: " + entry.getKey());
content.endText();

// Center
content.beginText();
content.setFont(boldFont, headerFontSize);
content.newLineAtOffset(centerX - (centerTextWidth / 2f), yPos);
content.showText(centerTitle);
content.endText();

// Right (from BigQuery value)
float rightTextWidth = font.getStringWidth(creation) / 1000f * headerFontSize;
float rightX = page.getMediaBox().getWidth() - margin - rightTextWidth;
content.beginText();
content.setFont(font, headerFontSize);
content.newLineAtOffset(rightX, yPos);
content.showText(creation);
content.endText();

yPos -= 30;


---

2) Control which columns print in the PDF

Add a selectable list near your other constants:

// Only these columns will be printed, in this order:
static final List<String> SELECTED_COLUMNS = Arrays.asList(
    "client_order_id", "exchange", "trader", "status" // <-- edit as needed
);

Build headers from SELECTED_COLUMNS (instead of all fields) where you currently do:

// OLD:
// List<String> headers = fields.stream().map(Field::getName).collect(Collectors.toList());

// NEW:
List<String> headers = SELECTED_COLUMNS.stream()
    .filter(col -> fields.stream().anyMatch(f -> f.getName().equals(col))) // keep only those present in BQ result
    .collect(Collectors.toList());

This headers list is already used everywhere below (header measurement, drawing headers, and row rendering), so no other code needs to change. When rendering row values, keep the same access:

String txt = row.get(col).isNull() ? "" : row.get(col).getStringValue();

If you want to make the widths align, ensure your config.yaml has entries for those selected columns; otherwise, your existing getOrDefault(col, 60) fallback is fine.


---

That’s it:

Right-top header now comes from a BigQuery column (first row of that exchange).

The table prints only the columns you list in SELECTED_COLUMNS, in that exact order.


