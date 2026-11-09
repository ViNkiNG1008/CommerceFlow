# CommerceFlow Power BI Project Kit

## What this kit does
This package prepares the theme, DAX measures, and exact three-page report build specification for your already-connected PostgreSQL model. It is **not a native .pbix/.pbip file**; Power BI Desktop must save the final report. I will not label a template as a finished report.

## Existing connection
- Server: `localhost:5432`
- Database: `commerceflow_spark_test`
- Schema: `gold`
- Seven analytical views and four dimensions are already imported.

## Apply the theme
In Power BI Desktop: Design > Import theme > select `theme/CommerceFlow-Modern-Theme.json`.

## Add DAX measures
Open `scripts/CommerceFlow-Measures.dax`. Create each measure separately using Modeling > New measure. The names are based on the fields visible in your Navigator screenshots. If Power BI flags a missing column, inspect that view's actual fields before proceeding.

## Page 1 — Executive Sales Overview
Canvas: 16:9; background `#F5F7FB`.
Header: `CommerceFlow` / `Executive Sales Overview` / `Brazilian E-commerce.

Four KPI cards:
1. Total Revenue — `[Total Revenue]`, currency BRL (R$), display units Millions.
2. Total Orders — `[Total Orders]`, display units Thousands.
3. Average Order Value — `[Average Order Value]`, currency BRL (R$).
4. Total Freight Revenue — `[Total Freight Revenue]`, currency BRL (R$), display units Millions.

Charts:
- Line: `gold v monthly_sales[month_name]` vs `[Total Revenue]`; sort month_name by month_number. Add year_number as a slicer or legend if multiple years appear.
- Column: month_name vs `[Total Orders]`, with year_number as slicer/legend.
- Donut: `gold v_payment_analysis[payment_type]` vs `[Payment Value]` (add this measure if the view contains total_payment_value).
- Bar: product_category_name_english vs `[Product Revenue]`, Top 10.

## Page 2 — Product & Seller Performance
KPI cards: `[Units Sold]`, `[Product Revenue]`, `[Seller Revenue]`, `[Seller Orders]`.
Charts:
- Bar: product_category_name_english vs `[Product Revenue]`, Top 10.
- Bar: product_name_english vs `[Units Sold]`, Top 10 (if populated).
- Bar: seller_city vs `[Seller Revenue]`, Top 10.
- Column: seller_state vs `[Seller Revenue]`.
- Table: category, product name, units sold, product sales.

## Page 3 — Delivery, Customer & Reviews
KPI cards: `[Delivered Orders]`, `[Late Deliveries]`, `[Late Delivery Rate]` (percentage), `[Weighted Average Review Score]` (one decimal).
Charts:
- Bar: order_status vs `[Delivered Orders]`.
- Column: customer_state vs `[Customer Orders]`, Top 10.
- Bar: review_score vs `[Total Reviews]`.
- Bar: customer_city vs `[Customer Orders]`, Top 10.

## Model cautions
- Keep the two detected 1:1 relationships between product dimension/product performance and seller dimension/seller performance only if their keys are unique.
- Each analytical view has its own aggregation grain. Filters from one view will not necessarily filter another.
- Do not combine measures from separate views in one visual without a validated shared dimension.
- Averages must not be summed. The review score measure is weighted by review count.
- Average delivery days in the current view is a simple average of grouped averages, not necessarily a true overall weighted average.
- Payment value is not interchangeable with sales revenue.
- Customer view does not show a revenue field in the Navigator screenshot; do not invent a customer revenue KPI.
- All monetary values remain BRL (R$), the source dataset currency.

## Save
When the three pages are built, File > Save As:
`D:\Projects\CommerceFlow\CommerceFlow.pbix`
