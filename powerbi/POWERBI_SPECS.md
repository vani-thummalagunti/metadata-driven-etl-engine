# Power BI Specifications: Executive Supply Chain & Sales Analytics

## 1. Dimensional Model Structure

```
[Dim_Date] ─────────┐
                    │ (1:*)
[Dim_Supplier] ─────┼────► [Fact_Orders] (TotalAmount, OrderDate, Region)
                    │ (1:*)
[Dim_Customer] ─────┘
```

---

## 2. Advanced DAX Measures

### A. Total Revenue & Fulfillment Rates
```dax
Total Revenue = SUM(Fact_Orders[TotalAmount])

Delivered Orders = 
CALCULATE(
    COUNTROWS(Fact_Orders),
    Fact_Orders[OrderStatus] = "Delivered"
)

On-Time In-Full (OTIF) % = 
DIVIDE(
    [Delivered Orders],
    COUNTROWS(Fact_Orders),
    0
)
```

### B. Year-over-Year (YoY) Sales Growth
```dax
Revenue Prior Year = 
CALCULATE(
    [Total Revenue],
    SAMEPERIODLASTYEAR(Dim_Date[Date])
)

YoY Revenue Growth % = 
DIVIDE(
    [Total Revenue] - [Revenue Prior Year],
    [Revenue Prior Year],
    0
)
```

---

## 3. Row-Level Security (RLS) Implementation

To ensure branch managers only see order data for their assigned geography:

1. **Role Definition in Power BI Desktop:**
   * Role Name: `Regional_Manager_Role`
   * Table: `Fact_Orders`
   * DAX Filter Expression:
     ```dax
     [Region] = USERPRINCIPALNAME() 
     -- Or via lookup against SecurityMapping table:
     -- [Region] = LOOKUPVALUE(UserRegionMapping[Region], UserRegionMapping[UserEmail], USERNAME())
     ```

2. **Publishing & Testing:**
   * Test role visibility using **"View As Role"** in Power BI Desktop.
   * Assign Azure Active Directory (AAD) security groups in Power BI Service under `Dataset -> Security`.
