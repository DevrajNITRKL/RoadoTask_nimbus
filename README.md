# NimbusAI — Data Analyst Intern Assignment
**Candidate:** Devraj  
**Focus Area:** Option A — Customer Churn & Retention Analysis  
**Submission Date:** April 2026

---

## Project Overview

This project investigates why customers are churning at NimbusAI, a B2B SaaS company with 400+ customers across four subscription tiers. The analysis combines PostgreSQL (customer, subscription, billing, and support data) with MongoDB (user activity logs, onboarding events, NPS responses) to identify churn patterns and recommend retention strategies.

---

## Files Included

```
nimbus_assignment/
│
├── README.md                        ← You are here
│
├── DATA (input files — provided)
│   ├── nimbus_core.sql              ← PostgreSQL database dump
│   └── nimbus_events.js             ← MongoDB events database
│
├── TASK 1 — SQL Queries
│   └── nimbus_sql_queries.sql       ← All 5 PostgreSQL queries with comments
│
├── TASK 2 — MongoDB Queries
│   └── nimbus_mongo_queries.js      ← All 4 aggregation pipelines with comments
│
├── TASK 3 — Python Analysis
│   ├── nimbus_analysis.py           ← Data wrangling + hypothesis test + segmentation
│   └── nimbus_analysis_output.csv   ← Output file (generated when script runs)
│
├── TASK 4 — Dashboard
│   └── nimbus_analysis_dashboard.xlsx ← Dashboard data source
│
└── TASK 5 — Video
    └── [Loom link in submission form]
```

---

## How to Run

### Requirements

| Tool | Version | Purpose |
|------|---------|---------|
| PostgreSQL | 14+ | SQL database |
| pgAdmin 4 | Any | Run SQL queries visually |
| MongoDB | 6+ | Events database |
| Python | 3.8+ | Data analysis script |

### Step 1 — Set up PostgreSQL

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE nimbus;"

# Import the data
psql -U postgres -d nimbus -f nimbus_core.sql
```

### Step 2 — Set up MongoDB

```bash
# Import events data
mongosh nimbus_events nimbus_events.js
```

### Step 3 — Install Python dependencies

```bash
pip install pandas numpy scipy scikit-learn pymongo psycopg2-binary sqlalchemy
```

### Step 4 — Run SQL queries

Open `nimbus_sql_queries.sql` in pgAdmin Query Tool and press F5.

Or from terminal:
```bash
psql -U postgres -d nimbus -f nimbus_sql_queries.sql
```

### Step 5 — Run MongoDB queries

```bash
mongosh nimbus_events nimbus_mongo_queries.js
```

Save output to file:
```bash
mongosh nimbus_events nimbus_mongo_queries.js > mongo_results.txt
```

### Step 6 — Run Python analysis

```bash
# Demo mode (no DB needed — uses synthetic data)
python nimbus_analysis.py

# Real DB mode — set DEMO_MODE = False inside the script first
python nimbus_analysis.py
```

Output file `nimbus_analysis_output.csv` will be created in the same folder.

---

## Key Findings

### Dataset Summary
- **400 customers** across 4 plan tiers (free, starter, professional, enterprise)
- **Overall churn rate: 24.2%** (97 out of 400 customers churned)
- **6 industries:** Logistics, Education, E-commerce, Healthcare, Technology, Finance
- **6 countries:** US, GB, CA, DE, JP, SG

### Finding 1 — Churn is concentrated in lower tiers

| Plan Tier | Customers | Churn Rate |
|-----------|-----------|------------|
| Free | 147 | 28.6% |
| Starter | 116 | 31.9% |
| Professional | 99 | 16.2% |
| Enterprise | 38 | 5.3% |

Free and starter plans account for over 80% of all churned customers.

### Finding 2 — Three distinct customer segments

| Segment | Count | % of Customers | Action |
|---------|-------|----------------|--------|
| At-Risk / Low-Value | 232 | 58% | Retention campaigns |
| Growth Potential | 131 | 33% | Upsell targeting |
| Champions (High-Value) | 37 | 9% | Protect and reward |

### Finding 3 — AI feature adoption not yet a churn differentiator

| Used AI Suggest | Churn Rate | Count |
|----------------|------------|-------|
| Yes | 25.0% | 176 |
| No | 23.7% | 224 |

Hypothesis test (two-proportion z-test, α=0.05): **Not statistically significant.**  
Conclusion: The feature needs broader adoption before its churn-reduction effect can be measured.

---

## Hypothesis Test Details

| Item | Detail |
|------|--------|
| H0 | ai_task_suggest users and non-users have the same churn rate |
| H1 | ai_task_suggest users have lower churn rate |
| Test | Two-proportion z-test (one-tailed) |
| Significance level | α = 0.05 |
| Result | Fail to reject H0 — not statistically significant |
| Robustness check | Chi-square test confirms result |

---

## Data Quality Issues Found & Handled

| Issue | Location | How it was handled |
|-------|----------|--------------------|
| Mixed field names (userId / customerId / customerID) | MongoDB | Normalized to single field using combine_first() |
| Mixed timestamp formats (ISODate, string, MM/DD/YYYY) | MongoDB | Parsed using dateFromString with error handling |
| Orphan records (MongoDB IDs not in SQL) | MongoDB | Dropped ~2% orphan records |
| Duplicate events | MongoDB | Removed duplicates on (customer_id, member_id, feature, timestamp) |
| NULL values in plan_tier, industry, company_size | PostgreSQL | Filled with 'unknown' |
| NULL values in mrr_usd, ticket_count | PostgreSQL | Filled with 0 |
| MRR outliers | PostgreSQL | Capped at 99th percentile |
| Blank company names (customer_id 1204) | PostgreSQL | Excluded from analysis |
| Duplicate customer accounts (1201 and 1202) | PostgreSQL | Detected via Q5 duplicate detection query |

---

## Recommendations

### 1. Intervene early on free and starter customers
With churn rates above 28%, these tiers are losing nearly 1 in 3 customers. Trigger a customer success outreach automatically when a customer files their 2nd support ticket — before they reach the frustration threshold that leads to churn.

### 2. Target Growth Potential segment for upsell
131 customers are already engaged and using multiple features but are on lower-tier plans. A personalized in-app upgrade offer tied to the specific features they are already clicking would likely convert a meaningful percentage.

### 3. Drive AI task suggest adoption through onboarding
Only 44% of customers have used the AI task suggestion feature. Build it into the mandatory onboarding flow so every new user experiences it in their first week. Re-test the churn hypothesis after 3 months of increased adoption.

---

## SQL Queries Summary (Task 1)

| Query | Description | Techniques Used |
|-------|-------------|-----------------|
| Q1 | Active customers, avg MRR, ticket rate per plan | JOINs, CTEs, aggregation |
| Q2 | Customer LTV ranking within plan tier | Window functions (RANK, AVG OVER) |
| Q3 | High-friction downgrades in last 90 days | CTEs, subqueries, date arithmetic |
| Q4 | MoM subscription growth + rolling churn rate | Time series, LAG, rolling window |
| Q5 | Duplicate account detection | Self-join, fuzzy name matching, scoring |

---

## MongoDB Queries Summary (Task 2)

| Query | Description | Techniques Used |
|-------|-------------|-----------------|
| Q1 | Sessions per user per week + percentiles by tier | Aggregation pipeline, $sortArray, percentiles |
| Q2 | Feature DAU + 7-day retention rate | $group, $filter, date arithmetic |
| Q3 | Onboarding funnel drop-off analysis | Multi-stage pipeline, $cond, funnel math |
| Q4 | Top 20 free-tier upsell candidates | Engagement scoring, $switch, cross-reference |

---

## Contact

For any questions about this submission please reach out via the RoaDo submission form.
