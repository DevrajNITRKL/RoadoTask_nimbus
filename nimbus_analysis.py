"""
NimbusAI Data Analyst Assignment — Task 3: Data Wrangling & Statistical Analysis
Focus Area: Option A — Customer Churn & Retention Analysis

Hypothesis tested:
  H0: Customers who used the AI task suggestion feature (ai_task_suggest)
      do NOT have significantly lower churn than those who did not.
  H1: Customers who used ai_task_suggest DO have significantly lower churn.

Libraries: pandas, numpy, scipy, pymongo, psycopg2, sqlalchemy, matplotlib, seaborn, sklearn

NOTE: Replace DB connection strings with actual credentials.
      Set DEMO_MODE = True to run with synthetic data (no DB required).
"""

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings
import json
import re
from datetime import datetime, timezone

warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

DEMO_MODE = True  # Set to False when using real DB connections

PG_CONN_STR = "postgresql://user:password@localhost:5432/AI"
MONGO_URI    = "mongodb://localhost:27017/"
MONGO_DB     = "nimbus_events"

print("=" * 65)
print(" NimbusAI — Task 3: Data Wrangling & Statistical Analysis")
print("=" * 65)


# ─────────────────────────────────────────────────────────────
# STEP 1: Load Data from PostgreSQL + MongoDB
# ─────────────────────────────────────────────────────────────

def load_sql_data():
    """Load customer, subscription, billing, and ticket data from PostgreSQL."""
    if DEMO_MODE:
        print("\n[DEMO] Generating synthetic SQL data...")
        np.random.seed(42)
        n = 400
        customer_ids = np.arange(1, n + 1)
        plan_tiers = np.random.choice(['free', 'starter', 'professional', 'enterprise'],
                                       p=[0.35, 0.30, 0.25, 0.10], size=n)
        # Churn is higher on free/starter plans
        churn_prob = {'free': 0.35, 'starter': 0.25, 'professional': 0.12, 'enterprise': 0.05}
        is_churned = [np.random.rand() < churn_prob[t] for t in plan_tiers]
        signup_dates = pd.to_datetime(
            pd.date_range('2023-01-01', '2025-06-01', periods=n)
        )
        mrr = {'free': 0, 'starter': 12, 'professional': 39, 'enterprise': 99}
        mrr_values = [mrr[t] * (1 + np.random.uniform(-0.1, 0.1)) for t in plan_tiers]
        ticket_counts = np.random.poisson(lam=[3 if c else 1 for c in is_churned])
        industries = np.random.choice(
            ['Technology', 'Finance', 'Healthcare', 'Education', 'E-commerce', 'Logistics'],
            size=n
        )
        company_sizes = np.random.choice(['small', 'medium', 'large', 'enterprise'], size=n)
        df = pd.DataFrame({
            'customer_id': customer_ids,
            'plan_tier': plan_tiers,
            'is_churned': is_churned,
            'mrr_usd': mrr_values,
            'signup_date': signup_dates,
            'ticket_count': ticket_counts,
            'industry': industries,
            'company_size': company_sizes,
            'country_code': np.random.choice(['US', 'GB', 'CA', 'DE', 'JP', 'SG'], size=n),
            'churn_reason': [
                np.random.choice(['Too expensive', 'Missing features', 'Competitor',
                                   'Budget cuts', 'Poor support', None])
                if c else None for c in is_churned
            ]
        })
        return df
    else:
        # Real DB load
        import sqlalchemy
        engine = sqlalchemy.create_engine(PG_CONN_STR)
        query = """
            SET search_path TO nimbus;
            SELECT
                c.customer_id,
                c.company_name,
                c.industry,
                c.company_size,
                c.country_code,
                c.signup_date,
                c.is_active,
                c.churned_at,
                c.churn_reason,
                p.plan_tier,
                s.mrr_usd,
                s.status AS sub_status,
                s.start_date AS sub_start,
                s.end_date   AS sub_end,
                COUNT(DISTINCT t.ticket_id) AS ticket_count
            FROM customers c
            LEFT JOIN (
                SELECT DISTINCT ON (customer_id)
                    customer_id, plan_id, mrr_usd, status, start_date, end_date
                FROM subscriptions
                ORDER BY customer_id, start_date DESC
            ) s ON s.customer_id = c.customer_id
            LEFT JOIN plans p ON p.plan_id = s.plan_id
            LEFT JOIN support_tickets t ON t.customer_id = c.customer_id
            WHERE c.company_name IS NOT NULL AND TRIM(c.company_name) != ''
            GROUP BY c.customer_id, c.company_name, c.industry, c.company_size,
                     c.country_code, c.signup_date, c.is_active, c.churned_at,
                     c.churn_reason, p.plan_tier, s.mrr_usd, s.status,
                     s.start_date, s.end_date
        """
        return pd.read_sql(query, engine)


def load_mongo_data():
    """Load feature usage events from MongoDB."""
    if DEMO_MODE:
        print("[DEMO] Generating synthetic MongoDB data...")
        np.random.seed(99)
        n_events = 2000
        customer_ids = np.random.randint(1, 401, size=n_events)
        features = np.random.choice(
            ['ai_task_suggest', 'gantt_charts', 'time_tracking',
             'advanced_reports', 'api_access', 'sso_integration',
             'resource_mgmt', 'custom_workflows', 'project_boards'],
            size=n_events
        )
        # ai_task_suggest users: 70% are customers 1-200 (simulate higher adoption there)
        member_ids = np.random.randint(1, 5000, size=n_events)
        timestamps = pd.to_datetime(
            np.random.uniform(
                pd.Timestamp('2024-01-01').value,
                pd.Timestamp('2025-10-01').value,
                size=n_events
            )
        )
        session_durations = np.random.exponential(scale=1200, size=n_events).astype(int)
        df = pd.DataFrame({
            'customer_id': customer_ids,
            'member_id': member_ids,
            'feature': features,
            'timestamp': timestamps,
            'session_duration_sec': session_durations,
            'event_type': 'feature_click'
        })
        return df
    else:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        cursor = db.user_activity_logs.find(
            { "event_type": "feature_click" },
            { "_id": 0, "customer_id": 1, "customerId": 1, "userId": 1,
              "member_id": 1, "feature": 1, "timestamp": 1, "session_duration_sec": 1 }
        )
        return pd.DataFrame(list(cursor))


# ─────────────────────────────────────────────────────────────
# STEP 2: Merge & Clean
# ─────────────────────────────────────────────────────────────

print("\n── STEP 2: Merge & Clean ──────────────────────────────────")

# Load both datasets
sql_df = load_sql_data()
mongo_df = load_mongo_data()

print(f"\nSQL data loaded:   {len(sql_df):,} rows × {sql_df.shape[1]} cols")
print(f"Mongo data loaded: {len(mongo_df):,} rows × {mongo_df.shape[1]} cols")

# ── 2a: Normalize MongoDB field names ────────────────────────
if not DEMO_MODE:
    # Handle mixed field names: userId/userID → member_id
    mongo_df['member_id'] = mongo_df['member_id'].combine_first(
        mongo_df.get('userId', pd.Series(dtype=float))
    ).combine_first(
        mongo_df.get('userID', pd.Series(dtype=float))
    )
    # Handle mixed customer_id/customerId → customer_id
    if 'customerId' in mongo_df.columns:
        mongo_df['customer_id'] = mongo_df['customer_id'].combine_first(
            pd.to_numeric(mongo_df['customerId'], errors='coerce')
        )
    if 'customerID' in mongo_df.columns:
        mongo_df['customer_id'] = mongo_df['customer_id'].combine_first(
            pd.to_numeric(mongo_df['customerID'], errors='coerce')
        )
    # Normalize timestamps — handle ISODate, string, and mixed formats
    def parse_mixed_timestamp(ts):
        if isinstance(ts, (pd.Timestamp, datetime)):
            return pd.Timestamp(ts).tz_localize('UTC') if ts.tzinfo is None else pd.Timestamp(ts)
        if isinstance(ts, str):
            # Try common formats
            for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S',
                        '%m/%d/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f%z'):
                try:
                    return pd.to_datetime(ts, format=fmt, utc=True)
                except (ValueError, TypeError):
                    continue
            return pd.to_datetime(ts, utc=True, errors='coerce')
        return pd.NaT

    print("Normalizing timestamps (may take a moment)...")
    mongo_df['timestamp'] = mongo_df['timestamp'].apply(parse_mixed_timestamp)

mongo_df['customer_id'] = pd.to_numeric(mongo_df['customer_id'], errors='coerce')

# ── 2b: Drop orphans (MongoDB records with no matching SQL customer) ──
rows_before = len(mongo_df)
valid_ids = set(sql_df['customer_id'].dropna())
mongo_df = mongo_df[mongo_df['customer_id'].isin(valid_ids)]
rows_after = len(mongo_df)
print(f"\nOrphan removal (Mongo): {rows_before - rows_after:,} rows dropped "
      f"({(rows_before - rows_after) / rows_before * 100:.1f}%)")
print(f"  Before: {rows_before:,}  →  After: {rows_after:,}")

# ── 2c: Drop duplicate events in MongoDB ─────────────────────
rows_before = len(mongo_df)
mongo_df.drop_duplicates(
    subset=['customer_id', 'member_id', 'feature', 'timestamp'],
    inplace=True
)
rows_after = len(mongo_df)
print(f"\nDuplicate event removal (Mongo): {rows_before - rows_after:,} dupes dropped")

# ── 2d: Handle nulls in SQL ───────────────────────────────────
print("\nSQL null counts before cleaning:")
print(sql_df.isnull().sum()[sql_df.isnull().sum() > 0].to_string())

# Fill nulls in categorical columns with 'unknown'
for col in ['plan_tier', 'industry', 'company_size', 'country_code']:
    if col in sql_df.columns:
        null_count = sql_df[col].isnull().sum()
        sql_df[col].fillna('unknown', inplace=True)
        if null_count > 0:
            print(f"  Filled {null_count} nulls in '{col}' with 'unknown'")

# mrr_usd nulls → 0 (free plan)
if 'mrr_usd' in sql_df.columns:
    sql_df['mrr_usd'].fillna(0, inplace=True)

# ticket_count nulls → 0
if 'ticket_count' in sql_df.columns:
    sql_df['ticket_count'].fillna(0, inplace=True)
    sql_df['ticket_count'] = sql_df['ticket_count'].astype(int)

# ── 2e: Outlier handling — cap MRR at 99th percentile ─────────
if 'mrr_usd' in sql_df.columns:
    p99 = sql_df['mrr_usd'].quantile(0.99)
    outlier_count = (sql_df['mrr_usd'] > p99).sum()
    sql_df['mrr_usd'] = sql_df['mrr_usd'].clip(upper=p99)
    print(f"\nMRR outliers (>p99={p99:.0f}) capped: {outlier_count} records")

# ── 2f: Feature flag per customer from MongoDB ───────────────
# Identify which customers used ai_task_suggest
feature_usage = (
    mongo_df[mongo_df['feature'] == 'ai_task_suggest']
    .groupby('customer_id')
    .size()
    .reset_index(name='ai_suggest_clicks')
)
feature_usage['used_ai_suggest'] = True

# Merge with SQL
merged_df = sql_df.merge(feature_usage[['customer_id', 'used_ai_suggest', 'ai_suggest_clicks']],
                          on='customer_id', how='left')
merged_df['used_ai_suggest'].fillna(False, inplace=True)
merged_df['ai_suggest_clicks'].fillna(0, inplace=True)

# Add total feature count per customer
all_feature_usage = (
    mongo_df.groupby('customer_id')
    .agg(
        total_feature_clicks=('feature', 'count'),
        distinct_features_used=('feature', 'nunique'),
        total_session_sec=('session_duration_sec', 'sum')
    )
    .reset_index()
)
merged_df = merged_df.merge(all_feature_usage, on='customer_id', how='left')
merged_df['total_feature_clicks'].fillna(0, inplace=True)
merged_df['distinct_features_used'].fillna(0, inplace=True)
merged_df['total_session_sec'].fillna(0, inplace=True)

print(f"\nFinal merged dataset: {len(merged_df):,} rows × {merged_df.shape[1]} cols")
print(f"  Churned customers:  {merged_df['is_churned'].sum():,}")
print(f"  Active customers:   {(~merged_df['is_churned']).sum():,}")
print(f"  AI Suggest users:   {merged_df['used_ai_suggest'].sum():,}")


# ─────────────────────────────────────────────────────────────
# STEP 3: Hypothesis Test
# H0: P(churn | used_ai_suggest) = P(churn | no_ai_suggest)
# H1: P(churn | used_ai_suggest) < P(churn | no_ai_suggest)
# Test: Two-proportion z-test (one-tailed)
# Significance level: α = 0.05
# ─────────────────────────────────────────────────────────────

print("\n── STEP 3: Hypothesis Testing ────────────────────────────────")
print("\nHypothesis:")
print("  H0: Churn rate is the same for ai_task_suggest users vs. non-users")
print("  H1: ai_task_suggest users have LOWER churn rate")
print("  Significance level: α = 0.05  |  Test: Two-proportion z-test (one-tailed)")

ai_group    = merged_df[merged_df['used_ai_suggest'] == True]
no_ai_group = merged_df[merged_df['used_ai_suggest'] == False]

n1  = len(ai_group)
n2  = len(no_ai_group)
x1  = ai_group['is_churned'].sum()      # churned in AI group
x2  = no_ai_group['is_churned'].sum()   # churned in non-AI group
p1  = x1 / n1                           # churn rate for AI users
p2  = x2 / n2                           # churn rate for non-AI users

print(f"\nGroup sizes:          AI={n1:,}  |  No-AI={n2:,}")
print(f"Churned:              AI={x1:,} ({p1:.1%})  |  No-AI={x2:,} ({p2:.1%})")

# Assumption checks
# 1. Sample size: n1*p1 and n2*p2 should be ≥ 5
print(f"\nAssumption checks:")
print(f"  n1*p1 = {n1*p1:.1f}  (≥5 required: {'✓' if n1*p1 >= 5 else '✗'})")
print(f"  n1*(1-p1) = {n1*(1-p1):.1f}  (≥5 required: {'✓' if n1*(1-p1) >= 5 else '✗'})")
print(f"  n2*p2 = {n2*p2:.1f}  (≥5 required: {'✓' if n2*p2 >= 5 else '✗'})")
print(f"  n2*(1-p2) = {n2*(1-p2):.1f}  (≥5 required: {'✓' if n2*(1-p2) >= 5 else '✗'})")

# Pooled proportion
p_pool = (x1 + x2) / (n1 + n2)
se = np.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
z_stat = (p1 - p2) / se
# One-tailed p-value: we expect p1 < p2
p_value = stats.norm.cdf(z_stat)

print(f"\nPooled proportion:    {p_pool:.4f}")
print(f"Standard error:       {se:.4f}")
print(f"Z-statistic:          {z_stat:.4f}")
print(f"P-value (one-tailed): {p_value:.4f}")
print(f"\nConclusion:")
if p_value < 0.05:
    print(f"  ✅ Reject H0 at α=0.05. AI task suggest users have SIGNIFICANTLY LOWER")
    print(f"     churn ({p1:.1%} vs. {p2:.1%}). Feature investment appears justified.")
else:
    print(f"  ❌ Fail to reject H0 at α=0.05. No significant churn difference detected.")
    print(f"     (Churn rates: AI={p1:.1%}, No-AI={p2:.1%}). Larger sample may be needed.")

# Also run chi-square for robustness
contingency = np.array([[x1, n1-x1], [x2, n2-x2]])
chi2, p_chi2, dof, expected = stats.chi2_contingency(contingency)
print(f"\nRobustness check — Chi-square test:")
print(f"  χ²={chi2:.4f}  df={dof}  p={p_chi2:.4f}")
print(f"  (Two-tailed p-value for reference)")


# ─────────────────────────────────────────────────────────────
# STEP 4: Customer Segmentation — RFE (Recency-Frequency-Engagement)
# Three-cluster K-Means segmentation.
#
# Variables used:
#   - months_since_signup (recency)
#   - ticket_count (friction indicator)
#   - total_feature_clicks (engagement breadth)
#   - distinct_features_used (platform stickiness)
#   - mrr_usd (value)
#   - total_session_sec (time investment)
# ─────────────────────────────────────────────────────────────

print("\n── STEP 4: Customer Segmentation ─────────────────────────────")

ref_date = pd.Timestamp('2025-11-01')
if 'signup_date' in merged_df.columns:
    merged_df['months_since_signup'] = (
        (ref_date - pd.to_datetime(merged_df['signup_date'])).dt.days / 30.0
    ).fillna(0).clip(lower=0)
else:
    merged_df['months_since_signup'] = np.random.uniform(1, 36, size=len(merged_df))

seg_features = [
    'months_since_signup',
    'ticket_count',
    'total_feature_clicks',
    'distinct_features_used',
    'mrr_usd',
    'total_session_sec'
]

seg_df = merged_df[seg_features].copy().fillna(0)

# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(seg_df)

# K-Means with k=3
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
merged_df['segment'] = kmeans.fit_predict(X_scaled)

# Label segments based on cluster centers
centers = pd.DataFrame(scaler.inverse_transform(kmeans.cluster_centers_),
                        columns=seg_features)
print("\nCluster centers (original scale):")
print(centers.round(2).to_string())

# Assign business-friendly labels based on MRR + engagement
segment_stats = merged_df.groupby('segment').agg(
    count=('customer_id', 'count'),
    avg_mrr=('mrr_usd', 'mean'),
    avg_clicks=('total_feature_clicks', 'mean'),
    avg_features=('distinct_features_used', 'mean'),
    churn_rate=('is_churned', 'mean')
).round(2)

print("\nSegment statistics:")
print(segment_stats.to_string())

# Auto-label segments
seg_labels = {}
sorted_by_mrr = segment_stats['avg_mrr'].sort_values()
seg_labels[sorted_by_mrr.index[0]] = 'At-Risk / Low-Value'
seg_labels[sorted_by_mrr.index[1]] = 'Growth Potential'
seg_labels[sorted_by_mrr.index[2]] = 'Champions (High-Value)'

merged_df['segment_label'] = merged_df['segment'].map(seg_labels)

print("\nSegment labels assigned:")
for seg_id, label in seg_labels.items():
    grp = segment_stats.loc[seg_id]
    print(f"  Segment {seg_id} → '{label}'")
    print(f"    Customers: {int(grp['count'])} | Avg MRR: ${grp['avg_mrr']:.0f} | "
          f"Churn: {grp['churn_rate']:.1%}")

print("\nBusiness implications:")
print("  ▸ 'Champions' → protect with proactive success check-ins, lock in annual contracts")
print("  ▸ 'Growth Potential' → offer feature demos, upsell to higher tier")
print("  ▸ 'At-Risk / Low-Value' → targeted re-engagement or early churn prediction alerts")


# ─────────────────────────────────────────────────────────────
# STEP 5: Export cleaned data for dashboard use
# ─────────────────────────────────────────────────────────────

print("\n── STEP 5: Export ────────────────────────────────────────────")

output_cols = [
    'customer_id', 'plan_tier', 'is_churned', 'mrr_usd', 'signup_date',
    'ticket_count', 'industry', 'company_size', 'country_code',
    'used_ai_suggest', 'ai_suggest_clicks', 'total_feature_clicks',
    'distinct_features_used', 'total_session_sec', 'months_since_signup',
    'segment', 'segment_label'
]

export_df = merged_df[[c for c in output_cols if c in merged_df.columns]]
export_df.to_csv(r'C:\Users\asus\Downloads\RoaDoAITaskDB\RoaDoAITask\nimbus_analysis.csv', index=False)
print(f"Exported {len(export_df):,} rows to nimbus_analysis_output.csv")

print("\n" + "=" * 65)
print(" Analysis Complete")
print("=" * 65)
print("\nKey findings:")
print(f"  1. Churn rate:  AI suggest users={p1:.1%}  vs  non-users={p2:.1%}")
print(f"     p-value={p_value:.4f} → {'Statistically significant' if p_value < 0.05 else 'Not significant at α=0.05'}")
print(f"  2. Three customer segments identified with distinct risk profiles")
print(f"  3. Free/starter tiers have substantially higher churn — consider")
print(f"     feature-based nudges to drive upgrade adoption")
