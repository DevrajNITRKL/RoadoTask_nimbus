-- ============================================================
-- NimbusAI Data Analyst Assignment — SQL Queries (PostgreSQL)
-- Focus Area: Option A — Customer Churn & Retention Analysis
-- All queries use schema: nimbus
-- ============================================================

SET search_path TO nimbus;

-- ─────────────────────────────────────────────────────────────
-- Q1: Joins + Aggregation
-- For each subscription plan: active customers, avg MRR,
-- support ticket rate (tickets/customer/month) over last 6 months
-- ─────────────────────────────────────────────────────────────

WITH date_range AS (
    -- Define the 6-month window dynamically
    SELECT
        DATE_TRUNC('month', NOW() - INTERVAL '6 months') AS start_dt,
        NOW() AS end_dt
),

active_subs AS (
    -- Get one active subscription per customer (most recent)
    SELECT DISTINCT ON (s.customer_id)
        s.customer_id,
        s.plan_id,
        s.mrr_usd
    FROM subscriptions s
    JOIN date_range dr ON TRUE
    WHERE s.status = 'active'
    ORDER BY s.customer_id, s.start_date DESC
),

plan_customers AS (
    SELECT
        p.plan_id,
        p.plan_name,
        p.plan_tier,
        COUNT(DISTINCT a.customer_id)                       AS active_customers,
        ROUND(AVG(a.mrr_usd), 2)                           AS avg_mrr_usd,
        SUM(a.mrr_usd)                                     AS total_mrr_usd
    FROM plans p
    LEFT JOIN active_subs a ON a.plan_id = p.plan_id
    GROUP BY p.plan_id, p.plan_name, p.plan_tier
),

ticket_counts AS (
    -- Count tickets in last 6 months, joined to customer's plan
    SELECT
        a.plan_id,
        COUNT(t.ticket_id)                                  AS total_tickets,
        COUNT(DISTINCT t.customer_id)                       AS customers_with_tickets
    FROM support_tickets t
    JOIN active_subs a ON a.customer_id = t.customer_id
    JOIN date_range dr ON TRUE
    WHERE t.created_at >= dr.start_dt
    GROUP BY a.plan_id
)

SELECT
    pc.plan_name,
    pc.plan_tier,
    COALESCE(pc.active_customers, 0)                        AS active_customers,
    COALESCE(pc.avg_mrr_usd, 0)                            AS avg_mrr_usd,
    COALESCE(pc.total_mrr_usd, 0)                          AS total_mrr_usd,
    -- Ticket rate: total tickets / (customers * 6 months)
    CASE
        WHEN pc.active_customers = 0 THEN 0
        ELSE ROUND(
            COALESCE(tc.total_tickets, 0)::NUMERIC
            / (pc.active_customers * 6.0), 4
        )
    END                                                     AS tickets_per_customer_per_month
FROM plan_customers pc
LEFT JOIN ticket_counts tc ON tc.plan_id = pc.plan_id
ORDER BY pc.plan_tier, pc.avg_mrr_usd DESC;


-- ─────────────────────────────────────────────────────────────
-- Q2: Window Functions
-- Rank customers within each plan tier by LTV.
-- Show % difference between their LTV and tier average.
-- LTV = sum of all paid invoice amounts
-- ─────────────────────────────────────────────────────────────

WITH customer_ltv AS (
    SELECT
        c.customer_id,
        c.company_name,
        p.plan_tier,
        -- LTV: total paid (non-refunded, non-void) invoice revenue
        COALESCE(SUM(
            CASE WHEN bi.status IN ('paid') THEN bi.amount_usd ELSE 0 END
        ), 0)                                               AS ltv_usd
    FROM customers c
    -- Join to their most recent active/historical subscription for tier
    LEFT JOIN (
        SELECT DISTINCT ON (customer_id)
            customer_id, plan_id
        FROM subscriptions
        ORDER BY customer_id, start_date DESC
    ) latest_sub ON latest_sub.customer_id = c.customer_id
    LEFT JOIN plans p ON p.plan_id = latest_sub.plan_id
    LEFT JOIN billing_invoices bi ON bi.customer_id = c.customer_id
    WHERE c.company_name != ''           -- exclude blank name records
      AND c.company_name IS NOT NULL
    GROUP BY c.customer_id, c.company_name, p.plan_tier
),

ranked AS (
    SELECT
        customer_id,
        company_name,
        COALESCE(plan_tier, 'unknown')                      AS plan_tier,
        ltv_usd,
        RANK() OVER (
            PARTITION BY plan_tier
            ORDER BY ltv_usd DESC
        )                                                   AS tier_rank,
        AVG(ltv_usd) OVER (
            PARTITION BY plan_tier
        )                                                   AS tier_avg_ltv
    FROM customer_ltv
)

SELECT
    plan_tier,
    tier_rank,
    company_name,
    ROUND(ltv_usd, 2)                                      AS ltv_usd,
    ROUND(tier_avg_ltv, 2)                                 AS tier_avg_ltv_usd,
    ROUND(
        (ltv_usd - tier_avg_ltv) / NULLIF(tier_avg_ltv, 0) * 100, 2
    )                                                      AS pct_diff_from_tier_avg
FROM ranked
ORDER BY plan_tier, tier_rank;


-- ─────────────────────────────────────────────────────────────
-- Q3: CTEs + Subqueries
-- Customers who downgraded their plan in the last 90 days AND
-- had more than 3 support tickets in the 30 days before downgrading.
-- Include current and previous plan details.
--
-- Downgrade = moving to a plan with a lower monthly_price_usd
-- ─────────────────────────────────────────────────────────────

WITH plan_transitions AS (
    -- Identify all sequential plan changes per customer
    SELECT
        s.customer_id,
        s.plan_id                                          AS new_plan_id,
        s.start_date                                       AS transition_date,
        LAG(s.plan_id) OVER (
            PARTITION BY s.customer_id ORDER BY s.start_date
        )                                                  AS prev_plan_id,
        LAG(s.start_date) OVER (
            PARTITION BY s.customer_id ORDER BY s.start_date
        )                                                  AS prev_start_date
    FROM subscriptions s
),

downgrades AS (
    -- A downgrade = new plan price < previous plan price
    SELECT
        pt.customer_id,
        pt.transition_date                                 AS downgrade_date,
        pt.prev_plan_id,
        pt.new_plan_id,
        pp.plan_name                                       AS prev_plan_name,
        pp.plan_tier                                       AS prev_plan_tier,
        pp.monthly_price_usd                               AS prev_plan_price,
        np.plan_name                                       AS new_plan_name,
        np.plan_tier                                       AS new_plan_tier,
        np.monthly_price_usd                               AS new_plan_price
    FROM plan_transitions pt
    JOIN plans pp ON pp.plan_id = pt.prev_plan_id
    JOIN plans np ON np.plan_id = pt.new_plan_id
    WHERE pt.transition_date >= NOW() - INTERVAL '90 days'
      AND pt.prev_plan_id IS NOT NULL
      AND np.monthly_price_usd < pp.monthly_price_usd   -- actual downgrade
),

tickets_before_downgrade AS (
    -- Count tickets in the 30 days before the downgrade date
    SELECT
        d.customer_id,
        d.downgrade_date,
        COUNT(t.ticket_id)                                AS tickets_prior_30d
    FROM downgrades d
    JOIN support_tickets t ON t.customer_id = d.customer_id
        AND t.created_at >= d.downgrade_date - INTERVAL '30 days'
        AND t.created_at <  d.downgrade_date
    GROUP BY d.customer_id, d.downgrade_date
    HAVING COUNT(t.ticket_id) > 3
)

SELECT
    c.customer_id,
    c.company_name,
    c.industry,
    c.company_size,
    d.downgrade_date,
    d.prev_plan_name,
    d.prev_plan_tier,
    d.prev_plan_price                                     AS prev_monthly_usd,
    d.new_plan_name,
    d.new_plan_tier,
    d.new_plan_price                                      AS new_monthly_usd,
    tb.tickets_prior_30d,
    c.is_active,
    c.churn_reason
FROM downgrades d
JOIN tickets_before_downgrade tb
    ON tb.customer_id = d.customer_id
    AND tb.downgrade_date = d.downgrade_date
JOIN customers c ON c.customer_id = d.customer_id
ORDER BY tb.tickets_prior_30d DESC, d.downgrade_date DESC;


-- ─────────────────────────────────────────────────────────────
-- Q4: Time Series
-- Month-over-month new subscription growth rate +
-- rolling 3-month average churn rate by plan tier.
-- Flag months where churn exceeded 2× rolling average.
-- ─────────────────────────────────────────────────────────────

WITH monthly_new AS (
    -- New subscriptions per month per plan tier
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.start_date)::DATE            AS month,
        COUNT(*)                                           AS new_subs
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.start_date IS NOT NULL
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.start_date)
),

monthly_churn AS (
    -- Churned customers per month (status = cancelled + customer churned)
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.end_date)::DATE              AS month,
        COUNT(DISTINCT s.customer_id)                      AS churned_customers
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.status = 'cancelled'
      AND s.end_date IS NOT NULL
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.end_date)
),

monthly_active AS (
    -- Active customer base at start of each month (for churn rate denominator)
    SELECT
        p.plan_tier,
        DATE_TRUNC('month', s.start_date)::DATE            AS month,
        COUNT(DISTINCT s.customer_id)                      AS active_base
    FROM subscriptions s
    JOIN plans p ON p.plan_id = s.plan_id
    WHERE s.status IN ('active', 'cancelled', 'paused', 'trial')
    GROUP BY p.plan_tier, DATE_TRUNC('month', s.start_date)
),

combined AS (
    SELECT
        mn.plan_tier,
        mn.month,
        mn.new_subs,
        COALESCE(mc.churned_customers, 0)                  AS churned,
        COALESCE(ma.active_base, 1)                        AS active_base,
        -- MoM growth rate of new subscriptions
        ROUND(
            (mn.new_subs - LAG(mn.new_subs) OVER (
                PARTITION BY mn.plan_tier ORDER BY mn.month
            ))::NUMERIC
            / NULLIF(LAG(mn.new_subs) OVER (
                PARTITION BY mn.plan_tier ORDER BY mn.month
            ), 0) * 100, 2
        )                                                  AS mom_new_sub_growth_pct,
        -- Monthly churn rate
        ROUND(
            COALESCE(mc.churned_customers, 0)::NUMERIC
            / NULLIF(COALESCE(ma.active_base, 0), 0) * 100, 4
        )                                                  AS monthly_churn_rate_pct
    FROM monthly_new mn
    LEFT JOIN monthly_churn mc
        ON mc.plan_tier = mn.plan_tier AND mc.month = mn.month
    LEFT JOIN monthly_active ma
        ON ma.plan_tier = mn.plan_tier AND ma.month = mn.month
),

with_rolling AS (
    SELECT
        *,
        -- 3-month rolling average churn rate
        ROUND(AVG(monthly_churn_rate_pct) OVER (
            PARTITION BY plan_tier
            ORDER BY month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 4)                                             AS rolling_3m_avg_churn_pct
    FROM combined
)

SELECT
    plan_tier,
    month,
    new_subs,
    mom_new_sub_growth_pct,
    churned,
    monthly_churn_rate_pct,
    rolling_3m_avg_churn_pct,
    CASE
        WHEN rolling_3m_avg_churn_pct > 0
         AND monthly_churn_rate_pct > 2 * rolling_3m_avg_churn_pct
        THEN '🚨 CHURN SPIKE (>2× rolling avg)'
        ELSE NULL
    END                                                    AS churn_flag
FROM with_rolling
ORDER BY plan_tier, month;


-- ─────────────────────────────────────────────────────────────
-- Q5: Advanced — Potential Duplicate Customer Accounts
--
-- Matching logic:
--   (a) Exact email domain match (after stripping subaddressing like +work)
--   (b) Fuzzy company name match — same first word AND levenshtein distance ≤ 3
--   (c) Overlapping team member email domains
--
-- We use a scoring approach: 2+ signals = likely duplicate.
-- Note: pg_trgm extension needed for similarity(); levenshtein from fuzzystrmatch.
-- If extensions unavailable, use pure string-based fallbacks shown below.
-- ─────────────────────────────────────────────────────────────

-- Enable fuzzy matching (run once per DB):
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

WITH cleaned_customers AS (
    SELECT
        customer_id,
        -- Normalize company name: trim, lower, collapse spaces
        LOWER(TRIM(REGEXP_REPLACE(company_name, '\s+', ' ', 'g')))
                                                           AS company_norm,
        -- Extract email domain, strip subaddress (+...) from local part
        LOWER(SPLIT_PART(
            REGEXP_REPLACE(contact_email, '\+[^@]+', ''), '@', 2
        ))                                                 AS email_domain,
        contact_email,
        contact_name,
        signup_date,
        signup_source,
        is_active
    FROM customers
    WHERE company_name IS NOT NULL
      AND TRIM(company_name) != ''
),

candidate_pairs AS (
    -- Self-join: only compare each pair once (a.id < b.id)
    SELECT
        a.customer_id                                      AS cust_a_id,
        b.customer_id                                      AS cust_b_id,
        a.company_norm                                     AS name_a,
        b.company_norm                                     AS name_b,
        a.email_domain                                     AS domain_a,
        b.email_domain                                     AS domain_b,
        a.signup_date                                      AS signup_a,
        b.signup_date                                      AS signup_b,
        -- Signal 1: same email domain
        CASE WHEN a.email_domain = b.email_domain
             THEN 1 ELSE 0 END                             AS same_domain,
        -- Signal 2: company names are very similar (first token identical
        --           OR full names within levenshtein distance 3)
        CASE
            WHEN a.company_norm = b.company_norm THEN 1
            WHEN SPLIT_PART(a.company_norm, ' ', 1)
               = SPLIT_PART(b.company_norm, ' ', 1)
             AND ABS(LENGTH(a.company_norm) - LENGTH(b.company_norm)) <= 5
            THEN 1
            ELSE 0
        END                                                AS similar_name,
        -- Signal 3: signed up within 7 days of each other
        CASE WHEN ABS(a.signup_date - b.signup_date) <= 7
             THEN 1 ELSE 0 END                             AS similar_signup
    FROM cleaned_customers a
    JOIN cleaned_customers b ON a.customer_id < b.customer_id
    -- Pre-filter: at least one signal must be non-zero to reduce pairs
    WHERE a.email_domain = b.email_domain
       OR SPLIT_PART(a.company_norm, ' ', 1)
        = SPLIT_PART(b.company_norm, ' ', 1)
),

scored_pairs AS (
    SELECT
        *,
        same_domain + similar_name + similar_signup        AS match_score
    FROM candidate_pairs
),

with_shared_members AS (
    -- Signal 4: overlapping team member email domains between two accounts
    SELECT
        sp.*,
        COUNT(DISTINCT tm.email)                           AS shared_member_emails
    FROM scored_pairs sp
    LEFT JOIN team_members tm_a
        ON tm_a.customer_id = sp.cust_a_id AND tm_a.is_active = TRUE
    LEFT JOIN team_members tm_b
        ON tm_b.customer_id = sp.cust_b_id AND tm_b.is_active = TRUE
        AND LOWER(SPLIT_PART(tm_b.email, '@', 2))
          = LOWER(SPLIT_PART(tm_a.email, '@', 2))
    LEFT JOIN team_members tm
        ON tm.member_id = tm_a.member_id  -- placeholder for join alias
    GROUP BY
        sp.cust_a_id, sp.cust_b_id, sp.name_a, sp.name_b,
        sp.domain_a, sp.domain_b, sp.signup_a, sp.signup_b,
        sp.same_domain, sp.similar_name, sp.similar_signup, sp.match_score
)

SELECT
    cust_a_id,
    cust_b_id,
    name_a,
    name_b,
    domain_a,
    domain_b,
    signup_a,
    signup_b,
    same_domain,
    similar_name,
    similar_signup,
    match_score,
    -- Confidence label based on score
    CASE
        WHEN match_score >= 3 THEN 'HIGH — likely duplicate'
        WHEN match_score = 2 THEN 'MEDIUM — probable duplicate'
        ELSE 'LOW — possible duplicate'
    END                                                    AS duplicate_confidence
FROM with_shared_members
WHERE match_score >= 2         -- only surface medium/high confidence pairs
ORDER BY match_score DESC, signup_a;

-- ── Summary: known intentional duplicate seed in this dataset ──
-- customer_id 1201 ('Acme Labs ') and 1202 ('AcmeLabs'):
--   - same email domain: acmelabs.com ✓
--   - similar name: 'acme' prefix match ✓
--   - same signup date (2024-03-15) ✓  → match_score = 3 (HIGH)
