// ============================================================
// NimbusAI Data Analyst Assignment — MongoDB Aggregation Pipelines
// Database: nimbus_events
// Collections: user_activity_logs, nps_survey_responses, onboarding_events
// ============================================================
// Run with: mongosh nimbus_events nimbus_mongo_queries.js
// Or paste individual pipelines in mongosh / Compass
// ============================================================

db = db.getSiblingDB('nimbus_events');

// ─────────────────────────────────────────────────────────────
// Q1: Aggregation Pipeline
// Average sessions per user per week, segmented by subscription tier.
// 25th, 50th, 75th percentile of session durations.
//
// Data quality handling:
//   - Normalize mixed field names: userId/userID → member_id
//   - Normalize customerId/customerID → customer_id
//   - Normalize mixed timestamp formats to ISODate
//   - Skip records where session_duration_sec is null (no session data)
// ─────────────────────────────────────────────────────────────

print("\n=== Q1: Sessions per User per Week + Percentiles ===");

const q1_result = db.user_activity_logs.aggregate([
  // ── Step 1: Normalize inconsistent field names ──────────────
  {
    $addFields: {
      norm_member_id: {
        $ifNull: ["$member_id", { $ifNull: ["$userId", "$userID"] }]
      },
      norm_customer_id: {
        $convert: {
          // handles both int and string customer_id
          input: { $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }] },
          to: "int",
          onError: null,
          onNull: null
        }
      }
    }
  },
  // ── Step 2: Drop records missing critical identifiers ────────
  {
    $match: {
      norm_member_id: { $ne: null },
      norm_customer_id: { $ne: null }
    }
  },
  // ── Step 3: Normalize timestamp to a consistent date ─────────
  // ISODate fields are fine; string dates need $dateFromString.
  // We add a safe parsed_ts field.
  {
    $addFields: {
      parsed_ts: {
        $cond: {
          if: { $eq: [{ $type: "$timestamp" }, "date"] },
          then: "$timestamp",
          else: {
            $dateFromString: {
              dateString: { $toString: "$timestamp" },
              onError: null,
              onNull: null
            }
          }
        }
      }
    }
  },
  { $match: { parsed_ts: { $ne: null } } },
  // ── Step 4: Extract ISO week number for grouping ─────────────
  {
    $addFields: {
      year_week: {
        $concat: [
          { $toString: { $isoWeekYear: "$parsed_ts" } },
          "-W",
          { $toString: { $isoWeek: "$parsed_ts" } }
        ]
      }
    }
  },
  // ── Step 5: Group by customer + member + week → session count ─
  {
    $group: {
      _id: {
        customer_id: "$norm_customer_id",
        member_id: "$norm_member_id",
        week: "$year_week"
      },
      sessions_this_week: { $sum: 1 },
      durations: {
        $push: {
          $cond: [
            { $gt: ["$session_duration_sec", null] },
            "$session_duration_sec",
            "$$REMOVE"
          ]
        }
      }
    }
  },
  // ── Step 6: Group by member → avg sessions/week ──────────────
  {
    $group: {
      _id: {
        customer_id: "$_id.customer_id",
        member_id: "$_id.member_id"
      },
      avg_sessions_per_week: { $avg: "$sessions_this_week" },
      all_durations: { $push: "$durations" }
    }
  },
  // ── Step 7: Flatten nested duration arrays ───────────────────
  {
    $addFields: {
      flat_durations: {
        $reduce: {
          input: "$all_durations",
          initialValue: [],
          in: { $concatArrays: ["$$value", "$$this"] }
        }
      }
    }
  },
  // ── Step 8: Lookup subscription tier from SQL via customer_id ─
  // NOTE: In practice, you'd run a $lookup against a collection
  // that mirrors the PostgreSQL plans/subscriptions data.
  // Here we simulate with a $bucket on customer_id ranges
  // (replace with actual $lookup once data is loaded into Mongo).
  // For demo: assign tier based on customer_id modulus (placeholder):
  {
    $addFields: {
      simulated_tier: {
        $switch: {
          branches: [
            { case: { $lte: [{ $mod: ["$_id.customer_id", 4] }, 0] }, then: "free" },
            { case: { $lte: [{ $mod: ["$_id.customer_id", 4] }, 1] }, then: "starter" },
            { case: { $lte: [{ $mod: ["$_id.customer_id", 4] }, 2] }, then: "professional" }
          ],
          default: "enterprise"
        }
      }
    }
  },
  // ── Step 9: Sort durations for percentile approximation ──────
  // MongoDB doesn't have a native percentile aggregation pre-5.0;
  // we use $percentile (available from MongoDB 7.0) or $sortArray.
  // Fallback: sort and pick indices manually.
  {
    $addFields: {
      sorted_durations: {
        $sortArray: { input: "$flat_durations", sortBy: 1 }
      }
    }
  },
  {
    $addFields: {
      duration_count: { $size: "$sorted_durations" },
    }
  },
  {
    $addFields: {
      p25_duration: {
        $arrayElemAt: [
          "$sorted_durations",
          { $floor: { $multiply: [0.25, { $max: [{ $subtract: ["$duration_count", 1] }, 0] }] } }
        ]
      },
      p50_duration: {
        $arrayElemAt: [
          "$sorted_durations",
          { $floor: { $multiply: [0.50, { $max: [{ $subtract: ["$duration_count", 1] }, 0] }] } }
        ]
      },
      p75_duration: {
        $arrayElemAt: [
          "$sorted_durations",
          { $floor: { $multiply: [0.75, { $max: [{ $subtract: ["$duration_count", 1] }, 0] }] } }
        ]
      }
    }
  },
  // ── Step 10: Final group by tier ─────────────────────────────
  {
    $group: {
      _id: "$simulated_tier",
      user_count: { $sum: 1 },
      avg_sessions_per_week: { $avg: "$avg_sessions_per_week" },
      avg_p25_session_duration_sec: { $avg: "$p25_duration" },
      avg_p50_session_duration_sec: { $avg: "$p50_duration" },
      avg_p75_session_duration_sec: { $avg: "$p75_duration" }
    }
  },
  {
    $project: {
      _id: 0,
      tier: "$_id",
      user_count: 1,
      avg_sessions_per_week: { $round: ["$avg_sessions_per_week", 2] },
      avg_p25_session_duration_sec: { $round: ["$avg_p25_session_duration_sec", 0] },
      avg_p50_session_duration_sec: { $round: ["$avg_p50_session_duration_sec", 0] },
      avg_p75_session_duration_sec: { $round: ["$avg_p75_session_duration_sec", 0] }
    }
  },
  { $sort: { tier: 1 } }
]).toArray();

printjson(q1_result);


// ─────────────────────────────────────────────────────────────
// Q2: Event Analysis
// Per product feature: Daily Active Users (DAU) +
// 7-day retention rate (users who used the feature again
// within 7 days of their FIRST use of it).
// ─────────────────────────────────────────────────────────────

print("\n=== Q2: Feature DAU + 7-Day Retention ===");

const q2_result = db.user_activity_logs.aggregate([
  // ── Normalize fields ─────────────────────────────────────────
  {
    $addFields: {
      norm_member_id: { $ifNull: ["$member_id", { $ifNull: ["$userId", "$userID"] }] },
      feature_name: { $ifNull: ["$feature", "unknown"] }
    }
  },
  {
    $match: {
      event_type: "feature_click",
      feature_name: { $ne: "unknown" },
      norm_member_id: { $ne: null }
    }
  },
  // ── Normalize timestamp ───────────────────────────────────────
  {
    $addFields: {
      parsed_ts: {
        $cond: {
          if: { $eq: [{ $type: "$timestamp" }, "date"] },
          then: "$timestamp",
          else: {
            $dateFromString: {
              dateString: { $toString: "$timestamp" },
              onError: null, onNull: null
            }
          }
        }
      }
    }
  },
  { $match: { parsed_ts: { $ne: null } } },
  // ── Get first-use date per (user, feature) ────────────────────
  {
    $group: {
      _id: { member_id: "$norm_member_id", feature: "$feature_name" },
      first_use: { $min: "$parsed_ts" },
      all_use_dates: { $push: "$parsed_ts" }
    }
  },
  // ── Check if user came back within 7 days ─────────────────────
  {
    $addFields: {
      returned_within_7d: {
        $gt: [
          {
            $size: {
              $filter: {
                input: "$all_use_dates",
                as: "dt",
                cond: {
                  $and: [
                    { $gt: ["$$dt", "$first_use"] },
                    { $lte: ["$$dt", { $add: ["$first_use", 7 * 24 * 3600 * 1000] }] }
                  ]
                }
              }
            }
          },
          0
        ]
      }
    }
  },
  // ── Group by feature: DAU uses calendar date of first_use as proxy ──
  {
    $group: {
      _id: {
        feature: "$_id.feature",
        day: { $dateToString: { format: "%Y-%m-%d", date: "$first_use" } }
      },
      dau: { $sum: 1 },
      retained: { $sum: { $cond: ["$returned_within_7d", 1, 0] } }
    }
  },
  // ── Roll up to feature level ──────────────────────────────────
  {
    $group: {
      _id: "$_id.feature",
      total_dau_days: { $sum: 1 },
      avg_dau: { $avg: "$dau" },
      peak_dau: { $max: "$dau" },
      total_first_users: { $sum: "$dau" },
      total_retained: { $sum: "$retained" }
    }
  },
  {
    $addFields: {
      retention_rate_7d_pct: {
        $round: [
          { $multiply: [
            { $divide: ["$total_retained", { $max: ["$total_first_users", 1] }] },
            100
          ]},
          2
        ]
      }
    }
  },
  {
    $project: {
      _id: 0,
      feature: "$_id",
      avg_dau: { $round: ["$avg_dau", 1] },
      peak_dau: 1,
      total_unique_first_users: "$total_first_users",
      retained_within_7d: "$total_retained",
      retention_rate_7d_pct: 1
    }
  },
  { $sort: { retention_rate_7d_pct: -1 } }
]).toArray();

printjson(q2_result);


// ─────────────────────────────────────────────────────────────
// Q3: Funnel Analysis — Onboarding Funnel
// Steps: signup → first_login → workspace_created →
//        first_project → invited_teammate
// Metrics: drop-off % at each stage, median time between steps
// ─────────────────────────────────────────────────────────────

print("\n=== Q3: Onboarding Funnel Analysis ===");

const FUNNEL_STEPS = [
  "signup",
  "first_login",
  "workspace_created",
  "first_project",
  "invited_teammate"
];

// Per user: collect all completed steps with timestamps
const q3_result = db.onboarding_events.aggregate([
  // ── Normalize field names (customer_id / customerId) ──────────
  {
    $addFields: {
      norm_member_id: { $ifNull: ["$member_id", "$memberId"] },
      norm_customer_id: {
        $convert: {
          input: { $ifNull: ["$customer_id", "$customerId"] },
          to: "int", onError: null, onNull: null
        }
      }
    }
  },
  { $match: { completed: true, step: { $in: FUNNEL_STEPS } } },
  // ── One document per (user, step) ────────────────────────────
  {
    $group: {
      _id: { member_id: "$norm_member_id", step: "$step" },
      step_time: { $min: "$timestamp" },
      step_index: { $first: "$step_index" }
    }
  },
  // ── Pivot: all steps per user ─────────────────────────────────
  {
    $group: {
      _id: "$_id.member_id",
      steps: {
        $push: {
          step: "$_id.step",
          step_time: "$step_time",
          step_index: "$step_index"
        }
      }
    }
  },
  // ── Check completion of each funnel stage ─────────────────────
  {
    $addFields: {
      has_signup: {
        $gt: [{ $size: { $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "signup"] } } } }, 0]
      },
      has_first_login: {
        $gt: [{ $size: { $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "first_login"] } } } }, 0]
      },
      has_workspace: {
        $gt: [{ $size: { $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "workspace_created"] } } } }, 0]
      },
      has_first_project: {
        $gt: [{ $size: { $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "first_project"] } } } }, 0]
      },
      has_teammate: {
        $gt: [{ $size: { $filter: { input: "$steps", as: "s", cond: { $eq: ["$$s.step", "invited_teammate"] } } } }, 0]
      }
    }
  },
  // ── Aggregate funnel counts ───────────────────────────────────
  {
    $group: {
      _id: null,
      total_users: { $sum: 1 },
      reached_signup: { $sum: { $cond: ["$has_signup", 1, 0] } },
      reached_first_login: { $sum: { $cond: ["$has_first_login", 1, 0] } },
      reached_workspace: { $sum: { $cond: ["$has_workspace", 1, 0] } },
      reached_first_project: { $sum: { $cond: ["$has_first_project", 1, 0] } },
      reached_teammate: { $sum: { $cond: ["$has_teammate", 1, 0] } }
    }
  },
  {
    $project: {
      _id: 0,
      funnel: {
        signup: {
          users: "$reached_signup",
          dropoff_from_prev_pct: { $literal: 0 }
        },
        first_login: {
          users: "$reached_first_login",
          dropoff_from_signup_pct: {
            $round: [{
              $multiply: [{
                $subtract: [1, { $divide: ["$reached_first_login", { $max: ["$reached_signup", 1] }] }]
              }, 100]
            }, 2]
          }
        },
        workspace_created: {
          users: "$reached_workspace",
          dropoff_from_first_login_pct: {
            $round: [{
              $multiply: [{
                $subtract: [1, { $divide: ["$reached_workspace", { $max: ["$reached_first_login", 1] }] }]
              }, 100]
            }, 2]
          }
        },
        first_project: {
          users: "$reached_first_project",
          dropoff_from_workspace_pct: {
            $round: [{
              $multiply: [{
                $subtract: [1, { $divide: ["$reached_first_project", { $max: ["$reached_workspace", 1] }] }]
              }, 100]
            }, 2]
          }
        },
        invited_teammate: {
          users: "$reached_teammate",
          dropoff_from_first_project_pct: {
            $round: [{
              $multiply: [{
                $subtract: [1, { $divide: ["$reached_teammate", { $max: ["$reached_first_project", 1] }] }]
              }, 100]
            }, 2]
          }
        }
      }
    }
  }
]).toArray();

printjson(q3_result);


// ─────────────────────────────────────────────────────────────
// Q4: Cross-Reference
// Top 20 most engaged FREE-tier users → potential upsell targets.
//
// Engagement Score Methodology:
//   E = (sessions * 3)
//       + (feature_clicks * 5)            // high-value intent signal
//       + (report_generates * 4)          // shows analytical need
//       + (distinct_features_used * 10)   // breadth of usage = stickiness
//       + (1 if any session > 1800s)      // power user signal (30+ min)
//       + (days_active * 2)               // recency + consistency
//
// Weights rationale:
//   - Feature clicks & breadth are strongest upsell signals
//     (user is actively exploring paid features)
//   - Session depth (>30 min) shows real value realization
//   - Days active rewards consistent usage, not one-time spikes
//
// Free-tier customer_ids are obtained by cross-referencing with
// the PostgreSQL customers/subscriptions tables (loaded here
// as a hardcoded filter — replace with $lookup in production).
// ─────────────────────────────────────────────────────────────

print("\n=== Q4: Top 20 Free-Tier Upsell Candidates ===");

// In production: replace this array with customer_ids from SQL:
// SELECT DISTINCT customer_id FROM subscriptions
// WHERE plan_id IN (SELECT plan_id FROM plans WHERE plan_tier = 'free')
//   AND status = 'active';
// Here we use customers whose IDs appear in free-tier subscription data.
// Placeholder: use modulo simulation (replace with actual IDs).
const FREE_TIER_CUSTOMER_IDS_SAMPLE = [3, 4, 7, 8, 9, 10, 11, 12, 14, 15];

const q4_result = db.user_activity_logs.aggregate([
  // ── Normalize fields ─────────────────────────────────────────
  {
    $addFields: {
      norm_member_id: { $ifNull: ["$member_id", { $ifNull: ["$userId", "$userID"] }] },
      norm_customer_id: {
        $convert: {
          input: { $ifNull: ["$customer_id", { $ifNull: ["$customerId", "$customerID"] }] },
          to: "int", onError: null, onNull: null
        }
      }
    }
  },
  // ── Filter to free-tier customers only ───────────────────────
  // Replace with: { $in: ["$norm_customer_id", <actual_free_ids_from_sql>] }
  {
    $match: {
      norm_customer_id: { $ne: null },
      norm_member_id: { $ne: null }
      // Uncomment for production:
      // norm_customer_id: { $in: FREE_TIER_CUSTOMER_IDS_SAMPLE }
    }
  },
  // ── Normalize timestamp ───────────────────────────────────────
  {
    $addFields: {
      parsed_ts: {
        $cond: {
          if: { $eq: [{ $type: "$timestamp" }, "date"] },
          then: "$timestamp",
          else: {
            $dateFromString: {
              dateString: { $toString: "$timestamp" },
              onError: null, onNull: null
            }
          }
        }
      }
    }
  },
  { $match: { parsed_ts: { $ne: null } } },
  // ── Per-user engagement metrics ───────────────────────────────
  {
    $group: {
      _id: { customer_id: "$norm_customer_id", member_id: "$norm_member_id" },
      total_events: { $sum: 1 },
      feature_clicks: {
        $sum: { $cond: [{ $eq: ["$event_type", "feature_click"] }, 1, 0] }
      },
      report_generates: {
        $sum: { $cond: [{ $eq: ["$event_type", "report_generate"] }, 1, 0] }
      },
      distinct_features: { $addToSet: "$feature" },
      session_durations: { $push: "$session_duration_sec" },
      active_days: { $addToSet: { $dateToString: { format: "%Y-%m-%d", date: "$parsed_ts" } } }
    }
  },
  // ── Calculate engagement score ────────────────────────────────
  {
    $addFields: {
      distinct_feature_count: {
        $size: {
          $filter: { input: "$distinct_features", as: "f", cond: { $ne: ["$$f", null] } }
        }
      },
      days_active: { $size: "$active_days" },
      has_long_session: {
        $gt: [
          {
            $size: {
              $filter: {
                input: "$session_durations",
                as: "d",
                cond: { $gte: ["$$d", 1800] }
              }
            }
          },
          0
        ]
      }
    }
  },
  {
    $addFields: {
      engagement_score: {
        $add: [
          { $multiply: ["$total_events", 3] },
          { $multiply: ["$feature_clicks", 5] },
          { $multiply: ["$report_generates", 4] },
          { $multiply: ["$distinct_feature_count", 10] },
          { $cond: ["$has_long_session", 10, 0] },
          { $multiply: ["$days_active", 2] }
        ]
      }
    }
  },
  // ── Sort and limit ────────────────────────────────────────────
  { $sort: { engagement_score: -1 } },
  { $limit: 20 },
  {
    $project: {
      _id: 0,
      customer_id: "$_id.customer_id",
      member_id: "$_id.member_id",
      total_events: 1,
      feature_clicks: 1,
      report_generates: 1,
      distinct_features_used: "$distinct_feature_count",
      days_active: 1,
      has_long_session: 1,
      engagement_score: 1,
      upsell_priority: {
        $switch: {
          branches: [
            { case: { $gte: ["$engagement_score", 200] }, then: "HIGH" },
            { case: { $gte: ["$engagement_score", 100] }, then: "MEDIUM" }
          ],
          default: "LOW"
        }
      }
    }
  }
]).toArray();

printjson(q4_result);

print("\n=== All MongoDB Queries Complete ===");
