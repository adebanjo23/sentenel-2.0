# SENTINEL 2.0 — Frontend Specification

## Overview

SENTINEL is a Nigerian security intelligence platform. The backend collects data from Twitter, satellite imagery, and conflict databases, then runs an AI pipeline that produces **state-level threat assessments** and **alerts** for the 36 Nigerian states + FCT.

The frontend is a **command center dashboard** — dark-themed, military-grade, designed for analysts monitoring security across Nigeria in real time. Think Bloomberg Terminal meets Joint Operations Center. Clean, dense, no wasted space.

**Backend:** FastAPI running on `http://localhost:8000`
**API docs:** `http://localhost:8000/docs` (Swagger UI — use this to test every endpoint)

---

## Design Principles

- **Dark theme only.** Black/dark gray background, high-contrast text. No light mode.
- **Information dense.** Every pixel should communicate something. No hero sections, no marketing fluff, no empty space.
- **Military aesthetic.** Monospace fonts for data. Sharp corners. Subtle grid lines. Status indicators use color-coded dots/bars, not icons.
- **Color system:**
  - `#FF3B3B` — CRITICAL (red, pulsing)
  - `#FF9500` — HIGH (orange)
  - `#FFD60A` — ELEVATED (yellow)
  - `#30D158` — NORMAL (green)
  - `#0A84FF` — accent/interactive (blue)
  - `#1C1C1E` — background
  - `#2C2C2E` — card/panel background
  - `#3A3A3C` — borders
  - `#FFFFFF` — primary text
  - `#8E8E93` — secondary text
- **Typography:** Use a monospace font for all data (JetBrains Mono, Fira Code, or SF Mono). Use a clean sans-serif (Inter, SF Pro) for narrative text only.
- **No animations except:** pulsing for CRITICAL alerts, smooth transitions for panel changes, subtle fade-in for new data.
- **Responsive but desktop-first.** Primary use case is a 1920x1080+ display. Mobile is secondary.

---

## Pages & Layout

### 1. Dashboard (Main Page) — `/`

This is the primary view. Three-panel layout:

```
┌──────────────────────────────┬───────────────────────────────┐
│                              │                               │
│     NATIONAL THREAT MAP      │     STATE DETAIL PANEL        │
│     (interactive map)        │     (selected state)          │
│                              │                               │
│                              │                               │
│                              │                               │
├──────────────────────────────┴───────────────────────────────┤
│                                                              │
│     ALERT FEED + INCIDENT TIMELINE (tabbed or split)         │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Top bar:** SENTINEL 2.0 logo (left), system status indicators (center: scheduler running/paused, last cycle time, next cycle time), current UTC time (right).

---

#### 1A. National Threat Map (left panel)

An interactive map of Nigeria showing all 36 states + FCT, color-coded by threat level.

**API:** `GET /api/threats/`

**Response:**
```json
[
  {
    "state": "Plateau",
    "threat_level": "CRITICAL",
    "incident_count": 24,
    "incident_rate": 8.0,
    "baseline_rate": 0.1,
    "acceleration": 80.0,
    "fatalities": 175,
    "lgas_affected": 3,
    "repeat_lgas": ["Jos", "Jos North"],
    "last_assessment_at": "2026-03-31T18:59:12",
    "updated_at": "2026-03-31T18:59:12"
  },
  ...
]
```

**Behavior:**
- Each state is a clickable polygon filled with its threat level color
- On hover: tooltip with state name, threat level, incident count, fatalities
- On click: loads that state's detail in the right panel
- States with no data render as dark gray (no assessment yet)
- CRITICAL states should have a subtle pulsing glow effect
- Small dot indicators on LGAs with repeat incidents if zoom level permits

**Map library:** Use Mapbox GL JS or Leaflet with GeoJSON boundaries for Nigerian states. Nigerian state boundary GeoJSON is widely available.

---

#### 1B. State Detail Panel (right panel)

Shows the full threat assessment for the selected state.

**API:** `GET /api/threats/{state}`

**Response:**
```json
{
  "state": "Plateau",
  "threat_level": "CRITICAL",
  "metrics": {
    "incident_count": 24,
    "incident_rate": 8.0,
    "baseline_rate": 0.1,
    "acceleration": 80.0,
    "severity_distribution": {"critical": 5, "high": 8, "moderate": 11},
    "category_mix": {"report": 19, "tension": 4, "warning": 1},
    "lgas_affected": 3,
    "repeat_lgas": ["Jos", "Jos North"],
    "fatalities": 175
  },
  "assessments": [
    {
      "id": 1,
      "threat_level": "CRITICAL",
      "previous_level": "NORMAL",
      "primary_threat_areas": ["Jos", "Jos North"],
      "threat_timeframe": "Immediate — through Easter period",
      "key_indicators": [
        "175 fatalities reported in 72 hours",
        "Incident rate 80x above baseline",
        "Attackers using fake military uniforms",
        "Repeat targeting of Jos, Jos North"
      ],
      "specific_warnings": [
        "CAN urges churches to remain vigilant"
      ],
      "recommended_actions": [
        "Increase security at religious gatherings through Easter",
        "Reinforce military patrols in Jos North"
      ],
      "narrative_summary": "Plateau State, particularly Jos and Jos North LGAs...",
      "tweets_analyzed": 24,
      "created_at": "2026-03-31T18:59:12"
    }
  ]
}
```

**Layout:**
```
PLATEAU STATE                           ● CRITICAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[Threat level badge — large, color-coded]

ASSESSMENT — Mar 31, 2026 18:59 UTC
[Narrative summary text — sans-serif, readable]

PRIMARY AREAS          TIMEFRAME
• Jos North LGA        Immediate — through Easter
• Jos LGA

KEY INDICATORS
• 175 fatalities in 72 hours
• 80x acceleration above baseline
• Fake military uniforms reported
• Repeat targeting of same LGAs

⚠ WARNINGS
• CAN urges churches to remain vigilant

RECOMMENDED ACTIONS
□ Increase security at religious gatherings
□ Reinforce military patrols in Jos North
□ Enforce motorcycle ban

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
METRICS
Incidents   24        Categories
Rate        8.0/day   ██████████████ report (19)
Baseline    0.1/day   ████ tension (4)
Accel       80.0x     █ warning (1)
Fatalities  175
LGAs        3
```

**When no state is selected:** Show a summary table of all states sorted by threat level (same data as the map, in tabular form).

---

#### 1C. Bottom Panel — Alert Feed & Timeline

Two tabs: **ALERTS** and **TIMELINE**

**ALERTS tab:**

**API:** `GET /api/alerts/?limit=50`

**Response:**
```json
[
  {
    "id": 1,
    "state": "Plateau",
    "alert_type": "new_critical",
    "severity": "CRITICAL",
    "previous_level": "NORMAL",
    "new_level": "CRITICAL",
    "title": "Plateau threat level escalated: NORMAL → CRITICAL",
    "summary": "Plateau State facing critical security crisis...",
    "primary_threat_areas": ["Jos", "Jos North"],
    "recommended_actions": ["Increase security at religious gatherings..."],
    "acknowledged": false,
    "created_at": "2026-03-31T18:59:12"
  }
]
```

**Layout:** Scrolling list, newest first. Each alert is a card:

```
▲ CRITICAL   Plateau   19:59 UTC                    [ACKNOWLEDGE]
  Plateau threat level escalated: NORMAL → CRITICAL
  Areas: Jos, Jos North | 175 fatalities
  ─────────────────────────────────────────────────
▲ ELEVATED   Borno     19:59 UTC                    [ACKNOWLEDGE]
  Borno threat level escalated: NORMAL → ELEVATED
  Areas: Damboa, Gwoza, Askira Uba
```

- Unacknowledged CRITICAL alerts: red left border + subtle pulse
- Unacknowledged HIGH/ELEVATED: orange/yellow left border
- Acknowledged: muted, gray left border
- Clicking ACKNOWLEDGE calls `POST /api/alerts/{id}/acknowledge`
- Clicking the alert row navigates to that state's detail panel

**TIMELINE tab:**

**API:** `GET /api/events/?limit=100`

**Response:**
```json
[
  {
    "id": 1,
    "title": "ISWAP attack on military base in Malam Fatori",
    "event_type": "attack",
    "severity": "critical",
    "confidence": "moderate",
    "confidence_score": 0.25,
    "location": "Malam Fatori",
    "state": "Borno",
    "lga": "Abadam",
    "summary": "...",
    "actors": "ISWAP,Nigerian Army",
    "fatality_estimate": 12,
    "sources": {"twitter": 3, "firms": 0, "acled": 0},
    "first_reported": "2026-03-29T10:57:00",
    "last_updated": "2026-03-31T18:58:47"
  }
]
```

Display as a horizontal timeline or a scrollable event list. Each event is a card showing title, severity badge, state, time, and source count badges (Twitter/FIRMS/ACLED).

---

### 2. Events Page — `/events`

Detailed view of individual security events.

**List view:**

**API:** `GET /api/events/?status=active&limit=50`

Filterable by: status (active/resolved/false_positive), severity, state. Sortable by date, severity, fatalities.

Table columns: Severity | Title | State | LGA | Fatalities | Sources (T/F/A) | First Reported | Last Updated

**Detail view** (click an event):

**API:** `GET /api/events/{event_id}`

**Response includes linked tweets:**
```json
{
  "id": 1,
  "title": "...",
  "tweets": [
    {
      "tweet_id": "2038541751971889439",
      "author": "eonsintelligenc",
      "content": "Video Update: Not less than 20 people confirmed dead...",
      "posted_at": "2026-03-30T09:00:02",
      "likes": 45,
      "retweets": 120
    }
  ]
}
```

Show the event summary, then a list of supporting tweets with author, content, time, and engagement metrics. This is the provenance chain — every intelligence assessment traces back to actual sources.

---

### 3. Pipeline Page — `/pipeline`

Monitor and control the intelligence processing pipeline.

**API:** `GET /api/pipeline/status`

**Response:**
```json
[
  {
    "id": 1,
    "status": "completed",
    "started_at": "2026-03-31T18:56:10",
    "completed_at": "2026-03-31T18:59:12",
    "stages": {
      "filter": {"tweets_in": 500, "passed": 160, "filtered": 340, "completed": true},
      "classify": {"classified": 160, "events_created": 12, "events_updated": 9, "completed": true},
      "aggregate": {"states_analyzed": 16, "states_flagged": 4, "completed": true},
      "assess": {"assessments": 4, "completed": true},
      "alert": {"alerts": 3, "completed": true}
    },
    "error": null
  }
]
```

**Layout:** Show pipeline runs as rows in a table. Each row expands to show the 5-stage breakdown with a visual pipeline diagram:

```
Pipeline Run #1 — Completed — Mar 31, 18:56 → 18:59 (3m 2s)

  ┌─────────┐    ┌──────────┐    ┌───────────┐    ┌────────┐    ┌───────┐
  │ FILTER  │ →  │ CLASSIFY │ →  │ AGGREGATE │ →  │ ASSESS │ →  │ ALERT │
  │ 160/500 │    │ 160      │    │ 4/16      │    │ 4      │    │ 3     │
  │ passed  │    │ tweets   │    │ flagged   │    │ assess │    │alerts │
  └─────────┘    └──────────┘    └───────────┘    └────────┘    └───────┘
       ✓              ✓               ✓               ✓             ✓
```

**Trigger button:** "Run Pipeline Now" → calls `POST /api/pipeline/run` with `{}`

---

### 4. Scheduler Page — `/scheduler`

Control the automatic monitoring loop.

**API:** `GET /api/scheduler/status`

**Response:**
```json
{
  "running": true,
  "paused": false,
  "cycle_count": 3,
  "error_count": 0,
  "last_run_at": "2026-03-31T18:00:00",
  "next_run_at": "2026-04-01T02:00:00",
  "last_result": {
    "accounts": 68,
    "completed": 67,
    "failed": 1,
    "tweets": 1200,
    "new": 340
  },
  "last_error": null
}
```

**Layout:**

```
SCHEDULER                        ● RUNNING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Interval: Every 8 hours
Cycle:    #3
Errors:   0

Last run: Mar 31, 18:00 UTC
Next run: Apr 1, 02:00 UTC   (in 5h 23m)

Last Result:
  Accounts scraped: 67/68
  Tweets collected: 1,200
  New tweets:       340
  Failed accounts:  1

[START]  [STOP]  [PAUSE]  [RESUME]
```

**Controls:**
- `POST /api/scheduler/start` — start
- `POST /api/scheduler/stop` — stop
- `POST /api/scheduler/pause` — pause
- `POST /api/scheduler/resume` — resume

---

### 5. Data Sources Page — `/sources`

Overview of all data collection sources.

**APIs:**
- `GET /api/twitter/status`
- `GET /api/acled/status`
- `GET /api/firms/status`
- `GET /api/tiktok/status`

Show each source as a card with: total records, date range, last sync time, top contributors (for Twitter: top authors). Include manual sync buttons:

- "Sync ACLED" → `POST /api/acled/sync`
- "Fetch FIRMS" → `POST /api/firms/fetch`
- "Scrape TikTok" → `POST /api/tiktok/scrape`

---

### 6. Navigation

Sidebar navigation (left, collapsible):

```
┌──────────────────┐
│ ◉ SENTINEL 2.0   │
│                   │
│ ▣ Dashboard       │
│ ⚡ Events         │
│ ⬡ Pipeline        │
│ ⏱ Scheduler       │
│ ◎ Data Sources    │
│                   │
│ ─── System ───    │
│ ⚙ Settings        │
└──────────────────┘
```

---

## API Reference (Complete)

### Threat Intelligence

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/threats/` | All state threat levels, sorted by severity |
| `GET` | `/api/threats/{state}` | Single state: current level + recent assessments |
| `GET` | `/api/threats/{state}/history` | Historical assessments for a state |

### Alerts

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/alerts/` | List alerts. Filters: `?state=`, `?severity=`, `?acknowledged=` |
| `GET` | `/api/alerts/{id}` | Single alert detail |
| `POST` | `/api/alerts/{id}/acknowledge` | Mark alert as acknowledged |

### Events

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/events/` | List events. Filters: `?status=`, `?severity=`, `?state=`, `?limit=` |
| `GET` | `/api/events/{id}` | Event detail with linked tweets |
| `POST` | `/api/events/process` | Manually trigger old-style event processing |

### Pipeline

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/pipeline/run` | Trigger pipeline. Body: `{"stages": [1,2,3,4,5]}` or `{}` for all |
| `GET` | `/api/pipeline/status` | List recent pipeline runs |
| `GET` | `/api/pipeline/runs/{id}` | Single run detail with per-stage stats |

### Scheduler

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/scheduler/status` | Current scheduler state |
| `POST` | `/api/scheduler/start` | Start automatic monitoring |
| `POST` | `/api/scheduler/stop` | Stop |
| `POST` | `/api/scheduler/pause` | Pause (skip cycles) |
| `POST` | `/api/scheduler/resume` | Resume |

### Data Sources

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/twitter/status` | Twitter collection stats |
| `POST` | `/api/twitter/search` | Search Twitter. Body: `{"query": "...", "count": 50}` |
| `GET` | `/api/acled/status` | ACLED stats |
| `POST` | `/api/acled/sync` | Trigger ACLED sync. Params: `?since=`, `?historical=` |
| `GET` | `/api/firms/status` | FIRMS stats |
| `POST` | `/api/firms/fetch` | Fetch satellite data. Params: `?days=`, `?all_sources=` |
| `GET` | `/api/tiktok/status` | TikTok stats |
| `POST` | `/api/tiktok/scrape` | Run TikTok scrape |
| `POST` | `/api/monitor/run` | Scrape watchlist. Body: `{"tier": "1", "max_tweets_per_account": 20}` |
| `GET` | `/api/monitor/watchlist` | View the 68-account watchlist |
| `GET` | `/api/health` | Health check |

---

## Tech Recommendations

- **Framework:** Next.js 14+ (App Router) or React + Vite
- **Map:** Mapbox GL JS (preferred for styling control) or Leaflet + react-leaflet
- **Charts:** Recharts or Tremor for the metrics visualizations
- **State:** TanStack Query (React Query) for API data fetching with auto-refetch
- **Styling:** Tailwind CSS with a custom dark theme config
- **Auto-refresh:** Poll `/api/threats/` and `/api/alerts/` every 30 seconds. Poll `/api/scheduler/status` every 60 seconds. Use React Query's `refetchInterval`.

---

## Code Quality Expectations

- TypeScript strict mode throughout
- All API responses typed with interfaces
- Components are small, single-responsibility
- No `any` types — define proper interfaces for every API response
- Error states handled for every API call (loading, error, empty)
- Responsive but optimize for 1920x1080+
- Accessible (keyboard navigation, screen reader labels on interactive elements)
- No unused imports, no console.logs in production
- Git commits are atomic and descriptive
