# 🗺️ Location Intelligence API

**Unified API for Walk Score, School Ratings, and Crime Data**

## Overview

Location Intelligence API provides a unified interface for enriching property data with:
- **Walk Score** - Walkability, Transit, and Bike scores (0-100)
- **School Ratings** - NCES + Florida DOE school grades
- **Crime Data** - FBI UCR + FDLE county-level crime indices

Built for **BidDeed.AI** foreclosure auction platform and **ZoneWise.ai** zoning intelligence.

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Run enrichment for Malabar
python -m src.enrichment.pipeline --jurisdiction 14
```

## Data Sources

| Source | Type | Cost | Coverage |
|--------|------|------|----------|
| Walk Score API | REST | FREE (5K/day) | US addresses |
| NCES EDGE | ArcGIS REST | FREE | US Schools |
| Florida DOE | CSV Download | FREE | FL School Grades |
| FBI CDE | REST | FREE | US Counties |
| FDLE UCR | CSV Download | FREE | FL Counties |

## Malabar POC Results

| Metric | Value | Description |
|--------|-------|-------------|
| Walk Score | 20 | Car-dependent (rural area) |
| School Score | 82 | B-rated schools nearby |
| Crime Score | 60 | Safer than FL average |

## Score Methodology

### Walk Score (0-100)
- 90-100: Walker's Paradise
- 70-89: Very Walkable
- 50-69: Somewhat Walkable
- 25-49: Car-Dependent
- 0-24: Almost All Errands Require Car

### School Score (0-100)
- Based on Florida DOE school grades (A=95, B=82, C=70, D=55, F=35)
- Weighted by distance to property

### Crime Score (0-100)
- Higher = Safer
- Calculated: 100 - (county_rate / florida_avg * 50)
- Brevard County: 60 (lower crime than FL average)

## Integration

### ZoneWise.ai

```python
from location_intelligence import get_location_scores

scores = get_location_scores(lat=28.00, lon=-80.58)
print(f"Walk: {scores.walk_score}")
print(f"School: {scores.school_score}")
print(f"Crime: {scores.crime_score}")
```

---

**Part of the BidDeed.AI / ZoneWise.ai ecosystem**
