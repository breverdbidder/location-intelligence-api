#!/usr/bin/env python3
"""
Location Intelligence Integration for ZoneWise
Enriches properties with Walk Score, School Ratings, and Crime Data

Data Sources:
- NCES EDGE API: School locations and data
- Florida DOE: School grades (A-F)
- FBI UCR / County Stats: Crime indices
- Walk Score API: Walkability (requires API key)

For Malabar POC: Uses free data sources to populate school_score and crime_score
"""

import json
import time
import math
import urllib.request
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import List, Optional, Dict

# Configuration
SUPABASE_URL = "https://mocerqjnksmhcjzxrewo.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1vY2VycWpua3NtaGNqenhyZXdvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NDUzMjUyNiwiZXhwIjoyMDgwMTA4NTI2fQ.fL255mO0V8-rrU0Il3L41cIdQXUau-HRQXiamTqp9nE"

MALABAR_JURISDICTION_ID = 14

# Brevard County crime statistics (2023 FDLE UCR data)
# Source: https://www.fdle.state.fl.us/CJAB/UCR/Annual-Reports/UCR-Offense-Data
BREVARD_CRIME_STATS = {
    "county": "Brevard",
    "year": 2023,
    "population": 617_080,
    "violent_crime_rate": 392.4,  # per 100K
    "property_crime_rate": 1842.1,  # per 100K
    "total_crime_rate": 2234.5,
    # Florida average: ~2,800 per 100K, US average: ~2,400
    # Brevard is below average = safer
}

# Florida school grade to score mapping
SCHOOL_GRADE_SCORES = {
    "A": 95,
    "B": 82,
    "C": 70,
    "D": 55,
    "F": 35,
    "I": 50,  # Incomplete/Improving
    "N": None,  # No grade
}


@dataclass
class School:
    """School data from NCES"""
    nces_id: str
    name: str
    city: str
    state: str
    level: str  # Elementary, Middle, High
    lat: float
    lon: float
    distance_miles: float
    grade: Optional[str] = None
    score: Optional[int] = None


@dataclass
class LocationIntelligence:
    """Combined location intelligence data"""
    walk_score: Optional[int] = None
    bike_score: Optional[int] = None
    transit_score: Optional[int] = None
    school_score: Optional[int] = None
    nearest_school_name: Optional[str] = None
    nearest_school_grade: Optional[str] = None
    nearest_school_distance: Optional[float] = None
    crime_score: Optional[int] = None
    crime_index: Optional[int] = None


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles"""
    R = 3959  # Earth's radius in miles
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c


def get_nearby_schools(lat: float, lon: float, radius_miles: float = 5) -> List[School]:
    """Query NCES EDGE API for nearby schools"""
    # Convert radius to approximate degrees (1 degree ≈ 69 miles at equator)
    radius_deg = radius_miles / 69
    
    # Build bounding box
    min_lon = lon - radius_deg
    max_lon = lon + radius_deg
    min_lat = lat - radius_deg
    max_lat = lat + radius_deg
    
    url = (
        f"https://nces.ed.gov/opengis/rest/services/K12_School_Locations/"
        f"EDGE_GEOCODE_PUBLICSCH_2223/MapServer/0/query?"
        f"where=1%3D1&outFields=*"
        f"&geometry={min_lon},{min_lat},{max_lon},{max_lat}"
        f"&geometryType=esriGeometryEnvelope&inSR=4326"
        f"&spatialRel=esriSpatialRelIntersects&returnGeometry=true&f=json"
    )
    
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode())
    except Exception as e:
        print(f"    NCES API error: {e}")
        return []
    
    schools = []
    for feature in data.get('features', []):
        attrs = feature.get('attributes', {})
        geom = feature.get('geometry', {})
        
        school_lat = geom.get('y', 0)
        school_lon = geom.get('x', 0)
        
        if not school_lat or not school_lon:
            continue
        
        distance = haversine_distance(lat, lon, school_lat, school_lon)
        
        if distance <= radius_miles:
            level = attrs.get('LEVEL_', 'Unknown')
            if level == '1':
                level = 'Elementary'
            elif level == '2':
                level = 'Middle'
            elif level == '3':
                level = 'High'
            elif level == '4':
                level = 'Other'
            
            schools.append(School(
                nces_id=str(attrs.get('NCESSCH', '')),
                name=attrs.get('NAME', 'Unknown'),
                city=attrs.get('CITY', ''),
                state=attrs.get('STATE', ''),
                level=level,
                lat=school_lat,
                lon=school_lon,
                distance_miles=round(distance, 2)
            ))
    
    # Sort by distance
    schools.sort(key=lambda s: s.distance_miles)
    return schools


def get_florida_school_grades() -> Dict[str, str]:
    """
    Florida DOE school grades lookup.
    In production, this would download and parse the FLDOE spreadsheet.
    For POC, using hardcoded Brevard County schools.
    """
    # Brevard County schools near Malabar (from FLDOE 2024 School Grades)
    # Source: https://www.fldoe.org/accountability/accountability-reporting/school-grades/
    return {
        "PALM BAY ELEMENTARY": "B",
        "PALM BAY MAGNET HIGH": "B",
        "MALABAR INTERMEDIATE": "B",
        "JUPITER ELEMENTARY": "A",
        "GEMINI ELEMENTARY": "A",
        "SOUTHWEST MIDDLE": "C",
        "PALM BAY COMMUNITY CHARTER": "B",
        "HERITAGE HIGH": "B",
        "MELBOURNE HIGH": "A",
        "EAU GALLIE HIGH": "B",
        "BAYSIDE HIGH": "B",
        "WESTSIDE ELEMENTARY": "C",
        "RIVIERA ELEMENTARY": "B",
        "LOCKMAR ELEMENTARY": "B",
        "PORT MALABAR ELEMENTARY": "C",
    }


def calculate_crime_score(county_stats: dict) -> int:
    """
    Calculate crime score (0-100, higher = safer) based on county crime rates.
    Uses comparison to Florida and US averages.
    """
    # Florida average total crime rate: ~2,800 per 100K
    # US average: ~2,400 per 100K
    florida_avg = 2800
    
    county_rate = county_stats.get('total_crime_rate', florida_avg)
    
    # Score = 100 - (county_rate / florida_avg * 50)
    # Capped at 0-100
    # Lower crime rate = higher score
    score = 100 - (county_rate / florida_avg * 50)
    
    return max(0, min(100, int(score)))


def calculate_school_score(schools: List[School], florida_grades: Dict[str, str]) -> tuple:
    """
    Calculate school score based on nearest schools with Florida DOE grades.
    Returns (score, nearest_school_name, nearest_school_grade, distance)
    """
    if not schools:
        return None, None, None, None
    
    # Look for schools within 3 miles with grades
    weighted_scores = []
    nearest_with_grade = None
    
    for school in schools:
        if school.distance_miles > 5:
            continue
        
        # Try to match school name to Florida grades
        school_name_upper = school.name.upper()
        grade = None
        
        for name, g in florida_grades.items():
            if name in school_name_upper or school_name_upper in name:
                grade = g
                school.grade = grade
                school.score = SCHOOL_GRADE_SCORES.get(grade)
                break
        
        if grade and school.score:
            # Weight by distance (closer schools count more)
            weight = 1 / (1 + school.distance_miles)
            weighted_scores.append((school.score, weight))
            
            if nearest_with_grade is None:
                nearest_with_grade = school
    
    if not weighted_scores:
        # No graded schools found, use nearest school
        nearest = schools[0] if schools else None
        return None, nearest.name if nearest else None, None, nearest.distance_miles if nearest else None
    
    # Calculate weighted average
    total_score = sum(s * w for s, w in weighted_scores)
    total_weight = sum(w for _, w in weighted_scores)
    final_score = int(total_score / total_weight) if total_weight > 0 else None
    
    return (
        final_score,
        nearest_with_grade.name if nearest_with_grade else None,
        nearest_with_grade.grade if nearest_with_grade else None,
        nearest_with_grade.distance_miles if nearest_with_grade else None
    )


def get_location_intelligence(lat: float, lon: float) -> LocationIntelligence:
    """Get all location intelligence for a coordinate"""
    result = LocationIntelligence()
    
    # Get schools
    schools = get_nearby_schools(lat, lon, radius_miles=5)
    florida_grades = get_florida_school_grades()
    
    school_score, school_name, school_grade, school_dist = calculate_school_score(schools, florida_grades)
    result.school_score = school_score
    result.nearest_school_name = school_name
    result.nearest_school_grade = school_grade
    result.nearest_school_distance = school_dist
    
    # Get crime score (county-level for POC)
    result.crime_score = calculate_crime_score(BREVARD_CRIME_STATS)
    result.crime_index = 100 - result.crime_score  # Invert: higher index = more crime
    
    # Walk score requires API key - leave as None for now
    result.walk_score = None
    result.bike_score = None
    result.transit_score = None
    
    return result


def update_parcel_scores(parcel_id: int, scores: LocationIntelligence) -> bool:
    """Update parcel with location intelligence scores"""
    url = f"{SUPABASE_URL}/rest/v1/sample_properties?id=eq.{parcel_id}"
    
    update_data = {}
    
    if scores.school_score is not None:
        update_data["school_score"] = scores.school_score
    if scores.crime_score is not None:
        update_data["crime_score"] = scores.crime_score
    if scores.walk_score is not None:
        update_data["walk_score"] = scores.walk_score
    
    if not update_data:
        return False
    
    req = urllib.request.Request(
        url,
        data=json.dumps(update_data).encode('utf-8'),
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        },
        method='PATCH'
    )
    
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return True
    except Exception as e:
        print(f"    Error updating parcel {parcel_id}: {e}")
        return False


def process_malabar():
    """Process all Malabar parcels with location intelligence"""
    print("=" * 60)
    print("Location Intelligence Integration - Malabar POC")
    print("=" * 60)
    
    # Fetch Malabar parcels with centroids
    print("\n1. Fetching Malabar parcels...")
    
    all_parcels = []
    offset = 0
    
    while True:
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/sample_properties?jurisdiction_id=eq.{MALABAR_JURISDICTION_ID}"
            f"&centroid_lat=not.is.null&select=id,parcel_id,centroid_lat,centroid_lon&order=id",
            headers={"apikey": SUPABASE_KEY, "Range": f"{offset}-{offset+499}"}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            batch = json.loads(response.read().decode())
        
        if not batch:
            break
        
        all_parcels.extend(batch)
        offset += 500
        
        if len(batch) < 500:
            break
    
    print(f"   Found {len(all_parcels)} parcels with coordinates")
    
    # Process in batches
    print("\n2. Calculating location intelligence...")
    
    # Cache school lookups by rounded coordinates
    cache = {}
    stats = {"success": 0, "failed": 0}
    
    florida_grades = get_florida_school_grades()
    crime_score = calculate_crime_score(BREVARD_CRIME_STATS)
    
    for i, parcel in enumerate(all_parcels):
        lat = parcel['centroid_lat']
        lon = parcel['centroid_lon']
        
        # Round for caching (same location = same schools)
        cache_key = f"{round(lat, 3)},{round(lon, 3)}"
        
        if cache_key in cache:
            school_score = cache[cache_key]
        else:
            schools = get_nearby_schools(lat, lon, radius_miles=5)
            school_score, _, _, _ = calculate_school_score(schools, florida_grades)
            cache[cache_key] = school_score
            time.sleep(0.05)  # Rate limit NCES
        
        # Update parcel
        update_data = {
            "crime_score": crime_score
        }
        if school_score is not None:
            update_data["school_score"] = school_score
        
        url = f"{SUPABASE_URL}/rest/v1/sample_properties?id=eq.{parcel['id']}"
        req = urllib.request.Request(
            url,
            data=json.dumps(update_data).encode('utf-8'),
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal"
            },
            method='PATCH'
        )
        
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                stats["success"] += 1
        except:
            stats["failed"] += 1
        
        if (i + 1) % 200 == 0:
            print(f"   Processed {i + 1}/{len(all_parcels)}...")
    
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Total Parcels:    {len(all_parcels)}")
    print(f"Updated:          {stats['success']}")
    print(f"Failed:           {stats['failed']}")
    print(f"Cache Hits:       {len(all_parcels) - len(cache)} (from {len(cache)} unique locations)")
    print(f"Crime Score:      {crime_score} (county-wide)")
    print("=" * 60)


if __name__ == "__main__":
    process_malabar()
