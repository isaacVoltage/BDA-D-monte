import os
import sys
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.hotspot_detection import evaluate_hotspot_rules, calculate_wqi_row, CPCB_THRESHOLDS

def test_evaluate_pristine_record():
    # Pristine water record matching CPCB Class A/B standards
    pristine_row = {
        "dissolved_oxygen_mg_l": 8.5,
        "bod_mg_l": 1.2,
        "ph": 7.4,
        "fecal_coliform_mpn_100ml": 150,
        "nitrate_mg_l": 1.5,
        "turbidity_ntu": 4.0
    }
    is_hotspot, severity, violations, wqi = evaluate_hotspot_rules(pristine_row)

    assert is_hotspot is False
    assert severity == "Safe / Compliant"
    assert len(violations) == 0
    assert wqi >= 85.0

def test_evaluate_critical_hotspot_record():
    # Severe industrial / raw sewage pollution record
    critical_row = {
        "dissolved_oxygen_mg_l": 1.5,   # Critical hypoxia (< 4.0)
        "bod_mg_l": 18.0,               # Critical organic load (> 8.0)
        "ph": 8.8,                      # Alkaline anomaly (> 8.5)
        "fecal_coliform_mpn_100ml": 45000, # Massive coliform (> 2500)
        "nitrate_mg_l": 22.0,           # High nitrate (> 10.0)
        "turbidity_ntu": 65.0
    }
    is_hotspot, severity, violations, wqi = evaluate_hotspot_rules(critical_row)

    assert is_hotspot is True
    assert severity == "Critical"
    assert len(violations) >= 4
    assert any("Severe Organic Pollution" in v for v in violations)
    assert any("Critical DO Depletion" in v for v in violations)
    assert wqi < 40.0

def test_wqi_missing_parameter_handling():
    # Record with some missing fields should calculate without error by renormalizing weights
    partial_row = {
        "dissolved_oxygen_mg_l": 6.5,
        "bod_mg_l": 2.5,
        "ph": None,
        "fecal_coliform_mpn_100ml": None,
        "nitrate_mg_l": 3.0,
        "turbidity_ntu": None
    }
    wqi = calculate_wqi_row(partial_row)
    assert 0.0 <= wqi <= 100.0
    assert wqi > 70.0
