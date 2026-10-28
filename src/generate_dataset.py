import os
import sys
import random
from datetime import datetime, timedelta
import pandas as pd

# Real Indian River Monitoring Stations with accurate GPS coordinates and realistic environmental profiles
MONITORING_STATIONS = [
    # River Ganga (Uttarakhand, UP, Bihar, West Bengal)
    {"station_id": "CPCB_GAN_01", "station_name": "Rishikesh - Triveni Ghat", "river_name": "Ganga", "state": "Uttarakhand", "district": "Dehradun", "latitude": 30.1030, "longitude": 78.2948, "profile": "pristine"},
    {"station_id": "CPCB_GAN_02", "station_name": "Haridwar - Har Ki Pauri", "river_name": "Ganga", "state": "Uttarakhand", "district": "Haridwar", "latitude": 29.9560, "longitude": 78.1700, "profile": "moderate"},
    {"station_id": "CPCB_GAN_03", "station_name": "Kannauj - Rajghat", "river_name": "Ganga", "state": "Uttar Pradesh", "district": "Kannauj", "latitude": 27.0514, "longitude": 79.9249, "profile": "polluted"},
    {"station_id": "CPCB_GAN_04", "station_name": "Kanpur - Jajmau Tannery Area", "river_name": "Ganga", "state": "Uttar Pradesh", "district": "Kanpur Nagar", "latitude": 26.4358, "longitude": 80.4042, "profile": "critical"},
    {"station_id": "CPCB_GAN_05", "station_name": "Prayagraj - Sangam Point", "river_name": "Ganga", "state": "Uttar Pradesh", "district": "Prayagraj", "latitude": 25.4299, "longitude": 81.8845, "profile": "polluted"},
    {"station_id": "CPCB_GAN_06", "station_name": "Varanasi - Assi Ghat", "river_name": "Ganga", "state": "Uttar Pradesh", "district": "Varanasi", "latitude": 25.2882, "longitude": 83.0062, "profile": "polluted"},
    {"station_id": "CPCB_GAN_07", "station_name": "Patna - Gandhi Ghat", "river_name": "Ganga", "state": "Bihar", "district": "Patna", "latitude": 25.6207, "longitude": 85.1741, "profile": "polluted"},
    {"station_id": "CPCB_GAN_08", "station_name": "Kolkata - Howrah Riverside", "river_name": "Ganga", "state": "West Bengal", "district": "Kolkata", "latitude": 22.5850, "longitude": 88.3468, "profile": "critical"},

    # River Yamuna (Uttarakhand, Haryana, Delhi, UP)
    {"station_id": "CPCB_YAM_01", "station_name": "Dakpathar Barrage", "river_name": "Yamuna", "state": "Uttarakhand", "district": "Dehradun", "latitude": 30.5050, "longitude": 77.7850, "profile": "pristine"},
    {"station_id": "CPCB_YAM_02", "station_name": "Wazirabad Barrage U/S", "river_name": "Yamuna", "state": "Delhi", "district": "North Delhi", "latitude": 28.7135, "longitude": 77.2310, "profile": "moderate"},
    {"station_id": "CPCB_YAM_03", "station_name": "Delhi - ITO Bridge", "river_name": "Yamuna", "state": "Delhi", "district": "Central Delhi", "latitude": 28.6289, "longitude": 77.2514, "profile": "critical"},
    {"station_id": "CPCB_YAM_04", "station_name": "Delhi - Okhla Barrage D/S", "river_name": "Yamuna", "state": "Delhi", "district": "South East Delhi", "latitude": 28.5447, "longitude": 77.3094, "profile": "critical"},
    {"station_id": "CPCB_YAM_05", "station_name": "Mathura - Vishram Ghat", "river_name": "Yamuna", "state": "Uttar Pradesh", "district": "Mathura", "latitude": 27.5036, "longitude": 77.6838, "profile": "polluted"},
    {"station_id": "CPCB_YAM_06", "station_name": "Agra - Tajganj Downstream", "river_name": "Yamuna", "state": "Uttar Pradesh", "district": "Agra", "latitude": 27.1725, "longitude": 78.0435, "profile": "critical"},

    # River Godavari (Maharashtra, Telangana, Andhra Pradesh)
    {"station_id": "CPCB_GOD_01", "station_name": "Nashik - Trimbakeshwar", "river_name": "Godavari", "state": "Maharashtra", "district": "Nashik", "latitude": 19.9320, "longitude": 73.5307, "profile": "pristine"},
    {"station_id": "CPCB_GOD_02", "station_name": "Nashik - Ramkund", "river_name": "Godavari", "state": "Maharashtra", "district": "Nashik", "latitude": 20.0035, "longitude": 73.7915, "profile": "moderate"},
    {"station_id": "CPCB_GOD_03", "station_name": "Nanded - Old Bridge", "river_name": "Godavari", "state": "Maharashtra", "district": "Nanded", "latitude": 19.1450, "longitude": 77.3150, "profile": "polluted"},
    {"station_id": "CPCB_GOD_04", "station_name": "Rajahmundry - Godavari Arch Bridge", "river_name": "Godavari", "state": "Andhra Pradesh", "district": "East Godavari", "latitude": 17.0005, "longitude": 81.7774, "profile": "moderate"},

    # River Krishna (Maharashtra, Karnataka, Telangana, Andhra Pradesh)
    {"station_id": "CPCB_KRI_01", "station_name": "Mahabaleshwar Origin", "river_name": "Krishna", "state": "Maharashtra", "district": "Satara", "latitude": 17.9237, "longitude": 73.6586, "profile": "pristine"},
    {"station_id": "CPCB_KRI_02", "station_name": "Sangli - Irwin Bridge", "river_name": "Krishna", "state": "Maharashtra", "district": "Sangli", "latitude": 16.8524, "longitude": 74.5653, "profile": "polluted"},
    {"station_id": "CPCB_KRI_03", "station_name": "Almatti Dam Reservoir", "river_name": "Krishna", "state": "Karnataka", "district": "Bagalkot", "latitude": 16.3262, "longitude": 75.8920, "profile": "moderate"},
    {"station_id": "CPCB_KRI_04", "station_name": "Vijayawada - Prakasam Barrage", "river_name": "Krishna", "state": "Andhra Pradesh", "district": "NTR District", "latitude": 16.5097, "longitude": 80.6067, "profile": "polluted"},

    # River Kaveri / Cauvery (Karnataka, Tamil Nadu)
    {"station_id": "CPCB_KAV_01", "station_name": "Talakaveri Origin", "river_name": "Kaveri", "state": "Karnataka", "district": "Kodagu", "latitude": 12.3853, "longitude": 75.4925, "profile": "pristine"},
    {"station_id": "CPCB_KAV_02", "station_name": "Srirangapatna - Paschima Vahini", "river_name": "Kaveri", "state": "Karnataka", "district": "Mandya", "latitude": 12.4170, "longitude": 76.6850, "profile": "moderate"},
    {"station_id": "CPCB_KAV_03", "station_name": "Hogenakkal Falls Entry", "river_name": "Kaveri", "state": "Tamil Nadu", "district": "Dharmapuri", "latitude": 12.1186, "longitude": 77.7767, "profile": "moderate"},
    {"station_id": "CPCB_KAV_04", "station_name": "Tiruchirappalli - Grand Anicut", "river_name": "Kaveri", "state": "Tamil Nadu", "district": "Tiruchirappalli", "latitude": 10.8290, "longitude": 78.6910, "profile": "polluted"},

    # River Sabarmati (Gujarat)
    {"station_id": "CPCB_SAB_01", "station_name": "Gandhinagar - Sant Sarovar", "river_name": "Sabarmati", "state": "Gujarat", "district": "Gandhinagar", "latitude": 23.2350, "longitude": 72.6650, "profile": "moderate"},
    {"station_id": "CPCB_SAB_02", "station_name": "Ahmedabad - Nehru Bridge", "river_name": "Sabarmati", "state": "Gujarat", "district": "Ahmedabad", "latitude": 23.0270, "longitude": 72.5760, "profile": "critical"},
    {"station_id": "CPCB_SAB_03", "station_name": "Ahmedabad - Vasna Barrage D/S", "river_name": "Sabarmati", "state": "Gujarat", "district": "Ahmedabad", "latitude": 22.9850, "longitude": 72.5450, "profile": "critical"},

    # River Narmada (Madhya Pradesh, Gujarat)
    {"station_id": "CPCB_NAR_01", "station_name": "Amarkantak - Narmada Kund", "river_name": "Narmada", "state": "Madhya Pradesh", "district": "Anuppur", "latitude": 22.6715, "longitude": 81.7589, "profile": "pristine"},
    {"station_id": "CPCB_NAR_02", "station_name": "Jabalpur - Bhedaghat", "river_name": "Narmada", "state": "Madhya Pradesh", "district": "Jabalpur", "latitude": 23.1310, "longitude": 79.8000, "profile": "moderate"},
    {"station_id": "CPCB_NAR_03", "station_name": "Hoshangabad - Sethani Ghat", "river_name": "Narmada", "state": "Madhya Pradesh", "district": "Narmadapuram", "latitude": 22.7533, "longitude": 77.7275, "profile": "moderate"},
    {"station_id": "CPCB_NAR_04", "station_name": "Bharuch - Golden Bridge", "river_name": "Narmada", "state": "Gujarat", "district": "Bharuch", "latitude": 21.7050, "longitude": 72.9980, "profile": "moderate"},

    # River Brahmaputra (Assam)
    {"station_id": "CPCB_BRA_01", "station_name": "Dibrugarh - Mohanaghat", "river_name": "Brahmaputra", "state": "Assam", "district": "Dibrugarh", "latitude": 27.4850, "longitude": 94.9120, "profile": "pristine"},
    {"station_id": "CPCB_BRA_02", "station_name": "Guwahati - Uzanbazar Ghat", "river_name": "Brahmaputra", "state": "Assam", "district": "Kamrup Metropolitan", "latitude": 26.1950, "longitude": 91.7550, "profile": "moderate"},
    {"station_id": "CPCB_BRA_03", "station_name": "Tezpur - Bhomoraguri", "river_name": "Brahmaputra", "state": "Assam", "district": "Sonitpur", "latitude": 26.6120, "longitude": 92.8550, "profile": "moderate"},

    # River Mahanadi (Odisha)
    {"station_id": "CPCB_MAH_01", "station_name": "Sambalpur - Hirakud Reservoir", "river_name": "Mahanadi", "state": "Odisha", "district": "Sambalpur", "latitude": 21.5230, "longitude": 83.8750, "profile": "pristine"},
    {"station_id": "CPCB_MAH_02", "station_name": "Cuttack - Mahanadi Barrage", "river_name": "Mahanadi", "state": "Odisha", "district": "Cuttack", "latitude": 20.4850, "longitude": 85.8750, "profile": "polluted"},

    # River Sutlej (Punjab)
    {"station_id": "CPCB_SUT_01", "station_name": "Ropar - Headworks", "river_name": "Sutlej", "state": "Punjab", "district": "Rupnagar", "latitude": 30.9650, "longitude": 76.5300, "profile": "moderate"},
    {"station_id": "CPCB_SUT_02", "station_name": "Harike Pattan - Confluence with Beas", "river_name": "Sutlej", "state": "Punjab", "district": "Tarn Taran", "latitude": 31.1550, "longitude": 74.9650, "profile": "critical"}
]

# Environmental science ranges based on CPCB standards
PROFILE_CONFIG = {
    "pristine": {
        "ph_mean": 7.4, "ph_std": 0.25,
        "do_mean": 8.2, "do_std": 0.5,
        "bod_mean": 1.4, "bod_std": 0.4,
        "temp_mean": 19.0, "temp_std": 3.0,
        "cond_mean": 180.0, "cond_std": 35.0,
        "nitrate_mean": 1.2, "nitrate_std": 0.5,
        "fc_mean": 120.0, "fc_std": 60.0,
        "turb_mean": 4.5, "turb_std": 2.0
    },
    "moderate": {
        "ph_mean": 7.6, "ph_std": 0.35,
        "do_mean": 6.1, "do_std": 0.7,
        "bod_mean": 3.2, "bod_std": 0.8,
        "temp_mean": 24.5, "temp_std": 3.5,
        "cond_mean": 420.0, "cond_std": 80.0,
        "nitrate_mean": 4.5, "nitrate_std": 1.8,
        "fc_mean": 1800.0, "fc_std": 600.0,
        "turb_mean": 14.0, "turb_std": 5.0
    },
    "polluted": {
        "ph_mean": 7.8, "ph_std": 0.55,
        "do_mean": 4.1, "do_std": 0.9,
        "bod_mean": 7.8, "bod_std": 2.2,
        "temp_mean": 27.0, "temp_std": 3.0,
        "cond_mean": 780.0, "cond_std": 150.0,
        "nitrate_mean": 12.5, "nitrate_std": 3.5,
        "fc_mean": 7500.0, "fc_std": 2500.0,
        "turb_mean": 32.0, "turb_std": 10.0
    },
    "critical": {
        "ph_mean": 8.4, "ph_std": 0.8,
        "do_mean": 1.8, "do_std": 0.8,
        "bod_mean": 18.5, "bod_std": 5.0,
        "temp_mean": 28.5, "temp_std": 2.8,
        "cond_mean": 1350.0, "cond_std": 300.0,
        "nitrate_mean": 22.0, "nitrate_std": 6.0,
        "fc_mean": 35000.0, "fc_std": 12000.0,
        "turb_mean": 68.0, "turb_std": 22.0
    }
}

def generate_water_quality_dataset(
    num_samples_per_station=40,
    start_date="2021-01-15",
    inject_anomalies=True,
    random_seed=42
):
    """
    Generates a realistic, multi-station, multi-year water quality dataset
    modeled on real CPCB NWMP monitoring stations across India.
    Includes intentional edge cases (missing values, duplicates, outliers)
    for demonstrating Big Data Analytics data-cleaning & quality-audit pipelines.
    """
    random.seed(random_seed)
    records = []
    base_date = datetime.strptime(start_date, "%Y-%m-%d")

    for station in MONITORING_STATIONS:
        prof = PROFILE_CONFIG[station["profile"]]
        
        # Monthly to bi-monthly sampling over ~3 years
        for i in range(num_samples_per_station):
            sample_date = base_date + timedelta(days=int(i * 28 + random.randint(-4, 4)))
            
            # Seasonal variation (Monsoon July-Sept has higher turbidity, summer has higher temp and lower DO)
            month = sample_date.month
            monsoon_mult = 1.6 if month in [7, 8, 9] else (0.8 if month in [12, 1, 2] else 1.0)
            summer_mult = 1.3 if month in [4, 5, 6] else 1.0

            # Generate parameters
            ph = round(random.gauss(prof["ph_mean"], prof["ph_std"]), 2)
            do_val = round(max(0.1, random.gauss(prof["do_mean"] / (1.1 if summer_mult > 1 else 1.0), prof["do_std"])), 2)
            bod = round(max(0.2, random.gauss(prof["bod_mean"] * (1.15 if summer_mult > 1 else 0.9), prof["bod_std"])), 2)
            temp = round(random.gauss(prof["temp_mean"] * (1.2 if summer_mult > 1 else (0.8 if month in [12, 1] else 1.0)), prof["temp_std"]), 1)
            cond = round(max(50.0, random.gauss(prof["cond_mean"], prof["cond_std"])), 1)
            nitrate = round(max(0.1, random.gauss(prof["nitrate_mean"], prof["nitrate_std"])), 2)
            fc = int(max(10, random.gauss(prof["fc_mean"] * (1.4 if monsoon_mult > 1 else 1.0), prof["fc_std"])))
            tc = int(fc * random.uniform(1.8, 3.2))
            turb = round(max(0.5, random.gauss(prof["turb_mean"] * monsoon_mult, prof["turb_std"])), 1)

            records.append({
                "station_id": station["station_id"],
                "station_name": station["station_name"],
                "river_name": station["river_name"],
                "state": station["state"],
                "district": station["district"],
                "latitude": station["latitude"],
                "longitude": station["longitude"],
                "sampling_date": sample_date.strftime("%Y-%m-%d"),
                "temperature_c": temp,
                "dissolved_oxygen_mg_l": do_val,
                "ph": ph,
                "conductivity_umhos_cm": cond,
                "bod_mg_l": bod,
                "nitrate_mg_l": nitrate,
                "fecal_coliform_mpn_100ml": fc,
                "total_coliform_mpn_100ml": tc,
                "turbidity_ntu": turb
            })

    df = pd.DataFrame(records)

    # Inject realistic data quality anomalies if requested
    if inject_anomalies:
        total_rows = len(df)
        
        # 1. Missing values (simulating sensor maintenance / recording errors)
        null_indices_ph = random.sample(range(total_rows), int(total_rows * 0.015))
        df.loc[null_indices_ph, "ph"] = None

        null_indices_do = random.sample(range(total_rows), int(total_rows * 0.02))
        df.loc[null_indices_do, "dissolved_oxygen_mg_l"] = None

        null_indices_bod = random.sample(range(total_rows), int(total_rows * 0.018))
        df.loc[null_indices_bod, "bod_mg_l"] = None

        null_indices_fc = random.sample(range(total_rows), int(total_rows * 0.025))
        df.loc[null_indices_fc, "fecal_coliform_mpn_100ml"] = None

        # 2. Duplicate records (simulating multiple upload runs)
        dup_indices = random.sample(range(total_rows), 15)
        duplicates = df.iloc[dup_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)

        # 3. Invalid out-of-range sensor spikes (negative values or physical impossibilities)
        err_indices = random.sample(range(total_rows), 6)
        df.loc[err_indices[0], "ph"] = 15.2  # pH > 14 impossible
        df.loc[err_indices[1], "ph"] = -1.5  # pH < 0 invalid
        df.loc[err_indices[2], "dissolved_oxygen_mg_l"] = -4.0  # negative DO
        df.loc[err_indices[3], "temperature_c"] = 99.0  # boiling river
        df.loc[err_indices[4], "bod_mg_l"] = -8.0  # negative BOD
        df.loc[err_indices[5], "turbidity_ntu"] = -12.0  # negative turbidity

    # Sort deterministically
    df = df.sort_values(by=["river_name", "station_name", "sampling_date"]).reset_index(drop=True)
    return df

if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "sample_water_quality.csv")
    
    df = generate_water_quality_dataset(num_samples_per_station=40, inject_anomalies=True)
    df.to_csv(out_path, index=False)
    print(f"Generated {len(df)} records across {len(MONITORING_STATIONS)} stations.")
    print(f"Saved to: {out_path}")
