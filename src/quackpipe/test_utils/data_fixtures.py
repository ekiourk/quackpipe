import datetime

import pandas as pd


# Helper functions for data generation
def create_employee_data():
    """Generate sample employee data."""
    return {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'department': ['Engineering', 'Marketing', 'Sales', 'Engineering', 'Marketing'],
        'salary': [75000, 65000, 55000, 80000, 70000]
    }


def create_monthly_data():
    """Generate sample monthly financial data."""
    return {
        'month': ['2024-01', '2024-02', '2024-03'],
        'revenue': [100000, 120000, 110000],
        'expenses': [80000, 90000, 85000]
    }


def create_vessel_definitions():
    """Define vessel specifications for AIS data generation."""
    return [
        {'mmsi': 123456789, 'name': 'CARGO_VESSEL_ALPHA', 'type': 70, 'length': 180, 'width': 28,
         'start_lat': 55.6761, 'start_lon': 12.5683},
        {'mmsi': 987654321, 'name': 'TANKER_BETA', 'type': 80, 'length': 250, 'width': 35, 'start_lat': 55.7000,
         'start_lon': 12.6000},
        {'mmsi': 456789123, 'name': 'FISHING_GAMMA', 'type': 30, 'length': 25, 'width': 8, 'start_lat': 55.6500,
         'start_lon': 12.5000},
        {'mmsi': 789123456, 'name': 'PILOT_DELTA', 'type': 50, 'length': 15, 'width': 5, 'start_lat': 55.6800,
         'start_lon': 12.5800},
        {'mmsi': 321654987, 'name': 'FERRY_EPSILON', 'type': 60, 'length': 120, 'width': 18, 'start_lat': 55.6900,
         'start_lon': 12.5900}
    ]


def generate_synthetic_ais_data(vessels=None, hours=24, interval_minutes=5, base_time=None):
    """
    Generate synthetic AIS data.

    Args:
        vessels: List of vessel definitions (uses default if None)
        hours: Number of hours to generate data for
        interval_minutes: Interval between data points in minutes
        base_time: Starting datetime (uses 2024-01-01 00:00:00 if None)

    Returns:
        pandas.DataFrame: Generated AIS data
    """
    if vessels is None:
        vessels = create_vessel_definitions()

    if base_time is None:
        base_time = datetime.datetime(2024, 1, 1, 0, 0, 0)

    synthetic_records = []

    # Generate data points
    for hour in range(hours):
        for minute in range(0, 60, interval_minutes):
            timestamp = base_time + datetime.timedelta(hours=hour, minutes=minute)

            for vessel in vessels:
                # Simulate vessel movement
                lat_offset = (hour * 0.001 + minute * 0.0001) * (1 if vessel['mmsi'] % 2 == 0 else -1)
                lon_offset = (hour * 0.0015 + minute * 0.00015) * (1 if vessel['mmsi'] % 3 == 0 else -1)

                record = {
                    'MMSI': vessel['mmsi'],
                    'BaseDateTime': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'LAT': vessel['start_lat'] + lat_offset,
                    'LON': vessel['start_lon'] + lon_offset,
                    'SOG': max(0, 10 + (hour % 20) - 10 + (minute % 10)),  # Speed varies
                    'COG': (hour * 15 + minute) % 360,  # Course changes
                    'Heading': (hour * 15 + minute + 5) % 360,
                    'VesselName': vessel['name'],
                    'VesselType': vessel['type'],
                    'Status': 0 if minute < 50 else 1,  # Mostly underway
                    'Length': vessel['length'],
                    'Width': vessel['width'],
                    'Draft': vessel['length'] / 20,  # Proportional draft
                    'Cargo': vessel['type']
                }
                synthetic_records.append(record)

    return pd.DataFrame(synthetic_records)


def create_ais_summary(ais_df, vessels):
    """Create a summary of AIS data."""
    vessel_type_counts = {}
    type_names = {70: 'cargo', 80: 'tanker', 30: 'fishing', 50: 'pilot', 60: 'ferry'}

    for vessel in vessels:
        type_name = type_names.get(vessel['type'], 'unknown')
        vessel_type_counts[type_name] = vessel_type_counts.get(type_name, 0) + 1

    return {
        'data_type': 'synthetic',
        'records_count': len(ais_df),
        'vessels_count': len(vessels),
        'time_range': '24 hours',
        'update_frequency': '5 minutes',
        'start_time': ais_df['BaseDateTime'].min(),
        'end_time': ais_df['BaseDateTime'].max(),
        'vessel_types': vessel_type_counts
    }
