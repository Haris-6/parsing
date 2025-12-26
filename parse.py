from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta


# ---------------- SSH CONFIG ----------------
SSH_HOST = ""
SSH_PORT = ""
SSH_USER = ""
SSH_PASSWORD = ""

# ---------------- MONGO CONFIG ----------------
MONGO_USER = ""
MONGO_PASSWORD = urllib.parse.quote_plus("")
MONGO_DB = ""
MONGO_HOST = ""
MONGO_PORT = ""

# -------- INPUT DRIVER ID --------
DRIVER_ID = ObjectId("68cf142425b5590dd4e8a44a")
# DRIVER_ID=ObjectId("68496a5ff38dc2543d248051")

# -------- HELPER FUNCTION --------
def parse_event_date(event_date_str):
    """
    Converts 'MMDDYY' -> datetime
    """
    try:
        return datetime.strptime(event_date_str, "%m%d%y")
    except Exception:
        return None

# def safe_float(value, default=0):
#     try:
#         return float(value)
#     except (ValueError, TypeError):
#         return default

with SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_username=SSH_USER,
    ssh_password=SSH_PASSWORD,
    remote_bind_address=(MONGO_HOST, MONGO_PORT),
    local_bind_address=("localhost", 0),
) as tunnel:

    mongo_uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@localhost:{tunnel.local_bind_port}/{MONGO_DB}"
        f"?authSource=driverbookv2_stage"
    )

    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]

    # -------- DRIVER PROFILE --------
    driver = db.drivers.find_one(
        {"_id": DRIVER_ID},
        {"driverId": 1, "cycleRule": 1, "timeZone": 1}
    )

    # -------- TIMEZONE LOOKUP --------
    driver_timezone = None
    if driver and driver.get("timeZone"):
        tz_record = db.timezones.find_one(
            {"_id": driver["timeZone"]},
            {"tzCode": 1}
        )
        if tz_record:
            driver_timezone = tz_record.get("tzCode")

    # -------- ALL METAS --------
    metas_cursor = db.metas.find(
        {"driver": DRIVER_ID},
        projection={
            "clockData": 1,
            "voilations": 1,
            "ptiViolation": 1,
            "deviceCalculations": 1,
            "createdAt": 1,
            "lastActivity": 1
        }
    ).sort("createdAt", ASCENDING)

    records = []

    for meta in metas_cursor:
        clock = meta.get("clockData", {})
        violations = meta.get("voilations", [])
        pti_violations = meta.get("ptiViolation", [])
        device_calc = meta.get("deviceCalculations", {})
        data_date = meta.get("createdAt")
        last_act= meta.get("lastActivity",{})
        


        # -------- VIOLATIONS CALCULATION --------
        seven_days_ago = data_date - timedelta(days=7)

        violations_last7Days = 0
        violation_patterns = set()

        for v in violations:
            started_at = v.get("startedAt", {})
            event_date = parse_event_date(started_at.get("eventDate"))

            if event_date and event_date >= seven_days_ago:
                violations_last7Days += 1
                if v.get("type"):
                    violation_patterns.add(v["type"].lower())

        pti_violations_last7Days = 0
        pti_patterns = set()

        for pv in pti_violations:
            shift_start = pv.get("SHIFT_START_DATE", {})
            event_date = parse_event_date(shift_start.get("eventDate"))

            if event_date and event_date >= seven_days_ago:
                pti_violations_last7Days += 1
                if pv.get("type") is not None:
                    pti_patterns.add(str(pv.get("type")))

        # -------- FINAL RECORD --------
        
        record = {
            # Driver info
            "driver_id": driver.get("driverId") if driver else None,
            
            "cycleRule": driver.get("cycleRule") if driver else None,
            "timezone": driver_timezone,
            "splitShiftActive": clock.get("isSplitActive", False),

            # Time calculations
            "driveSeconds": device_calc.get("DRIVING", 0),
            "onDutySeconds": device_calc.get("ON_DUTY_CURRENT_TIME", 0),
            "cycleSeconds": device_calc.get("DRIVING_CYCLE", 0),
            "remainingDriveMinutes": clock.get("driveSeconds", 0) // 60,
            "remainingShiftMinutes": clock.get("shiftDutySecond", 0) // 60,
            "breakRemainingMinutes": clock.get("breakSeconds", 0) // 60,

            # Violations (STRUCTURED)
            
            "violation active": [len(violations)],
            "last7Days violation": violations_last7Days,
            "violation patterns": list(violation_patterns),
            
            
            "last7Days pti_violations": pti_violations_last7Days,
            "pti_violation patterns": list(pti_patterns),
            

            # Diagnostics
            "speed": last_act.get("speed")if last_act else None,
            "latitude": last_act.get("latitude")if last_act else None,
            "longitude": last_act.get("longitude") if last_act else None,
            "Address" :last_act.get("address") if last_act else None,
            # "moving": safe_float(last_act.get("speed"), 0) > 5 if last_act else None,

            # Meta info
            "dataDate": data_date
        }

        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna(0)

    df.to_csv("driver_metas_all_with_diagnostics_update.csv", index=False)
    print(f"Saved {len(df)} records to CSV")
