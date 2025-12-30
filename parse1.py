from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# -------- LOAD ENV VARIABLES --------
load_dotenv()

# ---------------- SSH CONFIG ----------------
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

# ---------------- MONGO CONFIG ----------------
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = urllib.parse.quote_plus(os.getenv("MONGO_PASSWORD"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))

# -------- INPUT DRIVER ID --------
DRIVER_ID = ObjectId("68e3ec982b51ef5f625ab138")

# -------- HELPER FUNCTIONS --------
def parse_event_date(event_date_str):
    try:
        return datetime.strptime(event_date_str, "%m%d%y")
    except Exception:
        return None

def end_of_day(dt):
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

def normalize_date(dt):
    if isinstance(dt, datetime):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return None

# -------- SSH TUNNEL --------
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

    # -------- TIMEZONE --------
    driver_timezone = None
    if driver and driver.get("timeZone"):
        tz = db.timezones.find_one(
            {"_id": driver["timeZone"]},
            {"tzCode": 1}
        )
        if tz:
            driver_timezone = tz.get("tzCode")

    # -------- METAS --------
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
    pti_violation_history = []

    for meta in metas_cursor:
        clock = meta.get("clockData", {})
        violations = meta.get("voilations", [])
        pti_violations = meta.get("ptiViolation", [])
        device_calc = meta.get("deviceCalculations", {})
        data_date = meta.get("createdAt")
        last_act = meta.get("lastActivity", {})

        # -------- HOS VIOLATIONS --------
        seven_days_ago = data_date - timedelta(days=7)
        violations_last7Days = 0
        violation_patterns =[]

        for v in violations:
            started_at = v.get("startedAt", {})
            event_date = parse_event_date(started_at.get("eventDate"))
            if event_date and event_date >= seven_days_ago:
                violations_last7Days += 1
                if v.get("type"):
                    violation_patterns.append(v["type"].lower())

        # -------- PTI VIOLATIONS (FIXED: Using Array Length by Date) --------
        # Save ptiViolation array for each date
        pti_violation_history.append({
            "date": data_date,
            "pti": pti_violations
        })

        # Calculate count and patterns from last 7 days
        pti_last7_count = 0
        pti_last7_patterns = []
        pti_cutoff = data_date - timedelta(days=7)

        for entry in pti_violation_history:
            if entry["date"] >= pti_cutoff:
                pti_last7_count += len(entry["pti"])
                for pv in entry["pti"]:
                    if pv.get("type") is not None:
                        pti_last7_patterns.append(str(pv["type"]))

        # -------- MINOR VIOLATIONS --------
        start_date = (data_date - timedelta(days=7)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = end_of_day(data_date)

        minor_violation_count = 0
        minor_violation_patterns = []

        tracking_cursor = db.trackingviolationevents.find(
            {
                "driverId": DRIVER_ID,
                "createdAt": {
                    "$gte": start_date,
                    "$lte": end_date
                },
                "isDeleted": False
            },
            {"violationType": 1}
        )

        for tv in tracking_cursor:
            minor_violation_count += 1
            if tv.get("violationType"):
                minor_violation_patterns.append(tv["violationType"])

        # -------- FINAL RECORD --------
        record = {
            "driver_id": driver.get("driverId") if driver else None,
            "driver_db_id": str(DRIVER_ID),
            "cycleRule": driver.get("cycleRule") if driver else None,
            "timezone": driver_timezone,
            "splitShiftActive": clock.get("isSplitActive", False),

            "driveSeconds": device_calc.get("DRIVING", 0),
            "onDutySeconds": device_calc.get("ON_DUTY_CURRENT_TIME", 0),
            "cycleSeconds": device_calc.get("DRIVING_CYCLE", 0),
            "remainingDriveMinutes": clock.get("driveSeconds", 0) // 60,
            "remainingShiftMinutes": clock.get("shiftDutySecond", 0) // 60,
            "breakRemainingMinutes": clock.get("breakSeconds", 0) // 60,

            "violation active": len(violations),
            "last7Days violation": violations_last7Days,
            "violation patterns": list(violation_patterns),

            "last7Days pti_violations": pti_last7_count,
            "pti_violation patterns": list(pti_last7_patterns),

            "last7_days_minior_violation": minor_violation_count,
            "minior_violation_pattern": list(minor_violation_patterns),

            "speed": last_act.get("speed"),
            "latitude": last_act.get("latitude"),
            "longitude": last_act.get("longitude"),
            "Address": last_act.get("address"),

            "dataDate": data_date
        }

        records.append(record)

    # -------- SAVE CSV --------
    df = pd.DataFrame(records).fillna(0)
    df.to_csv("driver_metas_all_with_diagnostics_update_final10.csv", index=False)

    print(f"Saved {len(df)} records to CSV")
