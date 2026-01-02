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
DRIVER_ID = ObjectId("686b6410a2a58a0379e78689")

#-------- HELPER FUNCTIONS --------
def parse_event_date(event_date_str):
    try:
        return datetime.strptime(event_date_str, "%m%d%y")
    except Exception:
        return None


def end_of_day(dt):
    return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

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
        {"driverId": 1, "cycleRule": 1, "timeZone": 1, "fullName": 1,"tenantId":1}
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
            "lastActivity": 1,
            "vehicle" : 1
        }
    ).sort("createdAt", ASCENDING)

    records = []
    pti_violation_history = []
    hos_violation_history = []   

    for meta in metas_cursor:
        clock = meta.get("clockData", {})
        violations = meta.get("voilations", [])
        pti_violations = meta.get("ptiViolation", [])
        device_calc = meta.get("deviceCalculations", {})
        data_date = meta.get("createdAt")
        last_act = meta.get("lastActivity", {})
        vehicle = meta.get("vehicle")

        # ================= HOS VIOLATIONS (FIXED – ROLLING 7 DAYS) =================
        # hos_violation_history.append({
        #     "date": data_date,
        #     "violations": violations
        # })

        # violations_last7Days = 0
        # violation_patterns = []
        # hos_cutoff = data_date - timedelta(days=7)

        # for entry in hos_violation_history:
        #     if entry["date"] >= hos_cutoff:
        #         for v in entry["violations"]:
        #             violations_last7Days += 1
        #             if v.get("type"):
        #                 violation_patterns.append(v["type"].lower())

        # # ================= PTI VIOLATIONS (UNCHANGED – CORRECT) =================
        # pti_violation_history.append({
        #     "date": data_date,
        #     "pti": pti_violations
        # })

        # pti_last7_count = 0
        # pti_last7_patterns = []
        # pti_cutoff = data_date - timedelta(days=7)

        # for entry in pti_violation_history:
        #     if entry["date"] >= pti_cutoff:
        #         pti_last7_count += len(entry["pti"])
        #         for pv in entry["pti"]:
        #             if pv.get("type") is not None:
        #                 pti_last7_patterns.append(str(pv["type"]))

        # ================= HOS VIOLATIONS (PER-DAY FIXED) =================
        for v in violations:
            started_at = v.get("startedAt")

            if started_at and started_at.get("eventDate"):
                event_date = parse_event_date(started_at.get("eventDate"))

                # ❌ eventDate exists but does NOT match → SKIP
                if not event_date or event_date.date() != data_date.date():
                    continue

            # ✅ either no startedAt OR valid matching eventDate
            hos_violation_history.append({
            "date": data_date.date(),
            "type": v.get("type", "").lower()
            })


        # Calculate rolling last 7 days HOS totals
        violations_last7Days = 0
        violation_patterns = []
        cutoff = data_date.date() - timedelta(days=7)
        for entry in hos_violation_history:
            if cutoff < entry["date"] <= data_date.date():
                violations_last7Days += 1
                if entry["type"]:
                    violation_patterns.append(entry["type"])

        # ================= PTI VIOLATIONS (PER-DAY FIXED) =================
        for pv in pti_violations:
            started_at = pv.get("SHIFT_START_DATE")

            if started_at and started_at.get("eventDate"):
                event_date = parse_event_date(started_at.get("eventDate"))

                # ❌ eventDate exists but does NOT match → SKIP
                if not event_date or event_date.date() != data_date.date():
                    continue

            # ✅ either no startedAt OR valid matching eventDate
            pti_violation_history.append({
                "date": data_date.date(),
                "type": str(pv.get("type", ""))
            })

        # Calculate rolling last 7 days PTI totals
        pti_last7_count = 0
        pti_last7_patterns = []
        cutoff = data_date.date() - timedelta(days=7)
        for entry in pti_violation_history:
            if cutoff < entry["date"] <= data_date.date():
                pti_last7_count += 1
                if entry["type"]:
                    pti_last7_patterns.append(entry["type"])



        # ================= MINOR VIOLATIONS (UNCHANGED) =================
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


        # =================  DISTANCE  =================
        day_start = data_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = end_of_day(data_date)

        record_table = db.recordtables.find_one(
            {
                "driverId": DRIVER_ID,
                "createdAt": {"$gte": day_start, "$lte": day_end},
                "isDeleted": False
            },
            {"distance": 1,},
            sort=[("createdAt", -1)]
        )

        distance = record_table.get("distance", 0) if record_table else 0
        driver_name = record_table.get("driverName", 0) if record_table else 0

        # ================= FINAL RECORD =================
        record = {
            "driver_id": driver.get("driverId") if driver else None,
            "driver_db_id": str(DRIVER_ID),
            "driver_name": driver.get("fullName") if driver else None,
            "Tenant_id": driver.get("tenantId") if driver else None,
            "vehicle_id": str(vehicle) if vehicle else None,
            "cycleRule": driver.get("cycleRule") if driver else None,
            "timezone": driver_timezone,
            "splitShiftActive": clock.get("isSplitActive", False),
            "consective_driving": device_calc.get("CONSECUTIVE_DRIVING", False),
            "driveSeconds": device_calc.get("DRIVING", 0),
            "onDutySeconds": device_calc.get("ON_DUTY_CURRENT_TIME", 0),
            "cycleSeconds": device_calc.get("DRIVING_CYCLE", 0),
            "cycle_start_date": device_calc.get("CYCLE_START_DATE", {}).get("eventDate", ""),
            "Addition_driving_time": device_calc.get("DRIVING_ADDED", 0),
            "ON_DUTY_NOT_DRIVING_CYCLE": device_calc.get("ON_DUTY_NOT_DRIVING_CYCLE", 0),
            "off_dutySeconds": device_calc.get("OFF_DUTY", 0),
            "OFF_DUTY_CYCLE": device_calc.get("OFF_DUTY_CYCLE", 0),
            # "remainingDriveSecond": clock.get("driveSeconds", 0),
            # "remainingShiftSecond": clock.get("shiftDutySecond", 0),
            # "breakRemainingSeconds": clock.get("breakSeconds", 0),
            "Total_number_of_shift": device_calc.get("device_calc", 0),

            "violation active": len(violations),
            "last7Days violation": violations_last7Days,
            "violation patterns": violation_patterns,

            "last7Days pti_violations": pti_last7_count,
            "pti_violation patterns": pti_last7_patterns,

            "last7_days_minior_violation": minor_violation_count,
            "minior_violation_pattern": minor_violation_patterns,

            # "speed": last_act.get("speed"),
            "latitude": last_act.get("latitude"),
            "longitude": last_act.get("longitude"),
            "Address": last_act.get("address"),
            "distance": distance, 
            "dataDate": data_date.date() if isinstance(data_date, datetime) else data_date
        }

        records.append(record)

    # -------- SAVE CSV --------
    df = pd.DataFrame(records).replace("", 0).fillna(0)
    df.to_csv("driver_metas_all_with_diagnostics_update_final2.csv", index=False)

    print(f"Saved {len(df)} records to CSV")
