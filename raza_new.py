from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse

# ---------------- SSH CONFIG ----------------
SSH_HOST = "135.181.142.22"
SSH_PORT = 22
SSH_USER = "apiuser"
SSH_PASSWORD = "xDXylzjYRFnehPwx"

# ---------------- MONGO CONFIG ----------------
MONGO_USER = "haris"
MONGO_PASSWORD = urllib.parse.quote_plus("Y5WstKDw5yJ3sAFWZ3f0")
MONGO_DB = "driverbookv2_stage"
MONGO_HOST = "localhost"
MONGO_PORT = 27017

# -------- INPUT DRIVER ID --------
DRIVER_ID = ObjectId("68e4dc10b56bc4691e8be015")

with SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_username=SSH_USER,
    ssh_password=SSH_PASSWORD,
    remote_bind_address=(MONGO_HOST, MONGO_PORT),
    local_bind_address=("localhost", 0),
) as tunnel:

    mongo_uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@localhost:{tunnel.local_bind_port}/{MONGO_DB}" # type: ignore
        f"?authSource=driverbookv2_stage"
    )

    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]

    # -------- DRIVER PROFILE --------
    driver = db.drivers.find_one(
        {"_id": DRIVER_ID},
        {"cycleRule": 1, "timeZone": 1}
    )

    # -------- ALL METAS --------
    metas_cursor = db.metas.find(
        {"driver": DRIVER_ID},
        projection={"clockData": 1, "violation": 1, "vehicle": 1, "createdAt": 1}
    ).sort("createdAt", ASCENDING)  # sort oldest -> newest

    records = []

    from datetime import datetime, timedelta
    seven_days_ago = datetime.utcnow() - timedelta(days=7)

    for meta in metas_cursor:
        clock = meta.get("clockData", {})
        data_date = meta.get("createdAt")
        vehicle_id = meta.get("vehicle")

        # -------- GET DIAGNOSTIC FOR SAME DATE --------
        diagnostic = None
        if vehicle_id:
            diagnostic = db.driverdiagnostics.find_one(
                {"vehicleId": vehicle_id, "updatedAt": {"$lte": data_date}},
                sort=[("updatedAt", DESCENDING)],
                projection={"speed": 1, "latitude": 1, "longitude": 1, "enginePowerState": 1}
            )
        # -------- VIOLATIONS FROM trackingviolationevents --------
        violation_query = {
            "driverId": DRIVER_ID,
            "isDeleted": False
        }

        violation_events = list(
            db.trackingviolationevents.find(
                violation_query,
                {
                    "violationType": 1,
                    "isActive": 1,
                    "createdAt": 1
                }
            )
        )

        active_violations = [
            v for v in violation_events if v.get("isActive") is True
        ]

        last_7_days_violations = [
            v for v in violation_events
            if v.get("createdAt") and v.get("createdAt") >= seven_days_ago
        ]

        violation_patterns = list({
            v.get("violationType")
            for v in violation_events
            if v.get("violationType")
        })  

        record = {
            # Flatten driver info
            "driver_id": str(DRIVER_ID),
            "cycleRule": driver.get("cycleRule") if driver else None,
            "timezone": str(driver.get("timeZone")) if driver else None,
            "splitShiftActive": clock.get("isSplitActive", False),

            # Flatten clock data
            "driveSeconds": clock.get("driveSeconds", 0),
            "onDutySeconds": clock.get("shiftDutySecond", 0),
            "cycleSeconds": clock.get("cycleSeconds", 0),
            "remainingDriveMinutes": clock.get("driveSecondsSplit", 0) // 60,
            "remainingShiftMinutes": clock.get("shiftDutySecondsSplit", 0) // 60,
            "breakRemainingMinutes": clock.get("breakSeconds", 0) // 60,

            # Violations
            "violations_active": len(active_violations),
            "violations_last7Days": len(last_7_days_violations),
            "violations_patterns": violation_patterns,  # can fill if available

            # Live context from diagnostics
            "speed": diagnostic.get("speed", {}).get("value") if diagnostic else None,
            "latitude": diagnostic.get("latitude", {}).get("value") if diagnostic else None,
            "longitude": diagnostic.get("longitude", {}).get("value") if diagnostic else None,
            "moving": diagnostic.get("enginePowerState", {}).get("value") == "ENGINE_ON" if diagnostic else None,

            # History & ML risk
            "avgShiftLength": None,
            "avgTimeToViolation": None,
            "mlRiskScore": None,

            # Meta date
            "dataDate": data_date
        }

        records.append(record)

    # -------- CONVERT TO PANDAS DATAFRAME --------
    df = pd.DataFrame(records)
    df = pd.DataFrame(records)

# -------- REPLACE NULLS WITH 0 --------
    df = df.fillna(0)


    # -------- SAVE TO CSV --------
    df.to_csv("raza_new.csv", index=False)
    print(f"Saved {len(df)} records to CSV")
