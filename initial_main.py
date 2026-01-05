from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime
import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

# ================= SSH CONFIG =================
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

# ================= MONGO CONFIG =================
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = urllib.parse.quote_plus(os.getenv("MONGO_PASSWORD"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))

# ================= INPUT VEHICLE ID =================
VEHICLE_ID = ObjectId("68cf155c25b5590dd4e8a479")

# ================= HELPER FUNCTION =================
def get_field(doc, key):
    if key not in doc:
        return "not_avail"
    
    value = doc[key]
    
    if value == "":
        return 0
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return value

# ================= SSH TUNNEL =================
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

    print("Connected to MongoDB")

    cursor = db.driverlocations.find(
        {
            "vehicleId": VEHICLE_ID,
            "isDeleted": False
        }
    ).sort("createdAt", ASCENDING)

    records = []

    #  ADD: track last saved timestamp
    last_saved_timestamp = None

    for doc in cursor:
        current_ts = get_field(doc, "timeStamp")

        # skip invalid timestamps
        if current_ts in ["not_avail", 0]:
            continue

        # first record always saved
        if last_saved_timestamp is None:
            last_saved_timestamp = current_ts
        else:
            # skip if within 60 seconds
            if current_ts < last_saved_timestamp + 60:
                continue
            last_saved_timestamp = current_ts

        record = {
            "vehicleId": str(get_field(doc, "vehicleId")),
            "tenantId": str(get_field(doc, "tenantId")),
            "driverId": str(get_field(doc, "driverId")),

            "timeStamp": current_ts,
            "engineParamsTimestamp": get_field(doc, "engineParamsTimestamp"),

            "speed": get_field(doc, "speed"),
            "moving": get_field(doc, "moving"),
            "direction": get_field(doc, "direction"),

            "engineState": get_field(doc, "engineState"),
            "load_pct": get_field(doc, "load_pct"),
            "latitude": get_field(doc, "latitude"),
            "longitude": get_field(doc, "longitude"),
            "address": get_field(doc, "address"),

            "odometer": get_field(doc, "odometer"),
            "engineHours": get_field(doc, "engineHours"),
            "tripDistance": get_field(doc, "tripDistance"),
            "tripHours": get_field(doc, "tripHours"),
            "voltage": get_field(doc, "voltage"),

            "engineCoolantTemp": get_field(doc, "engineCoolantTemp"),
            "coolantTemperature": get_field(doc, "coolantTemperature"),
            "engineCoolantLevel": get_field(doc, "engineCoolantLevel"),
            "coolantLevel": get_field(doc, "coolantLevel"),

            "oilTemprature": get_field(doc, "oilTemprature"),
            "engineOilTemp": get_field(doc, "engineOilTemp"),
            "engineOilTemperature": get_field(doc, "engineOilTemperature"),
            "oilPressure": get_field(doc, "oilPressure"),
            "engineOilLevel": get_field(doc, "engineOilLevel"),
            "oilLevel": get_field(doc, "oilLevel"),

            "turboBoost": get_field(doc, "turboBoost"),
            "intakePressure": get_field(doc, "intakePressure"),
            "intakeTemp": get_field(doc, "intakeTemp"),
            "chargeCoolerTemp": get_field(doc, "chargeCoolerTemp"),
            "turboRpm": get_field(doc, "turboRpm"),
            "crankCasePressure": get_field(doc, "crankCasePressure"),

            "createdAt": get_field(doc, "createdAt")
        }

        records.append(record)

    df = pd.DataFrame(records)

    output_file = f"initial_state_{str(VEHICLE_ID)}.csv"
    df.to_csv(output_file, index=False)

    print(f"Saved {len(df)} records to {output_file}")
