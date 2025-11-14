import logging
import azure.functions as func
import random
import datetime
import os
import pyodbc
import json

app = func.FunctionApp()

# Function to randomly generate sensor readings.
def generate_sensor_readings():
    """
    Return a list of 20 random sensor readings as floats.
    """
    readings = []
    num_sensors = 20  # sensor count

    for sensor_id in range(1, num_sensors + 1):
        readings.append({
            "sensor_id": sensor_id,
            "temperature": random.uniform(5.0, 18.0),    # °C  (float)
            "wind":        random.uniform(12.0, 24.0),   # mph (float)
            "humidity":    random.uniform(30.0, 60.0),   # %   (float)
            "co2":         random.uniform(400.0, 1600.0) # ppm (float)
        })

    return readings

@app.route(route="generate", auth_level=func.AuthLevel.ANONYMOUS)
def generate_http(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP trigger called to generate sensor data.")

    # Get readings
    readings = generate_sensor_readings()
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc)

    connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]

    try:
        # Connect to Azure SQL database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Insert readings into database
        for r in readings:
            cursor.execute(
                """
                INSERT INTO SensorReadings
                    (SensorId, Temperature, WindSpeed, Humidity, CO2, ReadingTimeUtc)
                VALUES
                    (?, ?, ?, ?, ?, ?)
                """,
                r["sensor_id"],
                r["temperature"],
                r["wind"],
                r["humidity"],
                r["co2"],
                utc_timestamp
            )

        conn.commit()
        logging.info("Inserted %d rows into SensorReadings.", len(readings))

    except Exception as e:
        logging.error("Error inserting into SQL: %s", e)
        return func.HttpResponse(
            f"Error inserting into SQL: {e}",
            status_code=500
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # return JSON for endpoint
    response_json = {
    "inserted": len(readings),
    "timestamp": utc_timestamp.isoformat(),
    "readings": readings      # ← THIS IS THE NEW PART
    }

    return func.HttpResponse(
        json.dumps(response_json, indent=2),
        mimetype="application/json",
        status_code=200
    )


# Function to calculate staticstics (task 2)
@app.route(route="stats", auth_level=func.AuthLevel.ANONYMOUS)
def stats_http(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Connect to Azure SQL database
        conn = pyodbc.connect(os.environ["AZURE_SQL_CONNECTIONSTRING"])
        cur = conn.cursor()

        # Run SQL query to calculate min, max, avg for each sensor for each variable
        cur.execute("""
            SELECT 
                SensorId,
                MIN(Temperature),
                MAX(Temperature),
                AVG(Temperature),
                MIN(WindSpeed),
                MAX(WindSpeed),
                AVG(WindSpeed),
                MIN(Humidity),
                MAX(Humidity),
                AVG(Humidity),
                MIN(CO2),
                MAX(CO2),
                AVG(CO2)
            FROM SensorReadings
            GROUP BY SensorId
            ORDER BY SensorId
        """)
        rows = cur.fetchall()
        data = []
        # Format for output
        for r in rows:
            data.append({
                "sensor_id": r[0],
                "temperature_min": r[1],
                "temperature_max": r[2],
                "temperature_avg": float(r[3]),
                "wind_min": r[4],
                "wind_max": r[5],
                "wind_avg": float(r[6]),
                "humidity_min": r[7],
                "humidity_max": r[8],
                "humidity_avg": float(r[9]),
                "co2_min": r[10],
                "co2_max": r[11],
                "co2_avg": float(r[12]),
            })
        return func.HttpResponse(
            # Return JSON to stats endpoint
            json.dumps(data),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(str(e), status_code=500)

# Timer trigger set to every 10 seconds
@app.timer_trigger(schedule="*/10 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 

def generate_timer(myTimer: func.TimerRequest) -> None:
    logging.info("Timer trigger called to generate sensor data.")

    # Generates readings (every 10 seconds)
    readings = generate_sensor_readings()

    utc_timestamp = datetime.datetime.now(datetime.timezone.utc)

    logging.info(f"[{utc_timestamp.isoformat()}] Generated {len(readings)} sensor readings.")

    connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]

    try:
        # Connects to Azure SQL database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Inserts readings into database
        for r in readings:
            cursor.execute(
                """
                INSERT INTO SensorReadings
                    (SensorId, Temperature, WindSpeed, Humidity, CO2, ReadingTimeUtc)
                VALUES
                    (?, ?, ?, ?, ?, ?)
                """,
                r["sensor_id"],
                r["temperature"],
                r["wind"],
                r["humidity"],
                r["co2"],
                utc_timestamp
            )

        conn.commit()
        logging.info("Inserted %d rows (timer trigger)", len(readings))

    except Exception as e:
        logging.error("Timer trigger SQL error: %s", e)

    finally:
        try:
            conn.close()
        except:
            pass

# SQL trigger when changes in table detected
@app.sql_trigger(arg_name="changes",table_name="SensorReadings",
    connection_string_setting="AZURE_SQL_CONNECTIONSTRING_TRIGGER")
def stats_sql_trigger(changes: str) -> None:
    logging.info("SQL trigger fired for SensorReadings. Recomputing statistics...")

    try:
        conn = pyodbc.connect(os.environ["AZURE_SQL_CONNECTIONSTRING"])
        cur = conn.cursor()

        # Calculates stats (same as task 2)
        cur.execute("""
            SELECT 
                SensorId,
                MIN(Temperature) AS MinTemp,
                MAX(Temperature) AS MaxTemp,
                AVG(Temperature) AS AvgTemp,
                MIN(WindSpeed) AS MinWind,
                MAX(WindSpeed) AS MaxWind,
                AVG(WindSpeed) AS AvgWind,
                MIN(Humidity) AS MinHumidity,
                MAX(Humidity) AS MaxHumidity,
                AVG(Humidity) AS AvgHumidity,
                MIN(CO2) AS MinCO2,
                MAX(CO2) AS MaxCO2,
                AVG(CO2) AS AvgCO2
            FROM SensorReadings
            GROUP BY SensorId
            ORDER BY SensorId;
        """)

        rows = cur.fetchall()
        results = []

        # Format for output
        for r in rows:
            results.append({
                "sensor_id": r.SensorId,
                "temperature_min": r.MinTemp,
                "temperature_max": r.MaxTemp,
                "temperature_avg": float(r.AvgTemp),
                "wind_min": r.MinWind,
                "wind_max": r.MaxWind,
                "wind_avg": float(r.AvgWind),
                "humidity_min": r.MinHumidity,
                "humidity_max": r.MaxHumidity,
                "humidity_avg": float(r.AvgHumidity),
                "co2_min": r.MinCO2,
                "co2_max": r.MaxCO2,
                "co2_avg": float(r.AvgCO2),
            })

        # Output to terminal
        logging.info("Statistics (triggered automatically): %s", json.dumps(results))

    except Exception as e:
        logging.error("SQL trigger statistics error: %s", e)

    finally:
        try: conn.close()
        except: pass

