import sys

import requests


def http_get(url, auth=None, timeout=10):
    try:
        r = requests.get(url, auth=auth, timeout=timeout)
        return r.status_code, r.text
    except Exception as e:
        return None, str(e)


def try_airflow_auth(url, candidates):
    for user, password in candidates:
        status, text = http_get(url, auth=(user, password), timeout=10)
        if status == 200:
            return user, password, status, text
        if status is not None and status != 401:
            return user, password, status, text
    return None, None, None, None


def check_airflow():
    print("[AIRFLOW] Checking Airflow API...")
    candidates = [("admin", "admin"), ("airflow", "airflow"), ("admin", "airflow")]
    user, password, status, text = try_airflow_auth("http://localhost:8088/api/v1/dags", candidates)
    if status == 200:
        print("[PASS] Airflow API reachable")
        print(text[:1000])
        return True
    if status is None:
        print("[FAIL] Could not reach Airflow API")
        return False
    print(f"[FAIL] Airflow API returned {status}")
    print(text[:1000])
    return False


def check_nessie():
    print("\n[NESSIE] Checking Nessie API...")
    status, text = http_get("http://localhost:19120/api/v2/trees/main", timeout=10)
    if status == 200:
        print("[PASS] Nessie API reachable")
        print(text[:1000])
        return True
    print(f"[FAIL] Nessie API returned {status}: {text}")
    return False


def check_spark():
    print("\n[SPARK] Checking Spark Connect...")
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        print(f"[FAIL] PySpark not available: {e}")
        return False

    try:
        spark = SparkSession.builder.remote("sc://localhost:15002").appName("validate-pipeline-status").getOrCreate()
        print("[PASS] Spark Connect session created")
        try:
            tables = spark.sql("SHOW TABLES IN nessie.risk_analytics_ods").collect()
            print("Tables:", tables)
            try:
                count = spark.sql("SELECT COUNT(*) AS c FROM nessie.risk_analytics_ods.risk_metrics").collect()[0]["c"]
                print("risk_metrics count:", count)
            except Exception as e:
                print(f"[WARN] risk_metrics query failed: {e}")
                print("[INFO] Spark connection is up, but the table/query path still needs validation")
        except Exception as e:
            print(f"[WARN] Table listing failed: {e}")
            print("[INFO] Spark connection is up, but the Iceberg catalog view may still be unavailable")
        spark.stop()
        return True
    except Exception as e:
        print(f"[FAIL] Spark query failed: {e}")
        return False


def main():
    results = []
    results.append(("Airflow", check_airflow()))
    results.append(("Nessie", check_nessie()))
    results.append(("Spark", check_spark()))

    print("\nSUMMARY")
    print("-------")
    for name, ok in results:
        print(f"{name}: {'PASS' if ok else 'FAIL'}")

    if all(ok for _, ok in results):
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":
    main()
