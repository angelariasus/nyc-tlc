import os
import sys
from pymongo import MongoClient

# -- Configuration --------------------------------------------------------------
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
YEAR_TO_CLEAN = 2026

def clean_mongodb():
    print(f"\n?? Cleaning MongoDB Data for Year {YEAR_TO_CLEAN}...")
    try:
        client = MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USER,
            password=MONGO_PASSWORD,
            serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        
        # 1. Drop Gold Stream Database (Since it is 100% live data, we can drop the whole DB)
        print("  ? Dropping database tlc_gold_stream...")
        client.drop_database("tlc_gold_stream")
        print("    ? tlc_gold_stream dropped.")

        # 2. Delete 2026 records from Silver Database
        silver_db = client["tlc_silver"]
        collections = ["trips_yellow", "trips_green", "trips_fhv", "trips_hvfhv"]
        
        for coll in collections:
            result = silver_db[coll].delete_many({"_meta.source_year": YEAR_TO_CLEAN})
            print(f"  ? Deleted {result.deleted_count:,} records from tlc_silver.{coll}")

        # 3. Clean up quarantine audit logs for 2026
        audit_db = client["tlc_audit"]
        res_audit = audit_db["quarantine"].delete_many({"raw_record._meta.source_year": YEAR_TO_CLEAN})
        print(f"  ? Deleted {res_audit.deleted_count:,} quarantine records from tlc_audit.")
        
    except Exception as e:
        print(f"? Error cleaning MongoDB: {e}")

def clean_kafka_topics():
    print(f"\n?? Cleaning Kafka Topics for Year {YEAR_TO_CLEAN}...")
    try:
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "kafka-python", "-q"], check=True)
        
        from kafka.admin import KafkaAdminClient
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="tlc_cleanup"
        )
        
        topics_to_delete = [
            "tlc-yellow-2026", 
            "tlc-green-2026", 
            "tlc-fhv-2026", 
            "tlc-hvfhv-2026"
        ]
        
        existing_topics = admin_client.list_topics()
        topics_to_delete = [t for t in topics_to_delete if t in existing_topics]
        
        if topics_to_delete:
            print(f"  ? Deleting topics: {topics_to_delete}")
            admin_client.delete_topics(topics_to_delete)
            print("    ? Topics deleted successfully.")
        else:
            print("  ? No 2026 topics found in Kafka to delete.")
            
        admin_client.close()
    except Exception as e:
        print(f"? Error cleaning Kafka: {e}")

if __name__ == "__main__":
    print("=====================================================")
    print(f"   LAMBDA STREAMING CLEANUP UTILITY ({YEAR_TO_CLEAN})")
    print("=====================================================")
    confirm = input("This will permanently delete live data for 2026. Continue? (y/n): ")
    if confirm.lower() == "y":
        clean_mongodb()
        clean_kafka_topics()
        print("\n? Cleanup Complete! You are ready to start a fresh demo.")
    else:
        print("\nCleanup aborted.")

