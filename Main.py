from pyspark.sql import SparkSession
from config.database_config import create_spark_session
from transformers.person_transformer import PersonTransformer
from transformers.audit_transformer import AuditTransformer
from transformers.alert_transformer import AlertTransformer

def run_pipeline():
    # Initialize Spark session
    spark = create_spark_session()
    
    # Read source tables
    personne_df = spark.table("ta_infos_personne")
    personne_physique_df = spark.table("ta_infos_personne_physique")
    personne_morale_df = spark.table("ta_infos_personne_morale")
    adresse_df = spark.table("ta_adresse")
    audit_df = spark.table("ta_audit")
    kyc_df = spark.table("ta_evaluation_kyc")
    alerte_df = spark.table("ta_alerte")
    status_alerte_df = spark.table("ta_status_alerte")
    
    # Create Person Hubs
    person_base_hub = PersonTransformer.create_person_base_hub(personne_df)
    person_physical_hub = PersonTransformer.create_person_physical_hub(
        personne_df,
        personne_physique_df
    )
    person_moral_hub = PersonTransformer.create_person_moral_hub(
        personne_df,
        personne_morale_df
    )
    person_address_hub = PersonTransformer.create_person_address_hub(
        personne_df,
        adresse_df
    )
    
    # Create Audit Hub
    audit_hub = AuditTransformer.create_audit_hub(
        audit_df,
        kyc_df
    )
    
    # Create Alert Hub
    alert_hub = AlertTransformer.create_alert_hub(
        alerte_df,
        status_alerte_df
    )
    
    # Save all hubs
    person_base_hub.write.mode("overwrite").saveAsTable("hub_person_base")
    person_physical_hub.write.mode("overwrite").saveAsTable("hub_person_physical")
    person_moral_hub.write.mode("overwrite").saveAsTable("hub_person_moral")
    person_address_hub.write.mode("overwrite").saveAsTable("hub_person_address")
    audit_hub.write.mode("overwrite").saveAsTable("hub_audit")
    alert_hub.write.mode("overwrite").saveAsTable("hub_alert")

if __name__ == "__main__":
    run_pipeline()
