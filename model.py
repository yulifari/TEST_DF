from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Common fields for all person types
person_schema = StructType([
    StructField("personne_id", StringType(), False),
    StructField("nom", StringType(), True),
    StructField("date_creation", TimestampType(), True),
    StructField("statut", StringType(), True)
])

personne_physique_schema = StructType([
    StructField("personne_id", StringType(), False),
    StructField("nom", StringType(), True),
    StructField("prenom", StringType(), True),
    StructField("date_naissance", TimestampType(), True),
    StructField("nationalite", StringType(), True)
])

personne_morale_schema = StructType([
    StructField("personne_id", StringType(), False),
    StructField("raison_sociale", StringType(), True),
    StructField("siren", StringType(), True),
    StructField("forme_juridique", StringType(), True)
])

adresse_schema = StructType([
    StructField("adresse_id", StringType(), False),
    StructField("personne_id", StringType(), False),
    StructField("rue", StringType(), True),
    StructField("ville", StringType(), True),
    StructField("code_postal", StringType(), True),
    StructField("pays", StringType(), True)
])

audit_schema = StructType([
    StructField("audit_id", StringType(), False),
    StructField("personne_id", StringType(), False),
    StructField("date_audit", TimestampType(), True),
    StructField("type_audit", StringType(), True),
    StructField("resultat", StringType(), True)
])

alerte_schema = StructType([
    StructField("alerte_id", StringType(), False),
    StructField("personne_id", StringType(), False),
    StructField("date_alerte", TimestampType(), True),
    StructField("type_alerte", StringType(), True),
    StructField("niveau_risque", StringType(), True),
    StructField("statut", StringType(), True)
])
