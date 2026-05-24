import os
import sys
from pyspark.sql import SparkSession

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("SparkSQL_CSV") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

#Wczytanie danych z pliku CSV z nagłówkiem i automatycznym wykrywaniem typów
csv_path = "data.csv"

df_csv = spark.read.csv(
    csv_path,
    header=True,
    inferSchema=True
)

#Rejestracja DataFrame jako widok tymczasowy (Temporary View)
df_csv.createOrReplaceTempView("widok_transakcje")
print(f"\nZarejestrowano widok tabelaryczny: 'widok_transakcje'")

#Wykonanie prostego zapytania SQL i wyświetlenie wyniku
print("\nWynik zapytania SQL (SELECT * FROM widok_transakcje LIMIT 10)")
sql_result = spark.sql("SELECT * FROM widok_transakcje LIMIT 10")
sql_result.show()

#Zamknięcie sesji Sparka
spark.stop()