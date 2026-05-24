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
    .appName("GeneratorTransakcjiCSV") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

print("Generowanie zbioru danych CSV...")

#Przygotowanie danych transakcyjnych
transactions_data = [
    (1, "Elektronika", 3200.50, "Północ"),
    (2, "Moda", 450.00, "Południe"),
    (3, "Elektronika", 5400.00, "Zachód"),
    (4, "Dom i Ogród", 120.80, "Północ"),
    (5, "Sport", 890.00, "Wschód"),
    (6, "Dom i Ogród", 2300.10, "Zachód"),
    (7, "Moda", 710.00, "Południe"),
    (8, "Elektronika", 150.00, "Północ"),
    (9, "Sport", 3100.00, "Wschód"),
    (10, "Elektronika", 420.50, "Zachód"),
    (11, "Moda", 1650.00, "Północ"),
    (12, "Dom i Ogród", 95.00, "Południe"),
    (13, "Luksusowe", 12500.00, "Centrum"),
    (14, "Sport", 320.00, "Wschód"),
    (15, "Elektronika", 2150.00, "Zachód")
]

kolumny = ["id_pracownika", "kategoria", "kwota_transakcji", "region"]

#Utworzenie DataFrame
df_transactions = spark.createDataFrame(transactions_data, kolumny)

#Zapis do pliku csv
df_transactions.toPandas().to_csv("data.csv", index=False)

print("Plik 'data.csv' został poprawnie utworzony w katalogu projektu.")

#Zamknięcie sesji
spark.stop()