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
    .appName("SparkSQL_Zapytania") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()


#Wczytanie Parquet (Słownik pracowników)
df_pracownicy = spark.read.parquet("pracownicy.parquet")
df_pracownicy.createOrReplaceTempView("widok_pracownicy")

#Wczytanie CSV (Transakcje)
df_transactions = spark.read.csv("data.csv", header=True, inferSchema=True)
df_transactions.createOrReplaceTempView("widok_transakcje")

print("Zarejestrowano pomyślnie dwa widoki: 'widok_pracownicy' oraz 'widok_transakcje'.")


#Wykonanie zapytania SQL obejmującego: Agregacje, Grupowanie, WHERE i JOIN

sql_query = """
    SELECT 
        p.dzial,
        t.region,
        COUNT(t.kwota_transakcji) AS liczba_transakcji,
        SUM(t.kwota_transakcji) AS suma_transakcji,
        ROUND(AVG(t.kwota_transakcji), 2) AS srednia_transakcja
    FROM widok_transakcje t
    INNER JOIN widok_pracownicy p ON t.id_pracownika = p.id_pracownika
    WHERE t.kwota_transakcji > 300.0
    GROUP BY p.dzial, t.region
    ORDER BY suma_transakcji DESC
"""

#Wywołanie zapytania SQL
df_result = spark.sql(sql_query)

#Wykonanie dodatkowego zapytania SQL wyliczającego wartości MIN i MAX
min_max_sql_query = """
    SELECT 
        p.dzial,
        MIN(t.kwota_transakcji) AS minimalna_transakcja,
        MAX(t.kwota_transakcji) AS maksymalna_transakcja
    FROM widok_transakcje t
    INNER JOIN widok_pracownicy p ON t.id_pracownika = p.id_pracownika
    GROUP BY p.dzial
"""
df_min_max = spark.sql(min_max_sql_query)

#Wyświetlenie wyników i analiza
print("Wynik zaawansowanego zapytania Spark SQL")
df_result.show()

print("Wynik analizy wartości MIN i MAX dla działów")
df_min_max.show()

#Wyświetlenie podstawowych metryk statystycznych dla wyniku analizy
print("Metryki statystyczne zbioru wynikowego")
df_result.describe().show()

#Krótka analiza konsolowa struktury wynikowej
print("Struktura danych wynikowych")
df_result.printSchema()

#Wyświetlenie planu wykonania zapytania (explain)
print("Plan wykonania zapytania")
df_result.explain()

sciezka_zapisu = "raport_koncowy.csv"
df_result.toPandas().to_csv(sciezka_zapisu, index=False)

print(f"Wynik analizy został pomyślnie zapisany do pliku: '{sciezka_zapisu}'")

#Zamknięcie sesji Sparka
spark.stop()