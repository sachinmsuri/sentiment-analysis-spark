from pyspark.sql import SparkSession
from preprocessing.Preprocessor import Preprocessor
from time import time


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("generate_cleaned_csv").getOrCreate()
    print(f'Using spark version {spark.version}')  # I'm using 3.0.2

    start = time()
    pp = Preprocessor(spark)
    df = pp.generate_cleaned_csv('/home/y4tsu/Documents/uci_ml/full_raw.csv')  # /home/y4tsu/Documents/uci_ml/full_raw.csv

    # Write the cleaned .csv to disk (110.6MB before -> 71.5MB after preprocessing & cleaning)
    df.write.csv('cleaned_dataset')

    # Took 622 seconds to complete this initial stage on the full dataset
    print(f'Time taken to preprocess and save cleaned .csv: {time() - start:.3f} seconds')
