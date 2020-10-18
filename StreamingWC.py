import sys
from lib.utils import get_spark_app_config
from lib.utils import read_from_stream ,wrirte_to_console_from_stream
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting the Streaming application")

    # Reading the lines as datafrom 
    
    lines_df = read_from_stream(spark) 
    words_df = lines_df.select(f.expr("explode(split(value,' ')) as words"))

    # Counting the words
    logger.info("Counting the words")
    counts_df = words_df.groupBy("words").count()

    # Writing the output to console
    word_query_count = wrirte_to_console_from_stream(counts_df)
        
    word_query_count.awaitTermination()
    
    logger.info("Completing the Streaming application")

