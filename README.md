$SPARK_HOME/bin/spark-submit \
--py-files packages.zip \
--files configs/config.json \
--files configs/schema.json jobs/pyspark_job.py