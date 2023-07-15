from pyspark import SparkContext, Row
from pyspark.sql import SparkSession

# from pyspark.streaming import streaming

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")

# ssc = streaming(sc, 1)

# Create Spark Session from the Spark Context
ss = SparkSession(sc)

# Subscribe to topic test
df = (
    ss.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("auto.offset.reset", "earliest")
    .load()
)


def handler(input_df, batch_id):
    print("BATCH ID: " + str(batch_id))

    def splitter(row):
        print("This is going to split each array element!")
        print(row)
        print(row.value.decode("utf-8"))
        print(type(row.value.decode("utf-8")))

        # .split(" ")
        return Row(
            key=row.key,
            value=row.value.decode("utf-8"),
            topic=row.topic,
            partition=row.partition,
            offset=row.offset,
            timestamp=row.timestamp,
            timestampType=row.timestampType,
        )

    print("Input after splitting!")
    # input_df.foreach(splitter)
    input_df.printSchema()

    # inputs_after_split.write.format("kafka").option(
    #     "kafka.bootstrap.servers", "localhost:9092"
    # ).option("failOnDataLoss", "false").option("topic", "testingOutput").save()


query = (
    df.writeStream.format("console")
    .foreachBatch(handler)
    .trigger(processingTime="10 seconds")
    .start()
    .awaitTermination()
)

# words = lines.flatMap(lambda line: line.split(" "))
# # Count each word in each batch
# pairs = words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# # Print the first ten elements of each RDD generated in this DStream to the console
# wordCounts.pprint()
# ssc.start()  # Start the computation
# ssc.awaitTermination()  # Wait for the computation to terminate
