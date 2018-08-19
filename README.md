# Running event type count

the spark program will stream events created in a directory and will print the running total count of event types.

## build

use the following command to build a jar with the dependencies in the target/scala-x folder

> sbt assembly

## Example

> spark submit --class Processor --master local[*] spark-json-streaming-assembly-0.1.jar
