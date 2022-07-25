We used to work with Json schema in Spark ETL job. We can use

```python
spark.read.json(sc.parallelize(json_list), schema=decoding_schema)
```
with "decoding_shema = None" to let Spark infer the schema for input messages. Howerver, we have 2 drawbacks with this option

1. The decoding is slower than if we have a decoding schema
2. When there are elements with NULL value, the decoding infers them as StringType by default. This can lead to mismatch, eg. the real type of some element is not string,
   and as if this schema is written into the table, it will cause conflict when new data come with the real types of those elements.
 
It's why we would better manage the decoding schema for incoming Json messages.
 
```python
import pyspark.sql.functions as F
import pyspark.sql.types as T


test_schema = T.StructType([
  T.StructField("test", T.ArrayType(T.StructType([
      T.StructField("field_a", T.StringType(), True),
      T.StructField("field_b", T.BooleanType(), True),
      T.StructField("field_c", T.LongType(), True),
      T.StructField("field_d", T.StructType([
        T.StructField("nested_d", T.ArrayType(T.StructType([
            T.StructField("d_a", T.StringType(), True),
            T.StructField("d_b", T.BooleanType(), True),
            T.StructField("d_c", T.LongType(), True),
            T.StructField("d_d", T.DoubleType(), True)
          ]), True), True)
      ]), True)
    ]), True), True)
])


spark.read.json(json_rdd, schema=test_schema)
```

# UnitTesting the decoding

So the first need is to able to test the decoding schema. Suppose we have the input Json sample and the decoding schema. The purpose is be able to test if the decoding works.

```python
import json

def json2struct(message, decoding_schema):
  json_list = []
  json_list.append(message)
  if decoding_schema:
    df = spark.read.json(sc.parallelize(json_list), schema=decoding_schema)
  else:
    df = spark.read.json(sc.parallelize(json_list))
  return df.select(F.struct(*df.columns).alias("json_struct"))
 
def str_diff(a, b):
  for i in range(len(a)):
    if a[i] != b[i]:
      return a[i:i+100], b[i:i+100]
  return None

def test_decoding(message, decoding_schema):
  df = json2struct(message, decoding_schema)
  string1 = json.dumps(json.loads(message), sort_keys=True, separators=(',', ':')) #.replace(' ', '')
  string2 = (df.select(F.to_json("json_struct").alias("json_struct")).collect()[0]['json_struct'])
  string2 = json.dumps(json.loads(string2), sort_keys=True, separators=(',', ':'))
  diff = str_diff(string1, string2)
  if diff is not None:
    print('diff:', diff)
    raise Exception('Diff')

  display(df
        .select(F.to_json(struct(*df.columns)).alias("json_struct"))
       )
```

The idea is compare the final Json string after decording-serializing with to original one. We the above utils functions, we can test with

```python
test_json = """
{
  "test": [{
    "field_a": "",
    "field_b": true,
    "field_c": 0
  }]
}
"""

from pyspark.sql.types import (StringType, StructField, StructType, ArrayType,
                               LongType, DoubleType, BooleanType)

test_schema = StructType([
  StructField("test", ArrayType(StructType([
      StructField("field_a", StringType(), True),
      StructField("field_c", LongType(), True)
    ]), True), True)
])

test_decoding(test_json, test_schema)
```
This would fail as the decoding has a missing element ***field_b*. The output is
```
test_json: {"test":[{"field_a":"","field_c":0}]}
diff: ('b":true,"field_c":0}]}', 'c":0}]}')
```
If we correct the decoding schema, it's fine now, and the function output the decoded dataframe

```python
test_schema = StructType([
  StructField("test", ArrayType(StructType([
      StructField("field_a", StringType(), True),
      StructField("field_b", BooleanType(), True),
      StructField("field_c", LongType(), True)
    ]), True), True)
])

test_decoding(test_json, test_schema)
```
Output:
```
json_struct
1 {"json_struct":{"test":[{"field_a":"","field_b":true,"field_c":0}]}}
```
