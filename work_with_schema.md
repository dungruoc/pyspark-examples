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

# Manage data schema

So, as stated above, we would better have specified decoding schema for working with input json messages. The schema can be defined as Pyspark codes

```python
test_schema = StructType([
  StructField("test", ArrayType(StructType([
      StructField("field_a", StringType(), True),
      StructField("field_b", BooleanType(), True),
      StructField("field_c", LongType(), True)
    ]), True), True)
])
```

The codes can be managed with version control tools like Git. It's good till we have the requirement to implement the decoding schema for a big Json blob, thousands of fields. It's a nightmare now to define the above Pyspark structures and assure that it fits the provided specification. We would prefer now leaving the task to Spark to infer the schema. But as we noted above, the infering can make the job slow, and the schema could be different due to NULL values in the sample data.

## If we have a complete data sample? JsonSample-to-PySpark schema codes

So ideally, if we could have a complete data sample, with all required data fields and no NULL values? That's not too demanding. The business requirement should come with a data sample for eable us to test it.

With this sample, we can let Spark infer the schma from it. But then, we would need to freeze the schema, as if not, the infering on other real data can have mismatch.

For this purpose, I make here the scripts to printout the Pyspark codes defining the schema from a Pyspark type

```python
import pyspark.sql.functions as F
import pyspark.sql.types as T

def field_to_string(field, struct_path, indent = 2, hook_paths={}):
  struct_path = struct_path + [f'{field.name}']
  if tuple(struct_path) in hook_paths:
    type_string = hook_paths.get(tuple(struct_path))
  else:
    type_string = type_to_string(field.dataType, struct_path=struct_path, indent=indent, hook_paths=hook_paths)
  return f'StructField("{field.name}", {type_string}, {field.nullable})'

def type_to_string(data_type, struct_path, indent = 2, hook_paths = {}):
  if type(data_type) == T.StructType:
    spaces_i = ' ' * indent * (len(struct_path) + 1)
    spaces = ' ' * indent * (len(struct_path))
    field_strings = f',\n{spaces_i}'.join([ field_to_string(field=f, struct_path=struct_path, indent=indent, hook_paths=hook_paths) for f in data_type.fields ])
    return f'StructType([\n{spaces_i}{field_strings}\n{spaces}])'
  elif type(data_type) == T.ArrayType:
    if struct_path:
      struct_path[-1] = struct_path[-1]+'[]'
    if tuple(struct_path) in hook_paths:
      elem_str = hook_paths.get(tuple(struct_path))
    else:
      elem_str = type_to_string(data_type.elementType, struct_path=struct_path, indent=indent, hook_paths=hook_paths)
    return f'ArrayType({elem_str}, {data_type.containsNull})'
  else:
    name = data_type.__repr__()
    return f'{name}'
```

With these utils, we can print out the schema for PySpark

```python
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

print(type_to_string(test_schema, struct_path=[], indent = 2, hook_paths = {}))
```
Output
```
StructType([
  StructField("test", ArrayType(StructType([
    StructField("field_a", StringType(), True),
    StructField("field_b", BooleanType(), True),
    StructField("field_c", LongType(), True),
    StructField("field_d", StructType([
      StructField("nested_d", ArrayType(StructType([
        StructField("d_a", StringType(), True),
        StructField("d_b", BooleanType(), True),
        StructField("d_c", LongType(), True),
        StructField("d_d", DoubleType(), True)
      ]), True), True)
    ]), True)
  ]), True), True)
])
```
It's the same as the defined schema, what we expected. We can copy the output as PySpark codes.

Combining what we have, a complete Json sample, we need to let Spark to infer its schema, then print out the schema to be able to copy it to our PySpark codes. These are the needed steps.

```python
def json2struct(message, decoding_schema):
  json_list = []
  json_list.append(message)
  if decoding_schema:
    df = spark.read.json(sc.parallelize(json_list), schema=decoding_schema)
  else:
    df = spark.read.json(sc.parallelize(json_list))
  return df.select(F.struct(*df.columns).alias("json_struct"))

def pyspark_type(json_message, hook_paths):
  df = json2struct(json_message, decoding_schema=None)
  return type_to_string(df.schema['json_struct'].dataType, struct_path=list([]), indent=2, hook_paths=hook_paths)
```
So, with a json sample

```python
test_json = """
{
  "test": [{
    "field_a": "",
    "field_b": true,
    "field_c": 0,
    "field_d": {
      "nested_d":  [{
        "d_a": "",
        "d_b": true,
        "d_c": 0,
        "d_d": 0.1    
      }]
    }
  }]
}
"""

print(pyspark_type(test_json, hook_paths={}))
```
Output
```
StructType([
  StructField("test", ArrayType(StructType([
    StructField("field_a", StringType(), True),
    StructField("field_b", BooleanType(), True),
    StructField("field_c", LongType(), True),
    StructField("field_d", StructType([
      StructField("nested_d", ArrayType(StructType([
        StructField("d_a", StringType(), True),
        StructField("d_b", BooleanType(), True),
        StructField("d_c", LongType(), True),
        StructField("d_d", DoubleType(), True)
      ]), True), True)
    ]), True)
  ]), True), True)
])
```
It's what we needed above to copy to our PySpark codes.