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

## Too big schema? Split the schema

It's good now to have a flow: json sample -> PySpark schema codes. However, when the schema is too big, we can hardly print the output. We can of course dump it into the storage and open it as file, but it's not handy.

It came up to me the idea to split the schema into children. For this, I made some utils functions to first split the Json structure.

```python
def get_key_elem(root, json_key):
  if json_key[-2:] == '[]':
    return root[json_key[:-2]][0]
  return root[json_key]

def get_element(root, json_keys):
  if len(json_keys) == 1:
    return get_key_elem(root, json_keys[0])
  return get_element(get_key_elem(root, json_keys[0]), json_keys[1:])

def get_element_by_path(root, json_path):
  return get_element(root, json_path.split('.'))

def patch_element(root, json_keys):
  def patch_key_elem(root, json_key):
    if json_key[-2:] == '[]':
      root[json_key[:-2]][0] = None
    else:
      root[json_key] = None
    return root
  
  if len(json_keys) == 1:
    return patch_key_elem(root, json_keys[0])
  return patch_element(get_key_elem(root, json_keys[0]), json_keys[1:])
```
These help to get out children elements with Json paths and replace them with None value. With these, we can make a utils to hook, generate schemas for patched json structures.

```python
def gen_splitted_schemas(json_struct, splitted_paths, root_name='__root__'):
  schemas = {}
  for sch in sorted(splitted_paths, key=lambda x: 1.0/len(x[0].split('.'))):
    nested = get_element_by_path(json_struct, sch[0])
    patch_element(json_struct, sch[0].split('.'))
    child_paths = [p for p in filter(lambda x: x[0].startswith(sch[0]), splitted_paths)]
    hook_paths = {tuple(struct_path[0][len(sch[0])+1:].split('.')): struct_path[1] for struct_path in splitted_paths}
    schemas[sch[1]] = pyspark_type(json.dumps(nested), hook_paths=hook_paths)

  hook_paths = {tuple(struct_path[0].split('.')): struct_path[1] for struct_path in splitted_paths}
  schemas[root_name] = pyspark_type(json.dumps(json_struct), hook_paths)
  return schemas
```

Now, we can try to spit the above struct:

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
test_schemas = gen_splitted_schemas(json.loads(test_json), splitted_paths=[
  ('test[].field_d.nested_d[]', 'schema1'),
  ('test[].field_d', 'schema2')
], root_name='test_schema')

for schm in test_schemas:
  print(f'{schm} = {test_schemas[schm]}')
```
Output:
```
schema1 = StructType([
  StructField("d_a", StringType(), True),
  StructField("d_b", BooleanType(), True),
  StructField("d_c", LongType(), True),
  StructField("d_d", DoubleType(), True)
])
schema2 = StructType([
  StructField("nested_d", ArrayType(schema1, True), True)
])
test_schema = StructType([
  StructField("test", ArrayType(StructType([
    StructField("field_a", StringType(), True),
    StructField("field_b", BooleanType(), True),
    StructField("field_c", LongType(), True),
    StructField("field_d", schema2, True)
  ]), True), True)
])
```

## Manage Schema as data dictionary

So, with the above tools, we could easily create/test PySpark decoding schema if we have a complete Json data sample. We can make another step to make it more business-liked, rather than technical-liked.

For business staffs, it's not usual to manage data definition as a Json message, like the above

```json
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
```
and even bigger Json messages. It's easier for them to manage data schema as data dictionary. So, for data types of element data fields in the Json messages. For the above complete Json sample, it's comprehensive for them to define as a data dictionary of Json paths to data types like
```
'test[].field_a': 'string',
'test[].field_b': 'boolean',
'test[].field_c': 'integer',
'test[].field_d.nested_d[].d_a': 'string',
'test[].field_d.nested_d[].d_b': 'boolean',
'test[].field_d.nested_d[].d_c': 'integer',
'test[].field_d.nested_d[].d_d': 'double'
```

To provide this facility, we would make a utils too just generate the Json sample from the data dictionary

```python
import json

def bad_key(key):
  bad_chars = " ,;{}()\n\t="
  for char in bad_chars:
    if key.find(char) >= 0:
      return True
  return False  

def add_element(root, json_key, value):
  is_array = False
  if json_key[-2:] == '[]':
    is_array = True
    json_key = json_key[:-2]
  
  if bad_key(json_key):
    print('bad key', json_key)
    raise ""
  
  if not json_key in root:
    if is_array:
      root[json_key] = [value]
    else:
      root[json_key] = value
  if is_array:
    return root, root[json_key][0]
  return root, root[json_key]

def enum_types():
  return set([
    'ENUM_0', 'ENUM_1'
  ])

def standard_type(data_type):
  if data_type == 'decimal' or data_type == 'double':
    return 'double'
  elif data_type[:4] == 'bool':
    return 'boolean'
  elif data_type[:3] == 'int' or data_type == 'long' or data_type in enum_types():
    return 'long'
  elif data_type == 'string':
    return 'string'
  print('unknown', data_type)
  raise ""

def make_standard(schema):
  return {json_path: standard_type(schema[json_path].lower()) for json_path in schema}

def get_val_for_type(data_type):
  if data_type is None:
    return {}
  elif data_type == 'double':
    return 0.1
  elif data_type == 'boolean':
    return True
  elif data_type == 'long':
    return 0
  elif data_type == 'string':
    return ''
  print('unknown', data_type)
  raise ""

def add_deep_element(root, json_path, data_type):
  json_keys = json_path.split(".")
  child = root
  for key in json_keys[:-1]:
    _, child = add_element(child, key, get_val_for_type(None))
  _, child = add_element(child, json_keys[-1], get_val_for_type(data_type.lower()))
  return root, child

def create_sample(schema):
  element = {}
  schema = make_standard(schema)
  for json_path in schema:
    element, _ = add_deep_element(element, json_path, schema[json_path])
  return element

```

with these utils, we can generate the complete Json sample

```python
test_dict = {
  'test[].field_a': 'string',
  'test[].field_b': 'boolean',
  'test[].field_c': 'integer',
  'test[].field_d.nested_d[].d_a': 'string',
  'test[].field_d.nested_d[].d_b': 'boolean',
  'test[].field_d.nested_d[].d_c': 'integer',
  'test[].field_d.nested_d[].d_d': 'double'
}
print(json.dumps(create_sample(test_dict)))
```
Output
```json
{
  "test": [
    {
      "field_a": "",
      "field_b": true,
      "field_c": 0,
      "field_d": {
        "nested_d": [
          {
            "d_a": "",
            "d_b": true,
            "d_c": 0,
            "d_d": 0.1
          }
        ]
      }
    }
  ]
}
```
Great, two definitions are the same for us to generate the PySpark type as before.

Moreover, the definition dictionary can be managed as a CSV file, with 1 column for Json Paths, and one column for the data type. We can just get the dictionary from the CSV sheet

```python
def schema_sheet2dict(df, json_path_col, type_col):
  schema_types = df.select(F.col(json_path_col).alias('JsonPath'), F.col(type_col).alias('DataType')).where('JsonPath is not Null')
  pd_types = schema_types.toPandas()
  return dict(zip(pd_types.JsonPath, pd_types.DataType))
```

So, we could help to manage the data schema with CSV sheet. The helpers could help to generate the PySpark schema from it:

```
Definition Sheet -> Dictionary -> Json sample -> PySpark types
```

## More UnitTesting

We can enable more UnitTesting and schema validation with the above chain.
We add here a helper for re-generating the data dictionary from PySpark type

```python
def type2dict(data_type):
  def get_simple_type(data_type):
    return data_type[:-6].lower()

  def type_to_dict(data_type, current_path):

    if type(data_type) == T.StructType:
      children = [type_to_dict(f.dataType, f'{current_path}.{f.name}') for f in data_type.fields]
      ret = {}
      for child in children:
        for e in child:
          ret[e] = child[e]
      return ret
    elif type(data_type) == T.ArrayType:
      children = type_to_dict(data_type.elementType, f'{current_path}[]')
      return children
    else:
      return {current_path: get_simple_type(data_type.__repr__())}
  
  type_dict = type_to_dict(data_type, '')
  return {e[1:]: type_dict[e] for e in type_dict}

```

This helper execution

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

print(type2dict(test_schema))
```
ouputs
```
{
  'test[].field_a': 'string',
  'test[].field_b': 'boolean',
  'test[].field_c': 'long',
  'test[].field_d.nested_d[].d_a': 'string',
  'test[].field_d.nested_d[].d_b': 'boolean',
  'test[].field_d.nested_d[].d_c': 'long',
  'test[].field_d.nested_d[].d_d': 'double'
}
```

Well now, we can make a round: data dictionary -> PySpark type -> data dictionary to test the schema.

Moreover, the data dictionary can help us to track the evolution of the schema. For example, if we have an existing schema definition. The business guys come with a new version, adding more data fiedls to the schema. We can make
```
old schema -> old data dictionary
new dictionary -> new schema -> new data dictionary
```
and compary the 2 data dictionary to track what are the added/changed/deleted data fields.

```python
def schema_dict_changes(old_dict, new_dict):
  deleted = []
  added = []
  changed = []
  for elem in old_dict:
    if not elem in new_dict:
      deleted.append((elem, old_dict[elem]))
    elif old_dict[elem] != new_dict[elem]:
      changed.append((elem, (old_dict[elem], new_dict[elem])))

  for elem in new_dict:
    if not elem in old_dict:
      added.append((elem, new_dict[elem]))
  
  return deleted, changed, added
```