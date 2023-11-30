# pyiris
Python3 기준 IRIS DB 연결 라이브러리

# Usage
### Basic
```
import pyiris

conn = pyiris.Connection(host="", port=5050, user="foo", password="bar", database="foo")
cursor = conn.cursor()
cursor.execute("SELECT 1 AS no FROM my_table")
data = cursor.fetchall()
print(data)
cursor.close()
conn.close()
```

### Use *with*
```
import pyiris

with pyiris.Connection(host="", port=5050, user="foo", password="bar", database="foo") as conn:
 	with conn.cursor() as cursor:
 		cursor.execute("SELECT 1 AS no FROM my_table")
		data = cursor.fetchall()
print(data)
```

### Get result *Dictionary Format*
```
import pyiris
from pyiris.cursor import DictCursor

conn = pyiris.Connection(host="", port=5050, user="foo", password="bar", database="foo", cursor_class_=DictCursor)
cursor = conn.cursor()
cursor.execute("SELECT 1 AS no FROM my_table")
data = cursor.fetchall()
print(data)
cursor.close()
conn.close()
```
