import csv
import json


def dump_delimited(path, columns, rows, delimiter=','):
  count = 0
  with open(path, 'w') as f:
    writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
    for row in rows:
      writer.writerow(row)
      count += 1
  return count


def dump_json(path, columns, rows):
  count = 0
  with open(path, 'w') as f:
    for row in rows:
      count += 1
      d = dict(zip(columns, row))
      f.write(json.dumps(d) + '\n')
  return count
