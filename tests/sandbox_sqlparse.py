import sqlparse

query = "SELECT * FROM table WHERE id = 1 AND name = 'test' GROUP BY id ORDER BY name DESC"
formatted = sqlparse.format(query, reindent=True, keyword_case='upper')

print("--- ORIGINAL ---")
print(query)
print("\n--- FORMATTED ---")
print(formatted)
print("\n--- REPR ---")
print(repr(formatted))
