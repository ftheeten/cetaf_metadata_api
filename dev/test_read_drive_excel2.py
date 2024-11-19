import pygsheets
import pandas as pnd

print("test")

gs_auth_file="xxxx.json"
key_test="xxx"

client = pygsheets.authorize(service_file=gs_auth_file)
sh = client.open_by_url("https://docs.google.com/spreadsheets/d/xxxxxx/edit")
wks=sh.worksheet('index',0)
df = wks.get_as_df()
print(df.columns.to_series())