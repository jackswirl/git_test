import pandas as pd

data = pd.read_csv('./test.csv', header = 1, skip_footer=1)

print data
