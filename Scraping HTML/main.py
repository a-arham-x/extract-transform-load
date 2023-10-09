import requests
from bs4 import BeautifulSoup
import pandas as pd

"""
Extracting Data from the index.html page into a CSV file
Host the index.html before running the code
"""

#Extracting Data
url = "http://127.0.0.1:5500/work_with_bs4.html"

response = requests.get(url).text
doc = BeautifulSoup(response, "html.parser")
data_dict = {}
th = doc.find_all("th")

heads = []
for i in th:
    data_dict[i.get_text()] = []
    heads.append(i.get_text())

list_tr = doc.find_all("tr")


for i in list_tr:
    j = i.find_all("td")
    data_dict[heads[0]].append(j[0].get_text())
    data_dict[heads[1]].append(j[1].get_text())


#Loading Data to a Dataframe and CSV
df = pd.DataFrame(data_dict)

df.to_csv("HTML_DATA.csv")

print("Data successfully transmitted to csv file")
