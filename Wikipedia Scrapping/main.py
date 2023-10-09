import requests
from bs4 import BeautifulSoup
import pandas as pd

# Extract Process

url = "https://en.m.wikipedia.org/wiki/List_of_highest-grossing_films"

response = requests.get(url).text
content = BeautifulSoup(response, "html.parser")
L = content.find("table")
rows = L.find_all("tr")
data = {"Rank":[], "Title": [], "World Wide Collection": []}

for i in range(1, len(rows)):
    cells = rows[i].find_all("td")
    header = rows[i].find("th").get_text()
    data["Title"].append(header.replace("\n", ""))
    data["Rank"].append(cells[0].get_text().replace("\n", ""))
    data["World Wide Collection"].append(cells[2].get_text().replace("\n", ""))

#Transform
# Converting all the entries in rank to integer from string
for i in range(len(data["Rank"])):
    data["Rank"][i] = int(data["Rank"][i])

#Load
#loading data to a csv file
df = pd.DataFrame(data)
df.to_csv("MovieData.csv")
print("Data Scrapped and Loaded")