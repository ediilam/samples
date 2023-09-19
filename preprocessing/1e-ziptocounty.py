import pandas as pd
import requests
import config

years = list(range(2007,2023))

# return a Pandas Dataframe of HUD USPS Crosswalk values
output = pd.DataFrame()
token = config.token
headers = {"Authorization": "Bearer {0}".format(token)}
for year in years:
# Note that type is set to 1 which will return values for the ZIP to Tract file and query is set to VA which will return Zip Codes in Virginia
	url = f"https://www.huduser.gov/hudapi/public/usps?type=2&query=All&year={year}"

	response = requests.get(url, headers = headers)

	if response.status_code != 200:
		print ("Failure, see status code: {0}".format(response.status_code))
	else: 
		df = pd.DataFrame(response.json()["data"]["results"])	
		df['year'] = year
		output = pd.concat([output,df])


print(output.shape)
output.to_csv('yearzips_to_county_ratios.csv')
