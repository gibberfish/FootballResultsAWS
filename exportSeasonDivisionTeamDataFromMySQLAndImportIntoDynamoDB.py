"""
{
  "partKey": "DV_2010",
  "sortKey": "3",
  "divisionName": "Premier League",
  "position": 1,
  "Teams": [
    { "teamId": 121, "externalId": 4312, "teamName": "Portsmouth" },
    { "teamId": 12, "externalId": 2123, "teamName": "Crewe" }
  ]
}
"""

import requests
from requests.auth import HTTPDigestAuth
import json
import boto3

def getJsonFrom(url):
    response = requests.get(url)
    if (response.ok):
        data = json.loads(response.content)
    return data

dynamodbclient=boto3.resource('dynamodb')
footballTable = dynamodbclient.Table('football')

outputData = {}

divisionMap = {}
teamMap = {}

url = 'http://data-api-local:1980/dataapi/division_mapping'
divisionMappings = getJsonFrom(url)
for divisionMapping in divisionMappings['data']:
    divisionMap[str(divisionMapping['attributes']['fraId'])] = str(divisionMapping['attributes']['sourceId'])

url = 'http://data-api-local:1980/dataapi/team_mapping'
teamMappings = getJsonFrom(url)
for teamMapping in teamMappings['data']:
    teamMap[str(teamMapping['attributes']['fraId'])] = str(teamMapping['attributes']['sourceId'])

url = 'http://data-api-local:1980/dataapi/seasons'
seasons = getJsonFrom(url)

for season in seasons['data']:
#for season in [2019]:
    seasonNumber = season['id']
    seasonDivisionsUrl = season['relationships']['seasonDivisions']['links']['related']

    seasonDivisions = getJsonFrom(seasonDivisionsUrl)    
    
    for seasonDivision in seasonDivisions['data']:
        
        divId = divisionMap.get (seasonDivision['attributes']['divisionId'])
        
        divPos = seasonDivision['attributes']['position']

        divisionUrl = seasonDivision['relationships']['division']['links']['related']

        division = getJsonFrom(divisionUrl)

        divName = division['data']['attributes']['divisionName']
    
        outputData['partKey'] = 'DV_' + str(seasonNumber)
        outputData['sortKey'] = divId
        outputData['divisionName'] = divName
        outputData['position'] = divPos
        outputData['teams'] = []

        seasonDivisionTeamsUrl = seasonDivision['relationships']['teams']['links']['related']
        seasonDivisionTeams = getJsonFrom(seasonDivisionTeamsUrl)

        for seasonDivisionTeam in seasonDivisionTeams['data']:
            originalTeamId = seasonDivisionTeam['attributes']['teamId']
            teamId = teamMap.get (originalTeamId)

            teamUrl = 'http://data-api-local:1980/dataapi/teams/' + originalTeamId
            team = getJsonFrom(teamUrl)

            teamName = team['data']['attributes']['teamName']

            outputData['teams'].append ({
                "teamId": teamId,
                "teamName": teamName
            })

        #fileName = 'DV_' + str(seasonNumber) + '_' + divId + '.json'
        #with open(fileName, 'w') as outfile:
        #    json.dump(outputData, outfile)

        response = footballTable.put_item(
            Item=outputData
        )
        print(response)
