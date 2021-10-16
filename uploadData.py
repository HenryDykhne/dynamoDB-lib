import csv
from dynamoLib import DynamoLib

def csv_to_dict(csvFileName, keyString):
    csvDict = {}
    with open(csvFileName, 'r', encoding='utf-8-sig') as data:
        reader = csv.DictReader(data)
        for line in reader:
            csvDict[line[keyString]] = line
    return csvDict

def incomplete_headers_csv_to_dict(csvFileName, keyString, lastHeader):
    csvDict = {}
    with open(csvFileName, 'r', encoding='utf-8-sig') as data:
        reader = csv.reader(data)
        headers = next(reader, None)
        for line in reader:
            lineDict = {}
            for i, cell in enumerate(line):
                if lastHeader < i:
                    lineDict[i - lastHeader - 1] = cell
                else:
                    lineDict[headers[i]] = cell
            csvDict[lineDict[keyString]] = lineDict
    return csvDict

dLib = DynamoLib()

generalTableColumns = [
    {
        "attributeName": 'iso3',
        "keyType": 'HASH',
        "attributeType": 'S'
    },
]

economicTableInitialColumns = [
    {
        "attributeName": 'iso3',
        "keyType": 'HASH',
        "attributeType": 'S'
    }
]

populationTableInitialColumns = [
    {
        "attributeName": 'iso3',
        "keyType": 'HASH',
        "attributeType": 'S'
    }
]

dLib.create_table('edykhne_general_table', generalTableColumns)
dLib.create_table('edykhne_economic_table', economicTableInitialColumns)
dLib.create_table('edykhne_population_table', populationTableInitialColumns)

# getting un table
unTable = dLib.get_full_table('UN_country_codes')

# getting supporting csvs
areaCsvDict = csv_to_dict('shortlist_area.csv', 'ISO3')
capitalsCsvDict = csv_to_dict('shortlist_capitals.csv', 'ISO3')
curpopCsvDict = csv_to_dict('shortlist_curpop.csv', 'Country')
gdppcCsvDict = csv_to_dict('shortlist_gdppc.csv', 'Country')
languagesCsvDict = incomplete_headers_csv_to_dict('shortlist_languages.csv', 'ISO3', 1)

generalItems = []
economicItems = []
populationItems = []
for row in unTable:
    languageList = []
    i = 0
    while i in languagesCsvDict[row['iso3']]:
        languageList.append(languagesCsvDict[row['iso3']][i])
        i+=1
    #print(languageList)
    generalItems.append(
        {
            'iso3': row['iso3'],
            'country': row['name'],
            'official name': row['officialname'],
            'capital': capitalsCsvDict[row['iso3']]['Capital'],
            'area': areaCsvDict[row['iso3']]['Area'],
            'currency': curpopCsvDict[row['name']]['Currency'],
            'languages': set(languageList)
        }
    )
    econItem = {'iso3': row['iso3']}
    for key in gdppcCsvDict[row['name']].keys():
        if(any(i.isdigit() for i in key)) and gdppcCsvDict[row['name']][key] != '':
            econItem[''.join(c for c in key if c.isdigit())] = int(gdppcCsvDict[row['name']][key])
    economicItems.append(econItem)

    popItem = {'iso3': row['iso3']}
    for key in curpopCsvDict[row['name']].keys():
        if(any(i.isdigit() for i in key)) and curpopCsvDict[row['name']][key] != '':
            popItem[''.join(c for c in key if c.isdigit())] = int(curpopCsvDict[row['name']][key])
    populationItems.append(popItem)

dLib.bulk_load_items(generalItems, 'edykhne_general_table')
dLib.bulk_load_items(economicItems, 'edykhne_economic_table')
dLib.bulk_load_items(populationItems, 'edykhne_population_table')


