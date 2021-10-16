from dynamoLib import DynamoLib
import readline
import shlex

dLib = DynamoLib()

def update_population_record(countryName, year, popValue):
    iso3 = dLib.retrive_rows_by_attribute('country', countryName, 'edykhne_general_table')[0]['iso3']
    dLib.update_item_single_attribute('iso3', iso3, year, int(popValue), 'edykhne_population_table')


def add_language_record(iso3, language):
    #maybe check if iso3 value exists first
    dLib.update_item_add_to_string_set('iso3', iso3, 'languages', language, 'edykhne_general_table')

def print_instructions():
    print("avalaible commands: ")
    print("* update population info: upop <countryName> <year> <population_value>")
    print("* add language: alang <iso3> <language>")
    print("please use quotes for arguments that have spaces in them")
    print("* exit: exit")


print("welcome to the updateTables program")
print_instructions()
while(True):
    inputText = input("updateTables> ")
    args = shlex.split(inputText)
    if len(args) == 0:
        continue
    elif args[0] == "exit":
        exit()
    elif args[0] == "upop":
        update_population_record(args[1], args[2], args[3])
    elif args[0] == "alang":
        add_language_record(args[1], args[2])
    else: 
        print("invalid command")
        print_instructions()
