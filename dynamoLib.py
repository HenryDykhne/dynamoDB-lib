import boto3
import csv
from boto3.dynamodb.conditions import Key, Attr

class DynamoLib:
    def __init__(self):
        session = boto3.Session(profile_name='edykhne',region_name='ca-central-1')
        self.dynamodb = session.resource('dynamodb', region_name='ca-central-1')
            ##check if we need to use config file again


    def create_table(self, tableName, columns):
        keySchema=[]
        attributeDefinitions=[]
        for column in columns:
            if('keyType' in column):
                keySchema.append(
                    {
                        'AttributeName': column['attributeName'],
                        'KeyType': column['keyType']
                    }
                )
            attributeDefinitions.append(
                {
                'AttributeName': column['attributeName'],
                'AttributeType': column['attributeType']
                }
            )
        table = self.dynamodb.create_table(
            TableName=tableName,
            KeySchema=keySchema,
            AttributeDefinitions=attributeDefinitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.wait_until_exists()

    def delete_table(self, tableName):
        table = self.dynamodb.Table(tableName)
        table.delete()

    def bulk_load_csv(self, csvFileName, tableName):
        table = self.dynamodb.Table(tableName)
        with open(csvFileName, 'r') as file:
            reader = csv.reader(file)
            with table.batch_writer() as batch:
                headers = next(reader, None)
                for row in reader:
                    item = {}
                    for i, cell in enumerate(row):
                        item[headers[i]] = cell
                    batch.put_item(Item = item)

    def bulk_load_items(self, items, tableName):
        table = self.dynamodb.Table(tableName)
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item = item)

    def add_item(self, item, tableName):
        table = self.dynamodb.Table(tableName)
        table.put_item(Item=item)

    def delete_item(self, key, tableName):
        table = self.dynamodb.Table(tableName)
        table.delete_item(Key=key)

    def get_full_table(self, tableName):
        table = self.dynamodb.Table(tableName)
        response = table.scan()
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        return items
##not sure
    def update_item_single_attribute(self, keyCol, key, attribute, newValue, tableName):
        table = self.dynamodb.Table(tableName)
        table.update_item(
            Key = {
                keyCol: key,
            },
            UpdateExpression = 'SET #c = :val',
            ExpressionAttributeNames = {
                '#c': attribute
            },
            ExpressionAttributeValues = {
                ':val': newValue
            }
        )

    def update_item_add_to_string_set(self, keyCol, key, setName, newItem, tableName):
        table = self.dynamodb.Table(tableName)
        table.update_item(
            Key = {
                keyCol: key,
            },
            UpdateExpression = 'ADD #c :val',
            ExpressionAttributeNames = {
                '#c': setName
            },
            ExpressionAttributeValues = {
                ':val': set([newItem])
            }
        )

    def retrive_rows_by_attribute(self, attrName, attr, tableName):
        table = self.dynamodb.Table(tableName)
        response = table.scan(FilterExpression=Attr(attrName).eq(attr))
        return response['Items']
        


 

# self.dynamodb_table.update_item(
#             Key={'KEY_NAME': key},
#             UpdateExpression="ADD string_set :elements",
#             ExpressionAttributeValues={":elements": [new_str]})

# db.update_item(TableName=TABLE,
#                Key={'id':{'S':'test_id'}},
#                UpdateExpression="ADD test_set :element",
#                ExpressionAttributeValues={":element":{"SS":['five']}})