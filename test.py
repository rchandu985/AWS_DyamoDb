import boto3
import json
from boto3.dynamodb.conditions import Attr,Key

#boto3.setup_default_session(profile_name="rchandu985")
table_name = "chandu"
file=open("test.json",'r')
data=json.load(file)

def put_item_using_resource():
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.Table(table_name)
    response = table.put_item(
        Item={
            "id": "5",
            "skillKeywords":[{'python':19}]
        }
    )
    print(json.dumps(response, indent=2))

#put_item_using_resource()

def delete():
    client=boto3.resource("dynamodb")
    table=client.Table(table_name)

    responce=table.delete_item(
        Key={"id":"6"}
    )
    print(responce)

def bulkdelete():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('chandu') # Change to your table name

    item_1 = {"id":"2"} # Use your table's key/values
    item_2 = {"id":"8"}

    items_to_delete = [item_1, item_2]

    with table.batch_writer() as batch:
        for item in items_to_delete:
            response = batch.delete_item(Key={
                "id": item["id"], # Change key and value names
                
            })
#bulkdelete()

#delete()


def fetch():
    
    client=boto3.resource("dynamodb")
    table=client.Table(table_name)

    #response=table.scan(FilterExpression=Attr("skills.python").gt(5))#for dictionary
    response=table.scan(FilterExpression=Attr("skillKeywords").contains("python"))#list filter
    print(response['Items'])
fetch()

