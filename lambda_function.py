
from dataclasses import dataclass
import boto3

DYNAMO_TABLE_NAME = 'Client_Management'

@dataclass
class Client:
    id: str
    name: str
    operation_order_id: int

def put_item_into_dynamodb(id: str, name:str) -> str:
    '''Put a new item on dynamoDB table and then return an string labeling
        the operation result.'''
        
    dynamodb_client =  boto3.client('dynamodb')

    operation_order_id =  dynamodb_client.get_item(
    TableName = DYNAMO_TABLE_NAME,
    Key = {'id': {'S': 'client_meta_data'}},
    AttributesToGet=["operation_order_id"],
    ConsistentRead=True,
    )
    
    current_operation_order_id = operation_order_id['Item']['operation_order_id']['N']
    next_operation_order_id = int(current_operation_order_id) + 1

    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                'Update':{
                    'TableName': DYNAMO_TABLE_NAME,
                    'Key': {'id': {'S': 'client_meta_data'}},
                    "ConditionExpression": "operation_order_id = :current_operation_order_id",
                    "UpdateExpression": "SET operation_order_id = operation_order_id + :incr",
                    'ExpressionAttributeValues':{
                        ":incr": {"N": '1'},
                        ":current_operation_order_id": {"N": current_operation_order_id}
                    }                    
                }
            },

            {
                'Put':{
                    'TableName': DYNAMO_TABLE_NAME,
                    'Item':{
                        "id": {"S": id},
                        "name": {"S": name},
                        "operation_order_id": {"N": str(next_operation_order_id)},
                        },
                        'ConditionExpression': "attribute_not_exists(id)", 
                }
            }
        ]
    )


def lambda_handler(event, context):
    '''Start execution point.'''

print('start')

Client.id = '1234567890'
Client.name = 'Guilherme'

put_item_into_dynamodb(Client.id,Client.name)

lambda_handler('','')