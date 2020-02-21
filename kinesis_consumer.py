import os
import ast
import json
import logging
import base64
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BC_ADMIN_URL = os.environ["BC_ADMIN_URL"]
BC_KEY = os.environ["BC_KEY"]
FINANCIAL_URL = os.environ["FINANCIAL_URL"]
DELETE_EVENT = "DeleteRowsEvent"
UPDATE_EVENT = "UpdateRowsEvent"
WRITE_EVENT = "WriteRowsEvent"
params = {"couponId": None}
auth_dict = { "Authorization": BC_KEY }
content_type_dict = { "Content-Type": "application/json" }

# Convert bytes to str, if required
def convert_str(s):
    return s.decode("utf-8") if isinstance(s, bytes) else s


def get_coupon_complete_data(coupon_id):
    logger.info("## COUPON ID TO GET")
    logger.info(coupon_id)
    params["couponId"] = coupon_id
    req = requests.get(BC_ADMIN_URL, params=params, headers={**auth_dict, **content_type_dict})

    logger.info("## DATA FROM BC API")
    logger.info(req.json())

    return req.json()

def get_coupon_id(operation_type, row_dict):
    """
    Gets coupon id depending on the operation_type.

    Parameters:
    operation_type (string): Event type from MySQL
    row_dict (dict): The dict with the row data

    Returns:
    coupon_id (int): Coupon id
    """
    logger.info("{} {}".format("##", operation_type))

    if operation_type in [WRITE_EVENT, DELETE_EVENT]:
        return row_dict["values"]["id"]

    if operation_type == UPDATE_EVENT:
        # before_values = data['row']['before_values']
        after_values_row = row_dict["after_values"]

        return after_values_row["id"]

    return -1


def lambda_handler(event, context):
    status_code = None
    for record in event["Records"]:
        # Kinesis data is base64 encoded so decode here
        data_b = base64.b64decode(record["kinesis"]["data"])
        data_str = convert_str(data_b)
        data = json.loads(data_str)
        logger.info("## DATA")
        logger.info(data)

        table_name = data["table"]
        if table_name == "coupon":
            logger.info("{} {} {} ".format("##", table_name.upper(), "TABLE"))
            operation_type = data["type"]

            coupon_id = get_coupon_id(operation_type, data["row"])

            coupon_data = get_coupon_complete_data(coupon_id)
            coupon_data['deleted'] = operation_type == DELETE_EVENT or False

            fin_call = requests.post(FINANCIAL_URL, data=json.dumps(coupon_data), headers=content_type_dict)
            logger.info("## RESPONSE FROM FINANCIAL API:")
            if fin_call.status_code == 200:
                status_code = 200
                logger.info(fin_call.json())
            else:
                status_code = 500
                logger.info(fin_call)

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": "",
    }
