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


def lambda_handler(event, context):
    for record in event["Records"]:
        # Kinesis data is base64 encoded so decode here
        data_b = base64.b64decode(record["kinesis"]["data"])
        data_str = convert_str(data_b)
        data = json.loads(data_str)
        logger.info("## DATA")
        logger.info(data)

        if data["table"] == "coupon":
            logger.info("## COUPON TABLE")
            operation_type = data["type"]
            logger.info(operation_type)

            coupon_data = None

            if operation_type == "WriteRowsEvent":
                logger.info("## WRITE ROWS EVENT")
                row = data["row"]["values"]

                coupon_id = row["id"]
                coupon_data = get_coupon_complete_data(coupon_id)

                fin_call = requests.post(FINANCIAL_URL, data=json.dumps(coupon_data), headers=content_type_dict)
                logger.info("## RESPONSE FROM FINANCIAL API:")
                if fin_call.status_code == 200:
                    logger.info(fin_call.json())
                else:
                    logger.info(fin_call)



            if operation_type == "UpdateRowsEvent":
                logger.info("## UPDATE ROWS EVENT")
                # before_values = data['row']['before_values']
                after_values_row = data["row"]["after_values"]

                coupon_id = after_values_row["id"]
                coupon_data = get_coupon_complete_data(coupon_id)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": "",
    }
