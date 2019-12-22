## Lambda to consume the Kinesis Data Stream

This lambda will be triggered by the Kinesis Data Stream and will parse only data from the `coupon` table coming from Barato Coletivo Database (MariaDB).

**Requirements:**

* Python 3.7
* `requests`

It's necessary to generate the `layer.zip` file with `requests` dependencies in order to the Lambda be able to call Barato's PHP endpoint.
