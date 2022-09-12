# File: lambda_function.py
This file ingests data from the official USDA API (https://app.swaggerhub.com/apis/fdcnal/food-data_central_api/1.0.1#/Nutrient) to an S3 bucket. In this particular case, the bucket is named `usdacsvs` as in the `lambda_handler` function. Since there is a transfer limit, requests are made in chunks of up to 25(?) fdcIds at a time. For each chuck, the output is a file of the format `FDCID_{START}_{END}` where `START` and `END` are the lowest and highest fdcIds for that chunk. So, for a chunk of ingested data from fdcIds 1115900 to 1115910, the csv file will be named `FDCID_1115900_1115910.csv`.

The module `requests` is not included in the runtime of lambda, and so that module must be zipped with `lambda_function.py` before being deployed to the Lambda service. To see how to do this, visit the **Deployment package with dependencies** and **Deploy your .zip file to the function** sections in https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-dependencies. 

![File Structure](/assets/dependencies.png)

To test the Step function, pass the following json-formatted input, given that the `FDC_ID_RANGE` in `lambda_function.py` is of size 10. This will cause three instances of the function to run, resulting in three appropriately titled csv files in the `usdacsvs` bucket.
```
{
  "START_FDCID": 1105904,
  "END_FDCID": 1105930,
  "inc": 10
}
```

Delete the above.

![File Structure](/assets/state_input_as_payload.png)



## S3 CSVs to Redshift


















