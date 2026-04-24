### Storage links S3 - Data lake

https://eu-north-1.console.aws.amazon.com/s3/buckets/

### Description of our three buckets and their contents

/ppml-fastapi : contains the artifact store of our fastapi

/ppml-mlflow : contains the artifact store of our mlflow
f
/ppml2026 : 
    
    - datasets/ : Contains the data used for the model training, eda and feature engineering (Signof...) and the standard file reference for the inference service (df_train...)

    - image/ : For the Streamlit service (logo_...)

    - presentations/ : overview of our project in .pptx format

    - raw/ : stores the raw data from the call api responses (BRONZE). Each data is stored in a specific folder for the multi-user case (scale compatible)

            - 2026-04-22/ : the date on which the user run his/her request on our app

                - requete_AF1234_20260422_170113/ : the request id folder for the specific call in the day.

                    - SignoffFlightsDataset_Single_requete_AF7362_20260422_092108.parquet : the single line data that results from the call api that will be processed

                    - API_Single_ERR.log : a log file for us FlyOnTime for keeping track of these errors if any

                    - flight_request_status.json : a json file used by the the app to treat and output correctly the error at hand



    - processed/ : stores the processed data (SILVER). Its organisation is alike to the raw/ folder

            - 2026-04-22/ : the date on which the user run his/her request on our app

                - requete_AF7421_20260422_093538/ : the request id folder for the specific call in the day.

                    - single_flight_model_input_requete_AF7421_20260422_093538.parquet : the single line data that results from the call api that will be for the inference service
    

    - screen_records/ : a demo of our product FlyOnTime



     
