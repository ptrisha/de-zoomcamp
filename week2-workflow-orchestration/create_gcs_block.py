# Create gcs block for prefect cloud
from prefect_gcp import GcpCredentials 
from prefect_gcp.cloud_storage import GcsBucket    
                                                                      
gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")  
                                                                          
GcsBucket(bucket="dtc_data_lake_steady-cascade-376200",                                   
          gcp_credentials=gcp_credentials_block                
          ).save("zoom-gcs") 
