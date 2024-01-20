import pandas as pd
import os 
# year = 2023
# month = 1
# url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)



# print(url)
# # os.system('wget -O %s'%url)

# data = pd.read_parquet("yellow_tripdata_%s-%02d.parquet"%(year,month))


def download_store_data(year, month, parent_folder_path, create_new_folder=True):
    if create_new_folder:
        file_path = os.path.join(f'{parent_folder_path}', str(year)+"_data")
        file_path = str(file_path)
        os.makedirs(file_path, exist_ok=True)
    
        url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)
        # print(file_path)
        os.system(f"wget {url} -O {file_path}/yellow_tripdata_{year}-{month:02d}.parquet")

        # data = pd.read_parquet("yellow_tripdata_%s-%02d.parquet"%(year,month))

        return f"{file_path}/yellow_tripdata_{year}-{month:02d}.parquet"
    else:
        url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)
        os.system(f"wget {url} -O {parent_folder_path}/yellow_tripdata_{year}-{month:02d}.parquet")
        return f"{parent_folder_path}/yellow_tripdata_{year}-{month:02d}.parquet"
    



