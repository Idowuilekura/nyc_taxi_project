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
        folder_path = os.path.join(parent_folder_path, str(year)+'_' + str(month)+"_data")
        # file_path = os.path.join(parent_folder_path, f"{str(year)}_{str(month)}_data")
        # # file_path = str(file_path)
        # print(file_path)
        # file_path_new = parent_folder_path + file_path
        print(type(year))
        print(type(month))
        os.makedirs(folder_path, exist_ok=True,mode=0o777)
      
        print(folder_path)
    
        url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%s.parquet"%(year,month)
        # print(file_path)
        os.system(f"wget {url} -O {folder_path}/yellow_tripdata_{year}-{month}.parquet")

        # data = pd.read_parquet("yellow_tripdata_%s-%02d.parquet"%(year,month))

        return f"{folder_path}/yellow_tripdata_{year}-{month}.parquet",folder_path
    else:
        url ="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%s-%02d.parquet"%(year,month)
        os.system(f"wget {url} -O {parent_folder_path}/yellow_tripdata_{year}-{month:02}.parquet")
        return f"{parent_folder_path}/yellow_tripdata_{year}-{month:02}.parquet", 'X'
    


if __name__ == '__main__':
    download_store_data('2023','01','./data',True)