import os
import pandas as pd

if __name__ == "__main__":
    data_path = os.path.join(os.path.dirname(__file__), "../data/")
    file_name = os.path.join(data_path, "db.csv")

    df = pd.read_csv(file_name)

    df_no_empty = df.dropna(how='all')

    df_no_empty.columns = ['business_area', 'database_name', 'role_name']

    df_no_empty['business_area'] = df_no_empty['business_area'].fillna(method='ffill').str.strip()
    df_no_empty['database_name'] = df_no_empty['database_name'].fillna(method='ffill').str.lower().str.strip()
    df_no_empty['role_name'] = df_no_empty['role_name'].fillna(method='ffill').str.strip()

    df_no_empty.to_parquet(os.path.join(data_path, "db.parquet"), compression='snappy', index=False)
    pass
