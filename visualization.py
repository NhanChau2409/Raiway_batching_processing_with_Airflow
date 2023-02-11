import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from datetime import date

from private_key import *

def plot(db_conn, name: str, type, sql):
    '''Plot bar chart with first columns as name of object and second column is amount'''
        
    df = pd.read_sql(sql=sql, con=db_conn)
    match type:
        case 'bar':
            plt.barh(df.iloc[:, 0], df.iloc[:, 1], )
        case 'pie':
            plt.pie(x=df.iloc[:, 1], labels=df.iloc[:, 0])
            
    plt.title(name.upper)
    plt.savefig(f'/Users/nhanchau/gitRepo/Raiway_with_AIrflow/plot_fig/{date.today()}-{name.upper}')
    plt.close()
    
if __name__ == '__main__':
    engine = create_engine(f"postgresql+psycopg2://{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}@localhost:5432/airflow_db")
    db_conn = engine.connect()
    
    train_sql= 'SELECT traincategory_name, COUNT(traincategory_name) AS number FROM trains_res GROUP BY traincategory_name;'
    operator_sql= 'SELECT operatorname, COUNT(operatorname) AS number FROM operator_res GROUP BY operatorname;'
    type_sql= 'SELECT type, COUNT(type) AS number FROM station_res GROUP BY type;'
    
    plot(db_conn, 'train-category', 'pie', train_sql)
    plot(db_conn, 'operator', 'bar', operator_sql)
    plot(db_conn, 'type', 'pie', type_sql)
    
    db_conn.close()
    
