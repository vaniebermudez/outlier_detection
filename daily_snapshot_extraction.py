import getpass
from sqlalchemy import create_engine
import pandas as pd
import os
import datetime
from collections import Counter
from datetime import datetime, timedelta


def query_ids(user, password, query_text=None, query_file=None, output=None):
    """Query IDS data"""

    host = 'inteldatastore-cpm-prod.cuvvdjkpet4x.eu-west-1.redshift.amazonaws.com'
    db = 'analytical'
    port = '5439'

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}", connect_args={'sslmode':'require'})
    # engine = create_engine(f"jdbc:redshift:iam://inteldatastore-cpm-prod.cuvvdjkpet4x.eu-west-1.redshift.amazonaws.com:5439/analytical? idp_port=443&plugin_name=com.amazon.redshift.plugin.PingCredentialsProvider&ssl=true&partner_spid=intelds:prd:redshift:directclientEU&idp_host=ping-sso.schneider-electric.com")
    engine.connect()

    if query_file:
        with open(query_file, 'r') as query:
            df = pd.read_sql_query(query.read(), engine)
    
    if query_text:
        df = pd.read_sql_query(query_text, engine)
    
    if output:
        df.to_csv(fr'{output}.csv', index=False)
        
    else:
        return df
    

# report date
now = datetime.now()
report_date = now.strftime('%Y-%m-%d')
print(f'Data Extracting for Report Date {report_date}')


# Input from User
folder = r'C:/Users/SESA676486/OneDrive - Schneider Electric/7) Data Quality and Support Centre/02. Finance RDO/03. Data Discovery/Daily Snapshots'
user = input('SESAID: ')
password = getpass.getpass("Password for " + user + ":")
queries_path = folder + '/queries'
all_queries = os.listdir(queries_path)


# read SQL scripts 
sql_dict = {}
for query in all_queries:  
   with open(os.path.join(queries_path, query),"r") as f:
      text = f.read().rstrip()
      sql_dict[os.path.splitext(query)[0]] = text


# file name as key
for key, value in sql_dict.items():
    print(f'Extracting {key}')
    value.replace('%', '%%')
    df = query_ids(user, password, value)
    print(f'Exporting {report_date}_{key}.csv')
    df.to_csv(f'{folder}/{report_date}_{key}.csv', index=False)
    print('Done')