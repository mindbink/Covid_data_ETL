import requests
from tqdm.auto import tqdm
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.CRITICAL)

# Extract data from Github repo
def extract():

    url = 'https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports'
    res = requests.get(url)
    csv_files_urls = [data['download_url'] for data in tqdm(res.json()) if data['name'].endswith('.csv')]
    logging.debug('Number of files retrieved: ' + str(len(csv_files_urls)))
    return csv_files_urls

# Transform extracted data
new_labels = {'Lat':'Latitude',
              'Long_':'Longitude',
              'Confirmed':'Confirmed_cases'
              }

columns = ['Province_State', 'Country_Region', 'Last_Update',
           'Confirmed_cases', 'Deaths', 'Recovered'
           ]

def transform(files):

    df = pd.read_csv(files)
    logging.debug(df)
    df.rename(columns=new_labels, inplace=True)
    # print(list(df.columns))
    df = pd.DataFrame(df, columns = columns)
    # change datetime to date only
    df['Last_Update'] = pd.to_datetime(df['Last_Update']).dt.date

    return df


def load(db_name):
    
    # Credentials to database connection
    hostname ="localhost"
    user =    "root"
    password ="password"

    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                .format(host=hostname, db=db_name, user=user, pw=password))

    # transform every obtained csv file
    df_list = [transform(csv) for csv in tqdm(extract()[:1])]
    # crate single df from all obtained csv files
    single_df = pd.concat(df_list, axis=0, ignore_index=True, sort=False)
    # remove duplicate rows
    single_df.drop_duplicates(inplace=True)                                     
    # Convert dataframe to sql table    
    single_df.to_sql('example', engine, index=False, if_exists='append')
    # ALTER TABLE tbl ADD id INT PRIMARY KEY AUTO_INCREMENT;
    single_df.to_csv(r'C:\Users\Administrator\Desktop\File_Name.csv')

load('github')