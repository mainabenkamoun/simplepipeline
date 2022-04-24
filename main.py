import pandas as pd
import apache_beam as beam
from apache_beam.dataframe import convert
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "credentials.yaml"
os.environ['GCLOUD_PROJECT'] = "training-projetc"

df = pd.read_csv('cereal.csv')
pd.set_option('display.max_columns', None)
print(df.head(10))


# finding the null_ratio
def compile_null_ratio(df):
    null_ratio = pd.DataFrame(df.isnull().sum() / df.shape[0])
    null_ratio.reset_index(inplace=True)
    null_ratio.columns = ['columns', 'nan_percentage']
    print('the null ratio for each column is')
    print(null_ratio)
    return null_ratio


# dropping column where na percentage is higher than 10%


def drop_na_percentage(null_ratio):
    columns_to_drop = null_ratio.loc[null_ratio.nan_percentage > 0.1]['columns']
    df.drop(columns=columns_to_drop, inplace=True)


print(df.dtypes)

# finding the categorical columns
def categorical_columns(df):
    print('here are the categorical columns')
    categorical_cols = df.select_dtypes('object').columns.tolist()
    print(categorical_cols)
    return (categorical_cols)


def replace_na_categorical_columns(df, categorical_cols):
    for col in categorical_cols:
        mode_df = df[col].mode()[0]  # calculate the mode
        df[col].fillna(mode_df, inplace=True)  # replace NaNs with that mode

# finding the numerical columns
def numerical_columns(df, categorical_cols):
    numerical_cols = df.columns.difference(categorical_cols).tolist()
    print('here are the numerical columns')
    print(numerical_cols)
    return (numerical_cols)


def replace_na_numerical_columns(df, numerical_cols):
    for col in numerical_cols:
        mean_df = df[col].mean()  # calculate the mean
        df[col].fillna(mean_df, inplace=True)  # replace NaNs with the mean


drop_na_percentage(compile_null_ratio(df))

replace_na_categorical_columns(df, categorical_columns(df))

replace_na_numerical_columns(df, numerical_columns(df, categorical_columns(df)))

compile_null_ratio(df)

pipeline = beam.Pipeline()
SCHEMA = 'name:STRING,mfr:STRING,type:STRING,calories:FLOAT,protein:FLOAT,fat:FLOAT,sodium:FLOAT,fiber:FLOAT,' \
         'carbo:FLOAT,sugars:FLOAT,potass:FLOAT,vitamins:FLOAT,shelf:FLOAT,weight:FLOAT,cups:FLOAT,rating:STRING '

(
        convert.to_pcollection(df, pipeline=pipeline)
        | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))
        | beam.io.WriteToBigQuery('training-projetc:cereals.cereal_data', schema=SCHEMA,
                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                  custom_gcs_temp_location='gs://pipeline-training-project')
)
result = pipeline.run()
