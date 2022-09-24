import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../../')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.options.display.max_columns = 100
pd.options.display.width = 0


def add_features(table):
    non_features = ['date', 'team']
    features = [i for i in list(table) if i not in non_features]
    df = table.copy()
    means = df.groupby('date').transform('mean')
    stdevs = df.groupby('date').transform('std')
    for feature in features:
        # League Rank
        df[f'{feature}_rank'] = df.groupby(['date'
                                            ])[feature].rank(method='min',
                                                             ascending=False)
        # Difference from League Average
        df[f'{feature}_vla'] = df[feature] - means[feature]
        # Difference from League Average - Standard Deviations
        df[f'{feature}_vla_std'] = (df[feature] -
                                    means[feature]) / stdevs[feature]
    return df


if __name__ == "__main__":
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"
    port = "5432"

    with create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    ).connect() as connection:

        for table in [
                'traditional', 'advanced', 'four_factors', 'misc', 'scoring',
                'opponent', 'speed_distance', 'shooting', 'opponent_shooting',
                'hustle'
        ]:
            df = pd.read_sql_table(f"nba_{table}", connection)

            output_df = add_features(df)

            for column in list(output_df):
                if output_df[column].dtype == 'float64':
                    output_df[column] = pd.to_numeric(output_df[column],
                                                      downcast='float')
                if output_df[column].dtype == 'int64':
                    output_df[column] = pd.to_numeric(output_df[column],
                                                      downcast='integer')

            # print(output_df.head())
            # print(output_df.info(verbose=True))

            output_df.to_sql(f"{table}",
                             connection,
                             index=False,
                             if_exists="replace")
