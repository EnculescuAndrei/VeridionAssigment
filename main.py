import pandas as pd

facebook_df = pd.read_csv("dataIN/facebook_dataset.csv", on_bad_lines='warn')
google_df = pd.read_csv("dataIN/google_dataset.csv", on_bad_lines='warn')
website_df = pd.read_csv("dataIN/website_dataset.csv", on_bad_lines='warn')

facebook_df.rename(columns={'categories': 'category'}, inplace=True)
website_df.rename(columns={
    'root_domain': 'domain',
    'main_city': 'city',
    'main_country': 'country_name',
    'main_region': 'region_name',
    's_category': 'category'
}, inplace=True)

merged_df = pd.merge(google_df, facebook_df, on='name', how='outer')

for column in google_df.columns:
    if column == 'name':
        continue
    google_col = f"{column}_x"
    facebook_col = f"{column}_y"

    if google_col in merged_df.columns and facebook_col in merged_df.columns:
        merged_df[column] = merged_df[google_col].combine_first(merged_df[facebook_col])
        merged_df.drop(columns=[google_col, facebook_col], inplace=True)

merged_df = pd.merge(merged_df, website_df, on='domain', how='outer')

for column in website_df.columns:
    if column in ['domain', 'name']:
        continue
    website_col = f"{column}_y"
    merged_col = f"{column}_x"
    if merged_col in merged_df.columns and website_col in merged_df.columns:
        merged_df[column] = merged_df[merged_col].combine_first(merged_df[website_col])
        merged_df.drop(columns=[merged_col, website_col], inplace=True)

ordered_columns = ['category', 'address', 'country_name', 'region_name', 'phone', 'name']
merged_df = merged_df[ordered_columns]
merged_df = merged_df.sort_values(by='category')


merged_df.to_csv('dataOUT/merged_dataset.csv', index=False)
