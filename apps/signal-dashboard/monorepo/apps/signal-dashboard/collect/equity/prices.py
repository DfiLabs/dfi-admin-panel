import pandas as pd 
import numpy 
from datetime import datetime 
import json 
import os 

from dfilabs.utils.utils_os import get_root_directory 
from dfilabs.utils.utils_date import sort_date_index 
from dfilabs.collect.utils.utils_equity import convert_equity_name




def unzip_prices():
    root_directory = get_root_directory()
    root_directory = f'{root_directory}/storage/EQUITY'

    path_close = f'{root_directory}/RIY Index_PX_LAST.csv'
    path_high  = f'{root_directory}/RIY Index_PX_HIGH.csv'
    path_low   = f'{root_directory}/RIY Index_PX_LOW.csv'
    path_open  = f'{root_directory}/RIY Index_PX_OPEN.csv'
    path_volume = f'{root_directory}/RIY Index_PX_VOLUME.csv'
    path_cur_mkt_cap = f'{root_directory}/RIY Index_CUR_MKT_CAP.csv'
    #path_short_int_ratio = f'{root_directory}/RIY Index_SHORT_INT_RATIO.csv'
    #path_eqy_dvd_yld_12m = f'{root_directory}/RIY Index_EQY_DVD_YLD_12M.csv'

    feature_to_path = {
        'close': path_close,
        'high': path_high,
        'low': path_low,
        'open': path_open,
        'volume': path_volume,
        'marketcap': path_cur_mkt_cap,
        #'short_int_ratio': path_short_int_ratio,
        #'eqy_dvd_yld_12m': path_eqy_dvd_yld_12m
    }

    path_index = f'{root_directory}/RIY Index_binary.csv'
    df_index = pd.read_csv(path_index, index_col=0, parse_dates=True,engine='pyarrow')
    available_products = list(df_index.columns)

    # Read each feature CSV only once
    feature_dataframes = {}
    issue_products = set()
    
    print("Loading feature CSV files...")
    for feature, path in feature_to_path.items():
        try:
            print(f"Loading {feature} from {path}")
            df = pd.read_csv(path, index_col=0, parse_dates=True, engine='pyarrow')
            feature_dataframes[feature] = df
        except Exception as e:
            print(f'Error loading {feature} CSV: {e}')
            # If we can't load a feature CSV, mark all products as issues
            issue_products.update(available_products)
            continue
    
    print(f"Processing {len(available_products)} products...")
    for product in available_products:
        print(f'Processing {product}')
        product_name_convert = convert_equity_name(product)
        file_path = f'{root_directory}/EQUITY_prices/{product_name_convert}.csv'
        if os.path.exists(file_path):
            continue
        
        concat = []
        product_has_issues = False
        
        for feature, df_feature in feature_dataframes.items():
            try:
                if product not in df_feature.columns:
                    print(f'Product {product} not found in {feature} data')
                    product_has_issues = True
                    break
                
                series = df_feature[product]
                series.name = feature
                concat.append(series)
            except Exception as e:
                print(f'Error collecting {product} for {feature}: {e}')
                product_has_issues = True
                break
        
        if product_has_issues:
            issue_products.add(product)
        elif len(concat) == len(feature_to_path):
            df_prices = pd.concat(concat, axis=1)
            df_prices.columns = feature_to_path.keys()

            df_prices = sort_date_index(df_prices)
            df_prices = df_prices.dropna()
            df_prices.to_csv(file_path)
        else:
            issue_products.add(product)

    print(f"Completed processing. {len(issue_products)} products had issues.")
    with open(f'{root_directory}/issue_products.json', 'w') as f:
        json.dump(list(issue_products), f, indent=4)

def get_prices(product) : 
    root_directory = get_root_directory()
    root_directory = f'{root_directory}/storage/EQUITY'
    
    path = f'{root_directory}/EQUITY_prices/{product} US Equity.csv'
    df = pd.read_csv(path,index_col=0,parse_dates=True,engine='pyarrow')
    return df




    

