import polars as pl
from polars import col as c
import os
import re
import numpy as np

def load_sales(year):
    here = os.path.abspath(__file__)
    herepath = '/'.join(here.split('/')[:-1])
    os.chdir(herepath)
    if not os.path.isdir('raw'):
        raise FileNotFoundError('No \'raw\' folder found')
    
    os.chdir('raw')
    poss_years = os.listdir()
    folders = poss_years
    pattern = r'20[0-9]{2}'
    poss_years = [re.findall(pattern, folder)[-1] if re.search(pattern, folder) else None for folder in poss_years]
    poss_years = [int(year) for year in poss_years if year is not None]
    if year not in poss_years:
        raise AttributeError(f'{year} not found, options are {poss_years}')
    
    yearfolder = [folder for folder in folders if re.search(str(year), folder)]
    os.chdir(yearfolder[0])

    try:
        sales = pl.read_excel(f'Sales_Ult_Cust_{year}.xlsx')
    except FileNotFoundError:
        sales = pl.read_excel(f'Sales_Ult_Cust_{year}.xls')
    colnames = sales.row(1)
    colnames = [colname.split('\n')[0].replace(' ', '_').lower().strip() for colname in colnames]
    colnames = ['data_type' if colname == 'data_type_x000d_' else colname for colname in colnames]
    custy_types = ['RESIDENTIAL', 'COMMERCIAL', 'INDUSTRIAL', 'TRANSPORTATION', 'TOTAL']
    curtype = 0
    for i, colname in enumerate(colnames):
        custy_type = custy_types[curtype]
        if colname == 'thousands_dollars':
                colname = 'thousand_dollars'
        if colname in ['thousand_dollars', 'megawatthours']:
            colnames[i] = colname + '_' + custy_type.lower()

        if colname == 'count':
            colnames[i] = 'customers_' + custy_type.lower()
            curtype += 1

    sales = sales.tail(-2).head(-1)
    sales.columns = colnames
    relevant_cols = ['data_year', 
                     'utility_number', 
                     'utility_name', 
                     'part', 
                     'service_type', 
                     'data_type', 
                     'state', 
                     'ownership', 'thousand_dollars_residential', 
                     'megawatthours_residential', 
                     'customers_residential', 
                     'thousand_dollars_commercial', 
                     'megawatthours_commercial', 
                     'customers_commercial', 
                     'thousand_dollars_industrial', 
                     'megawatthours_industrial', 
                     'customers_industrial', 
                     'thousand_dollars_transportation', 
                     'megawatthours_transportation', 
                     'customers_transportation', 
                     'thousand_dollars_total', 
                     'megawatthours_total', 
                     'customers_total']

    os.chdir(herepath)
    return sales.select(relevant_cols)

def load_all_sales():
    dfs = [load_sales(year) for year in range(2010, 2025)]
    return pl.concat(dfs, how = 'vertical')

def main():
    all_sales = load_all_sales()
    all_sales.write_excel('data/all_sales.xlsx')

if __name__ == '__main__':
    main()