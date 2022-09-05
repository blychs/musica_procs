#!/usr/bin/env python

import xarray as xr
import datetime as dt
import glob


#====================================
# User mods
#====================================
pattern_of_files = 'emissions-finnv2.5modvirs_*_bb_surface_20170101-20201231_0.1x0.1.nc_ne0np4.SAMwrf01.ne30x4'

#====================================
# End user mods
#====================================

def add_date(file_name):
    print(file_name)
    with xr.open_dataset(file_name) as ds:
        date = ds['time'].dt.strftime('%Y%m%d').astype(int)
        ds = ds.assign(date=date)
        ds['date'] = ds['date'].assign_attrs(units='YYYYMMDD',
                                             long_name="Date")
        ds['time'].encoding['units'] = 'hours since 1850-01-01'
        ds.to_netcdf(f'date_{file_name}', format='NETCDF3_64BIT')
    print(f'{file_name} done')

def main():
    list_of_files = sorted(glob.glob(pattern_of_files))
    for i in list_of_files:
        add_date(i)

#===================================
if __name__ == '__main__':
    main()
