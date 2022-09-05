#!/usr/bin/env python

import os
import gc
import glob
import xarray as xr
import netCDF4 as nc4
import vivaldi_a.analysis.Regridding_ESMF as vvld
import numpy as np

#####################################
#           USER CHANGES            #
#####################################

# path_to_input_files
input_path = '/glade/scratch/plichtig/emis/finnv2.5/'
# Pattern to find emis
pattern_of_filenames = 'emissions-finnv2.5modvirs_*_bb_surface_20170101-20201231_0.1x0.1.nc'
input_grid = 'Gridinfo_CAMSv5.1_c20210407.nc' # should end in ".nc"
orig_res = '0.1x0.1'
dst_res = 'ne0np4.SAMwrf01.ne30x4'
output_grid = '/glade/work/plichtig/gridfiles/SAMwrf01_ne30x4_np4_SCRIP.nc'
output_path = '/glade/scratch/plichtig/emis/finnv2.5.ne0np4.SAMwrf01.ne30x4/'

# Fields to regrid (if dynamic fields are needed,
# uncomment lines 115-118
fields = ['fire'] 
#####################################
#         USER CHANGES END          #
#####################################

# Def some useful functions
# Wrap to 0-360

def wrap360(ds, lon='lon'):
    """
    wrap longitude coordinates to 0..360

    Parameters
    ----------
    ds : Dataset
        object with longitude coordinates
    lon : string
        name of the longitude ('lon', 'longitude', ...)

    Returns
    -------
    wrapped : Dataset
        Another dataset array wrapped around.
    """

    # wrap -180..179 to 0..359    
    ds.coords[lon] = np.mod(ds[lon], 360)

    # sort the data
    return ds.reindex({ lon : np.sort(ds[lon])})

def dst_file_name(output_path, orig_file, dst_res):
    
    # return orig_file.replace(orig_res, dst_res)
    return output_path + dst_res +'_' + orig_file

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# ! Prepare Grid and compute weights !
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# wrap grid to 0..360

wrapped_input_grid = f'{input_grid[:-3]}_0-360.nc'
with xr.open_dataset(input_grid) as not_wrapped_grid:
    wrapped_ds = wrap360(not_wrapped_grid)
wrapped_ds.to_netcdf(wrapped_input_grid)
input_grid = wrapped_input_grid

bnds_input_grid = f'{input_grid[:-3]}_bnds.nc'
vvld.Add_bounds(filename=input_grid,
                newfilename=bnds_input_grid, creation_date=False)

input_grid = bnds_input_grid

# Load
list_of_emis = sorted(glob.glob(input_path + pattern_of_filenames),
                      key=str.casefold)

print('Using files:')
print(*list_of_emis, sep='\n')


#Create weights
#If there is a previous one, delete it
weights = f'{output_grid[:-3]}_WEIGHTS_a.nc'
try:
    os.remove(weights)
    print('removing wgt_file')
except OSError:
    pass


print('Creating weights')
emis_for_wgt = list_of_emis[0]
with xr.open_dataset(emis_for_wgt) as emis:
    vvld.Regridding(
                    var_array=emis[fields[0]],
                    src_grid_file=input_grid,
                    dst_grid_file=output_grid, creation_date=False,
                    wgt_file=weights, save_wgt_file=True, 
                    save_wgt_file_only=True, dst_file=weights)
print(f'Weights file = {weights}')

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !       Regrid and add date        !
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


for emis_name in list_of_emis:
    print("################################################################\n",
          emis_name,
          "\n################################################################")
    
    with xr.open_dataset(emis_name) as emis:
        # The split is to select emis_name after last '/' char, so that
        # the emis_path is not copied
        dst_file = dst_file_name(output_path, dst_res, emis_name.split('/')[-1])
        print(f'dst_file = {dst_file}')
# Uncomment for dinamically changing fields
#########
#         fields = []
#         for var in emis.variables:
#             if len(emis[var].dims) >+ 2:
#                 fields.append(var)
#########
        print(f'regridding fields: {fields}')
        vvld.Regridding(
                        var_array=emis,
                        fields=fields,
                   #    add_fields=['date'],
                        src_grid_file=input_grid,
                        dst_grid_file=output_grid,
                        wgt_file=weights,
                        save_wgt_file=False,
                        save_results=True,
                        creation_date=False,
                        dst_file=dst_file)
    gc.collect()

 
# Add date (Doseong's vivaldi_a code doesn't do it)
# emis_regridded = sorted(glob.glob(f'{output_path}*{dst_res}'))
# print('Adding dates')
# for i in emis_regridded:
#     print(f'i:')
#     with xr.open_dataset(i, mode='a') as ds:
#         date = ds['time'].dt.strftime('%Y%m%d').astype(int)
#         ds = ds.assign(date=date)
#         ds['date'] = ds['date'].assign_attrs(units="YYYYMMDDY",
#                                              long_name="Date")
#         ds['time'].encoding['units'] = 'hours since 1850-01-01'
#         ds.to_netcdf(i, format='NETCDF3_64BIT')
#     print(f'{i} done')
