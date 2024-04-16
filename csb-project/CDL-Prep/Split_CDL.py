import os
import arcpy
from pathlib import Path
year_lst = [i for i in range(2009,2024)]
CDL_folder = 'C:\\Users\\sizek\\Documents\\CSB-Data\\CDLs'
project_folder = '\\'.join(arcpy.env.workspace.split('\\')[:-1])
CDL_files = [f'{CDL_folder}\\{y}_30m_cdls\\{y}_30m_cdls.tif' for y in year_lst]
CDL_null_files = [f'{project_folder}\\Clean\\clean_{y}' for y in year_lst]
split_folders = [f'{project_folder}\\Split\\{y}' for y in year_lst]

CDL_nulls = [63,64,65,81,82,83,87,88,92,111,112,121,122,123,124,131,141,142,143,152,176,190,195]
split_size = 4096

# make all the folders needed
folders = ['Clean','Split']+[f'Split\\{y}' for y in year_lst] #+[f'Clean\\{y}' for y in year_lst]
for f in folders:
    try: 
        os.mkdir(f'{project_folder}\\{f}')
    except:
        pass
for y,CDL_file,split_folder in zip(year_lst,CDL_files,split_folders):
    print(f'Set Null: {y}')
    out_set_null = arcpy.ia.SetNull(CDL_file,CDL_file, f"Value IN ({','.join([f'{i}' for i in CDL_nulls])})")
    print(f'Split Raster: {y}')
    arcpy.management.SplitRaster(out_set_null, split_folder, f'{y}_c', 'SIZE_OF_TILE', 'TIFF', 'NEAREST', '', f'{split_size} {split_size}')