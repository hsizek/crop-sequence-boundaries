import multiprocessing
import time
import os
from pathlib import Path
import argparse
import logging
import sys
# CSB-Run utility functions
import utils

# load ArcGIS packages, keep trying until it works
arcpy_loaded = False
while arcpy_loaded is False:
    try:
        import arcpy
        from arcpy.sa import *
        arcpy_loaded = True
        
    except RuntimeError:
        print('Arcpy not loaded. Trying again...')
        time.sleep(1)

#global vars
cfg = utils.GetConfig('default')
agdists = cfg['prep']['cnty_shp_file']
national_cdl_folder = cfg['prep']['national_cdl_folder']
cellsize = 30 # for polygon to raster line 256
ncl_start_year = 2014

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def CSB_calc(file_path,shape_path,prep_path,start_year,end_year):
    t_init = time.perf_counter()
    
    # gdb_name = c_{000}_{start_year}-{end_year}_In.gdb
    #gdb_name = shape_path.split('\\')[-1].split('.')[0]
    
    # layer_name = f'c{000}_0_In'
    
    # CombineAll_name = c_{000}_0_{start_year}-{end_year}.tif

    # NCL_names = 
    shapefile_name = shape_path.split('\\')[-1].split('.')[0]
    
    year_lst = [i for i in range(start_year,end_year+1)]

    #set up logger
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename=f'{prep_path}/log/{shapefile_name}.log',
                        level=logging.DEBUG, #by default it only log warming or above
                        format=LOG_FORMAT,
                        filemode='a')  # over write instead of appending
    logger = logging.getLogger()
    
    error_path = f'{prep_path}/log/overall_error.txt'

    # Rasterize in gdb
    # Add code to create .tif
    t1 = time.perf_counter()
    logger.info(f'{shapefile_name}: Create .tif ')
    assignmentType = "CELL_CENTER"
    convert_raster = False
    while convert_raster == False:
        try:
            arcpy.conversion.PolygonToRaster(subregion + '_CNTY1', "OBJECTID",
                                             file_path+f'\Raster_Out\{shapefile_name}.tif',
                                             assignmentType, "NONE", cellsize)

            tif_raster_file = file_path+f'\Raster_Out\{shapefile_name}.tif'
            convert_raster = True

        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
           
        
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
           
    
    t2 = time.perf_counter()
    
    logger.info(f'{shapefile_name}: Convert to .tif takes {round((t2 - t1) / 60, 2)} minutes')
    # Calculate NCL value for each year which we have NCL data
    if end_year>=ncl_start_year:
        for y in range(ncl_start_year,end_year+1):

    # Combine with CombineAll

    

    # Do the neighbor analysis
    import arcpy
    from arcpy.sa import *

    # Set environment settings
    arcpy.env.workspace = r"Path\to\your\workspace"
    arcpy.env.overwriteOutput = True

    # Define input raster
    original_raster = "path\to\your\original_raster.tif"

    # Create a raster that is one pixel smaller in every direction with a uniform value
    smaller_raster = Con(IsNull(original_raster), 0, 1)

    # Save the smaller raster
    smaller_raster.save("path\to\save\smaller_raster.tif")

    # Merge the two rasters together
    output_raster = arcpy.management.MosaicToNewRaster([original_raster, smaller_raster], 
                                                    arcpy.env.workspace, 
                                                    "output_raster.tif", 
                                                    pixel_type="32_BIT_FLOAT", 
                                                    number_of_bands=1)
    return 0 

if __name__ == '__main__':

    time0 = time.perf_counter()
    print('Starting CSB calc code... ')
    
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    # Modify from prep directory to calc directory 
    calc_dir = sys.argv[3] # create_1421_20220511_1
    print(f'Directory: {calc_dir}')
    # TODO insert calc into get run folder
    create_dir = utils.GetRunFolder('calc', start_year, end_year)
    print(f'Using results from: {create_dir}')

    csb_year = f'{str(start_year)[2:]}{str(end_year)[2:]}'

    cfg = utils.GetConfig('default')
    file_path = create_dir

    csb_filePath = f'{file_path}/Vectors_IN'
    # Check to make sure that this can get the gdb folders 
    file_obj = Path(csb_filePath).rglob(f'*.gdb')

    list_of_files = sorted(file_obj, key=lambda x: os.stat(x).st_size)
    file_lst = [x.__str__() for x in list_of_files]
    
    processes = []
    
    for shape_path in file_lst:

        p = multiprocessing.Process(target=CSB_calc, 
                                    args=[file_path, shape_path, calc_dir,
                                          csb_year, start_year, end_year])
        processes.append(p)

    # get number of CPUs to use in run
    cpu_prct = float(cfg['global']['cpu_prct'])
    run_cpu = int(round( cpu_prct * multiprocessing.cpu_count(), 0 ))
    print(f'Number of CPUs: {run_cpu}')
    for i in chunks(processes,run_cpu):
        for j in i:
            j.start()
        for j in i:
            j.join()

    time_final = time.perf_counter()
    print(f'Total time to run CSB calc: {round((time_final - time0) / 60, 2)} minutes')