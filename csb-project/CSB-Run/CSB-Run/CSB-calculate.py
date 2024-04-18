import multiprocessing
import time
import os
from pathlib import Path
import argparse
import logging
import sys
from tracemalloc import start
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
national_ncl_folder = cfg['prep']['national_cnl_folder']
cellsize = 30 # for polygon to raster line 256
ncl_start_year = 2014

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def CSB_calc(create_dir,calc_dir,area,start_year,end_year):
        # file_path,prep_path,area,start_year,end_year):
    t_init = time.perf_counter()
    gbd_name = f'{area}_{start_year}-{end_year}_In.gdb'
    layer_name = f'{area}_0_In'
    feature_Vector_In = f"{create_dir}/Vectors_In/{gbd_name}/{layer_name}"
    combine_All_Name = f'{area}_0_{start_year}_{end_year}'
    raster_Combine_All = f'{create_dir}/CombineAll/{area}_0_{start_year}-{end_year}.tif'
    raster_NCLs = []
    ncl_names = []
    if end_year>=ncl_start_year:
        for year in range(ncl_start_year,end_year+1):
            if year<=2020:
                raster_NCLs.append(f"{national_ncl_folder}/{year}_30m_confidence_layer/{year}_30m_confidence_layer.img")
                ncl_names.append(f'ncl{year}')
            else:
                raster_NCLs.append(f"{national_ncl_folder}/{year}_30m_Confidence_Layer/{year}_30m_confidence_layer.tif")
                ncl_names.append(f'ncl{year}')
    # year_lst = [i for i in range(start_year,end_year+1)]
    # shapefile_name = shape_path.split('\\')[-1].split('.')[0] 

    #set up logger
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename=f'{calc_dir}/log/{area}_{start_year}-{end_year}.log',
                        level=logging.DEBUG, #by default it only log warming or above
                        format=LOG_FORMAT,
                        filemode='a')  # over write instead of appending
    logger = logging.getLogger()
    
    error_path = f'{calc_dir}/log/overall_error.txt'

    createGDB = False
    while createGDB == False:
        try:
            t0 = time.perf_counter()
            print(f"{area}: Creating GDBs")
            logger.info(f"{area}: Creating GDBs")
            arcpy.CreateFileGDB_management(out_folder_path= f'{calc_dir}/Polygon_Clip',
                                            out_name=f"{area}_{str(start_year)}-{str(end_year)}.gdb",
                                            out_version="CURRENT")
            createGDB = True
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)    

    # Rasterize in gdb
    # Add code to create .tif
    t1 = time.perf_counter()
    logger.info(f'{area}_{start_year}-{end_year}: Create .tif ')
    assignmentType = "CELL_CENTER"
    convert_raster = False
    while convert_raster == False:
        try:
            raster_Vector_In_file = calc_dir+f'\Raster_In\VIn{area}_{start_year}-{end_year}.tif'
            arcpy.conversion.PolygonToRaster(feature_Vector_In, "OBJECTID",
                                             raster_Vector_In_file,
                                             assignmentType, "NONE", cellsize)
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
    logger.info(f'c{area}: Convert to .tif takes {round((t2 - t1) / 60, 2)} minutes')
    
    logger.info(f'{area}: Create NCL Sub-Rasters ')
    t1 = time.perf_counter()
    # Calculate NCL value for each year which we have NCL data
    zonal_ncls = [arcpy.sa.ZonalStatistics(raster_Vector_In_file, "Value", ncl, "MEAN")
                     for ncl in raster_NCLs]
    t2 = time.perf_counter()
    logger.info(f'c{area}: NCL Sub-rasters takes {round((t2 - t1) / 60, 2)} minutes')
    
    # Combine with CombineAll
    logger.info(f'{area}: Combine and Join rasters')
    t1 = time.perf_counter()
    raster_combined = arcpy.gp.Combine_sa([raster_Vector_In_file,raster_Combine_All]+zonal_ncls)
    for zncl,ncl in zip(zonal_ncls,ncl_names):
        prev_name = str(zncl).split('\\')[-1]
        arcpy.management.AlterField(raster_combined, prev_name, ncl)
    
    join_table  = arcpy.management.AddJoin(raster_combined, f'{combine_All_Name}', raster_Combine_All, "Value")
    
    # TODO Save join field. 
    table_out = f'{calc_dir}/Table_Out/{area}_{start_year}-{end_year}.csv'
    arcpy.conversion.ExportTable(join_table, table_out)
    # Save off lines for neighbors
    raster = arcpy.sa.Raster(raster_Vector_In_file)
    bounding_box = [(raster.extent.XMin,raster.extent.YMin,raster.extent.XMin+30,raster.extent.YMax),
    (raster.extent.XMax-30,raster.extent.YMin,raster.extent.XMax,raster.extent.YMax),
    (raster.extent.XMin,raster.extent.YMin,raster.extent.XMax,raster.extent.YMin+30),
    (raster.extent.XMin,raster.extent.YMax-30,raster.extent.XMax,raster.extent.YMax)]
    bounding_box_str = [f'{i[0]} {i[1]} {i[2]} {i[3]}' for i in bounding_box]
    raster_slices = [f'{calc_dir}\Raster_Clip\{area}_{i}.tif' for i in range(0,4)]
    polygon_slices = [f'{calc_dir}\Polygon_Clip\{area}_{str(start_year)}-{str(end_year)}.gdb\{area}_{i}_poly' for i in range(0,4)]
    for rs,bb,ps in zip(raster_slices,bounding_box_str,polygon_slices):
        arcpy.Clip_management(raster_Vector_In_file,bb,rs)
        arcpy.conversion.RasterToPolygon(rs, ps, 'NO_SIMPLIFY', 'Value')
    return 
    
def CSB_Neighbor(calc_dir,start_year,end_year):
        #set up logger
    LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename=f'{calc_dir}/log/{area}_{start_year}-{end_year}.log',
                        level=logging.DEBUG, #by default it only log warming or above
                        format=LOG_FORMAT,
                        filemode='a')  # over write instead of appending
    logger = logging.getLogger()
    
    error_path = f'{calc_dir}/log/overall_error.txt'

    createGDB = False
    while createGDB == False:
        try:
            t0 = time.perf_counter()
            print(f"Neighbor: Creating GDBs")
            logger.info(f"Neighbor: Creating GDBs")
            arcpy.CreateFileGDB_management(out_folder_path= f'{calc_dir}/Neighbor_Mesh',
                                            out_name=f"Neigh_Mesh_{str(start_year)}-{str(end_year)}.gdb",
                                            out_version="CURRENT")
            createGDB = True
        except Exception as e:
            error_msg = e.args
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)
        except:
            error_msg = arcpy.GetMessage(0)
            logger.error(error_msg)
            f = open(error_path,'a')
            f.write(''.join(str(item) for item in error_msg))
            f.close()
            sys.exit(0)    
    
    # Grab all of the polygon files
    gdb_folders = [i.parts[-1] for i in Path(f'{calc_dir}/Polygon_Clip').rglob(f'*.gdb')]
    areas = [i.split('_')[0] for i in gdb_folders]
    inputs = [f'{calc_dir}/Polygon_Clip/{area}_{str(start_year)}-{str(end_year)}.gdb\{area}_{i}_poly' for area in areas for i in range(0,4)]
    poly_mesh = f'{calc_dir}/Neighbor_Mesh/Neigh_Mesh_{str(start_year)}-{str(end_year)}.gdb/Poly_Mesh'
    neigh = f'{calc_dir}/Neighbor_Mesh/Neigh_Mesh_{str(start_year)}-{str(end_year)}.gdb/Neigh_Mesh'
    arcpy.management.Merge(inputs, poly_mesh, '', 'ADD_SOURCE_INFO')
    arcpy.analysis.PolygonNeighbors(in_features=poly_mesh, out_table=neigh,both_sides="NO_BOTH_SIDES")    
    return

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

    csb_filePath = f'{create_dir}/Vectors_IN'
    gdb_folders = [i.parts[-1] for i in Path(csb_filePath).rglob(f'*.gdb')]
    areas = [i.split('_')[0] for i in gdb_folders]
    processes = []
    
    for area in areas:
        p = multiprocessing.Process(target=CSB_calc, 
                                    args=[create_dir, calc_dir,
                                          area, start_year, end_year])
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
    
    CSB_Neighbor(calc_dir,start_year,end_year)

    time_final = time.perf_counter()
    print(f'Total time to run CSB calc: {round((time_final - time0) / 60, 2)} minutes')