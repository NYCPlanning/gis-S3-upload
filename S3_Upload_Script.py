import boto3, os, datetime, arcpy, shutil, zipfile, ConfigParser, traceback, time, sys, xml.etree.ElementTree as ET


try:
    # Set script start-time for logging run-time purposes
    print('Beginning script execution')
    StartTime = datetime.datetime.now().replace(microsecond = 0)

    # Set configuration file for defining path and credential information
    print('Parsing configuration file')
    config = ConfigParser.ConfigParser()
    config.read(r'S3_config_template.ini')

    # Open log file for outputting start-time, end-time, and uploaded datasets
    print('Assigning log path')
    log_path = config.get('PATHS', 'log_path')
    log = open(log_path, 'a')

    # Define function for generating temp directories
    def generate_directories(dir):
        if os.path.exists(dir):
            print('Previous export temp directory exists. Deleting and re-generating temp directory.')
            shutil.rmtree(dir, ignore_errors=True)
            time.sleep(0.1)
            os.mkdir(dir)
        else:
            print('Creating temp directory for exports')
            os.mkdir(dir)

    # Assign path variable from configuration file
    print('Assigning path variables')
    root_directory = config.get('PATHS', 'root_directory')
    temp_path = config.get('PATHS', 'temp_path')
    sde_path = config.get('PATHS', 'sde_path')
    DTM_path = config.get('PATHS', 'DTM_path')

    # Assign ArcGIS support files directory for use of appropriate translator xml
    Arcdir = arcpy.GetInstallInfo('desktop')['InstallDir']
    translator = Arcdir + 'Metadata/Translator/ARCGIS2FGDC.xml'

    # Assign list of data sets directories to parse
    print('Assigning data set directories')
    dcp_dataset_directories = ['MIH', 'E_Des', 'Zoning', 'IDHA', 'TransitZones', 'WAP', 'Designated_Areas_M_districts_AppendixJ', 'FRESH']

    # Assign list of data sets with same naming conventions on BytesProduction
    long_subdir_dates = ['MIH', 'E_Des', 'IDHA', 'TransitZones', 'WAP', 'Designated_Areas_M_districts_AppendixJ', 'FRESH']

    # Generate temp zip directory
    generate_directories(temp_path)
    # Generate temp metadata directory
    generate_directories(os.path.join(temp_path, 'metadata'))

    # Set arcpy environment workspace to temporary directory
    arcpy.env.workspace = temp_path
    arcpy.env.overwriteOutput = True

    # Steps for targeting most recent MapPLUTO dataset directory
    print('Beginning data sets copy to temp location')
    directory_years = []
    for directory in os.listdir(os.path.join(root_directory, 'MapPLUTO')):
        if directory[:2].isdigit():
            mappluto_path = os.path.join(root_directory, 'MapPLUTO', directory, 'shapefiles')
            if os.path.exists(mappluto_path) and os.listdir(mappluto_path) is not None:
                directory_years.append(directory)

    # Steps for exporting most recent MapPLUTO shapefile to temporary location
    target_dir = os.path.join(root_directory, 'MapPLUTO', directory_years[-1], 'shapefiles')
    for shapefile in os.listdir(target_dir):
        if 'MapPLUTO' in shapefile and 'UNCLIPPED' not in shapefile:
            if shapefile.endswith('.shp'):
                s3_file_title = shapefile.replace('MapPLUTO', 'mappluto_{}'.format(target_dir.split('\\')[4]))
                print('Copying - {} to S3 directory'.format(os.path.join(target_dir, shapefile).split('\\')[-1]))
                arcpy.Copy_management(os.path.join(target_dir, shapefile),
                                     os.path.join(r'C:\temp\s3_upload\{}'.format(s3_file_title)))
                print('Repairing geometry for - {}'.format(os.path.join(target_dir, shapefile).split('\\')[-1]))
                arcpy.RepairGeometry_management(os.path.join(os.path.join(temp_path, '{}'.format(s3_file_title))))

    # Define functioning for exporting metadata xmls from SDE sources
    def export_metadata(fc, desired_dict):
        arcpy.ExportMetadata_conversion(os.path.join(sde_path, fc),
                                        translator,
                                        os.path.join(temp_path, 'metadata', '{}.xml'.format(fc)))
        tree = ET.parse(os.path.join(temp_path, 'metadata', '{}.xml'.format(fc)))
        root = tree.getroot()
        for pubdate in root.iter('pubdate'):
            desired_dict[fc] = pubdate.text

    # Steps for exporting most recent LPC dataset directory
    desired_lpc_feature_classes = {'LPC_Historic_Districts': None, 'LPC_Individual_Landmarks': None, 'LPC_Scenic_Landmarks': None}
    for fc in desired_lpc_feature_classes.keys():
        export_metadata(fc, desired_lpc_feature_classes)
        if fc == 'LPC_Historic_Districts':
            print('Copying LPC_Historic_Districts to temporary directory')
            arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc),
                                                        temp_path,
                                                        'historic_districts_lpc_v{}'.format(desired_lpc_feature_classes[fc]))
            print('Repairing LPC_Historic_Districts')
            arcpy.RepairGeometry_management(os.path.join(temp_path, 'historic_districts_lpc_v{}.shp'.format(desired_lpc_feature_classes[fc])))
        if fc == 'LPC_Individual_Landmarks':
            print('Copying LPC_Individual_Landmarks to temporary directory')
            arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc),
                                                        temp_path,
                                                        'individual_landmarks_lpc_v{}'.format(desired_lpc_feature_classes[fc]))
            print('Repairing LPC_Individual_Landmarks')
            arcpy.RepairGeometry_management(os.path.join(temp_path, 'individual_landmarks_lpc_v{}.shp'.format(desired_lpc_feature_classes[fc])))
        if fc == 'LPC_Scenic_Landmarks':
            print('Copying LPC_Scenic_Landmarks to temporary directory')
            arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc),
                                                        temp_path,
                                                        'scenic_landmarks_lpc_v{}'.format(desired_lpc_feature_classes[fc]))
            print('Repairing LPC_Scenic_Landmarks')
            arcpy.RepairGeometry_management(os.path.join(temp_path, 'scenic_landmarks_lpc_v{}.shp'.format(desired_lpc_feature_classes[fc])))

    # Steps for targeting most recent Coastal Zone Boundary dataset
    desired_coastal_feature_class = {'DCP_WF_WRP_CZB': None}

    for fc in desired_coastal_feature_class.keys():
        export_metadata(fc, desired_coastal_feature_class)
        print('Copying Coastal Zone Boundary to temporary directory')
        arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc), temp_path,
                                                    'coastal_zone_boundary_v{}'.format(
                                                        desired_coastal_feature_class[fc]))
        print('Repairing Coastal Zone Boundary')
        arcpy.RepairGeometry_management(
            os.path.join(temp_path, 'coastal_zone_boundary_v{}.shp'.format(desired_coastal_feature_class[fc])))

    # Steps for targeting most recent Lower Density Growth Management Areas (LDGMA) dataset
    desired_ldgma_feature_class = {'DCP_PC_nyldgma': None}

    for fc in desired_ldgma_feature_class.keys():
        export_metadata(fc, desired_ldgma_feature_class)
        print(desired_ldgma_feature_class[fc])
        print("Copying LDGMA to temporary directory")
        arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc), temp_path,
                                                    'ldgma_v{}'.format(desired_ldgma_feature_class[fc]))
        print("Repairing LDGMA")
        arcpy.RepairGeometry_management(
            os.path.join(temp_path, 'ldgma_v{}.shp'.format(desired_ldgma_feature_class[fc])))

    # Steps for targeting most recent Upland Waterfront Area dataset
    desired_upland_water_area_feature_class = {'DCP_WF_nyuwa': None}

    for fc in desired_upland_water_area_feature_class.keys():
        export_metadata(fc, desired_upland_water_area_feature_class)
        print(desired_upland_water_area_feature_class[fc])
        print("Copying Upland Water Area to temporary directory")
        arcpy.FeatureClassToFeatureClass_conversion(os.path.join(sde_path, fc), temp_path,
                                                    'upland_waterfront_area_v{}'.format(desired_upland_water_area_feature_class[fc]))
        print("Repairing Upland Waterfront Area")
        arcpy.RepairGeometry_management(
            os.path.join(temp_path, 'upland_waterfront_area_v{}.shp'.format(desired_upland_water_area_feature_class[fc])))

    # Steps for targeting most recent DTM Block dataset directory
    gdb_dates = []
    arcpy.env.workspace = DTM_path
    for gdb in arcpy.ListWorkspaces(wild_card='*', workspace_type='FileGDB'):
        gdb_date = datetime.datetime.strptime(gdb.split('_')[-1].replace('.gdb', ''), '%Y%m%d').date()
        gdb_dates.append(gdb_date)
    current_gdb_date = max(gdb_dates)
    current_gdb_date_str = datetime.datetime.strftime(current_gdb_date, '%Y%m%d')

    DTM_Block_retain_fields = ['OID', 'BORO', 'BLOCK', 'Shape']
    DTM_Block_alter_fields = ['BORO', 'interim', 'BLOCK']

    # Steps for generating most recent DTM Block centroid feature class
    target_dir = os.path.join(DTM_path, 'export_{}.gdb'.format(current_gdb_date_str), 'Cadastral', 'Tax_Block_Polygon')
    print('Copying - {} to in memory location'.format(target_dir))
    arcpy.FeatureClassToFeatureClass_conversion(target_dir, 'in_memory', 'dtm_blocks')
    arcpy.FeatureToPoint_management(r'in_memory\dtm_blocks', r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str))
    DTM_Block_fc_existing_fields = [field.name for field in arcpy.ListFields(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str))]
    for field in DTM_Block_fc_existing_fields:
        if field not in DTM_Block_retain_fields:
            print('Deleting the following field from shapefile - {}'.format(field))
            arcpy.DeleteField_management(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str), field)
    DTM_Block_fc_existing_fields = [field.name for field in arcpy.ListFields(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str))]
    # Altering field names for block centroids data set.
    # Interim must be used for renaming BLOCK to block because of documented Arcpy bug
    for field in DTM_Block_fc_existing_fields:
        if field in DTM_Block_alter_fields:
            if field == 'BORO':
                print('Renaming BORO field to borocode')
                arcpy.AlterField_management(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str), field, field.replace('BORO', 'borocode'), field.replace('BORO', 'borocode'))
            if field == 'BLOCK':
                print('Renaming BLOCK to block')
                arcpy.AlterField_management(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str), field, field.replace('BLOCK', 'interim'), field.replace('BLOCK', 'interim'))
    DTM_Block_fc_existing_fields = [field.name for field in arcpy.ListFields(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str))]
    for field in DTM_Block_fc_existing_fields:
        if field == 'interim':
            arcpy.AlterField_management(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str), field, field.replace('interim', 'block'),
                                        field.replace('interim', 'block'))
    arcpy.FeatureClassToShapefile_conversion(r'in_memory\dtm_block_centroids_v{}'.format(current_gdb_date_str), temp_path)

    # Define function for exporting DTM files
    def export_DTM_files(dtm_file_name, dtm_feature_dataset, latest_export_str):
        target_file = os.path.join(DTM_path, 'export_{}.gdb'.format(latest_export_str), dtm_feature_dataset,
                                 dtm_file_name)
        if dtm_file_name is not 'Condo':
            arcpy.FeatureClassToFeatureClass_conversion(target_file, temp_path, '{}_v{}'.format(dtm_file_name.lower(), latest_export_str))
            arcpy.RepairGeometry_management(os.path.join(os.path.join(temp_path, '{}_v{}.shp'.format(dtm_file_name.lower(), latest_export_str))))
        else:
            arcpy.TableToTable_conversion(target_file, temp_path, '{}_v{}.csv'.format(dtm_file_name.lower(), latest_export_str))

    # Steps for exporting Tax Lot Polygon, Shoreline Polygon, and Condo table

    # Export Tax Lot Polygon FC
    print('Copying Tax Lot Polygon to s3 directory')
    export_DTM_files('Tax_Lot_Polygon', 'Cadastral', current_gdb_date_str)

    # Export Shoreline Polygon FC
    print('Copying Shoreline Polygon to s3 directory')
    export_DTM_files('Shoreline_Polygon', 'DCP', current_gdb_date_str)

    # Export Condo table
    print('Copying Condo to s3 directory')
    export_DTM_files('Condo', '', current_gdb_date_str)

    # Steps for targeting most recent data set directories in Bytes Production
    directory_years = []
    sub_dir_dates = []
    for directory in dcp_dataset_directories:
        dcp_dataset_path = os.path.join(root_directory, directory)
        for year_dir in os.listdir(dcp_dataset_path):
            if len(str(year_dir)) == 4 and year_dir[0].isdigit():
                year_dir = datetime.datetime.strptime(year_dir, '%Y')
                directory_years.append(year_dir.date().year)
        current_year = str(max(directory_years))
        dcp_current_dataset_path = os.path.join(root_directory, directory, current_year)
        for sub_dir in os.listdir(dcp_current_dataset_path):
            if directory in long_subdir_dates:
                sub_dir_date = datetime.datetime.strptime(sub_dir, '%Y%m%d')
                sub_dir_dates.append(sub_dir_date)
            if directory == 'Zoning':
                sub_dir_date = datetime.datetime.strptime(sub_dir, '%m%d%y')
                sub_dir_dates.append(sub_dir_date)
        current_sub_dir_date = max(sub_dir_dates)
        # Targeting data sets with long form directory date naming convention
        if directory in long_subdir_dates:
            current_sub_dir_str = datetime.datetime.strftime(current_sub_dir_date, '%Y%m%d')
            current_sub_dir_shp_path = os.path.join(dcp_dataset_path, current_year, current_sub_dir_str, 'shp')
            for file in os.listdir(current_sub_dir_shp_path):
                if not file.endswith('.pdf'):
                    # Targeting MIH data set
                    if 'nycmih' in file:
                        print('Copying - {} to S3 directory'.format(file))
                        file_no_ext = file.split('.')[0]
                        file_short_date = file_no_ext
                        s3_file_title = file.replace(file_no_ext, file_short_date)
                        s3_file_title = s3_file_title.replace('nycmih_', 'mandatory_inclusionary_housing_v')
                        print(s3_file_title)
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(s3_file_title)))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Environmental Designations data set
                    if 'nyedes' in file:
                        print('Copying - {} to S3 directory'.format(file))
                        file_no_ext = file.split('.')[0]
                        file_short_date = file_no_ext + '01'
                        s3_file_title = file.replace(file_no_ext, file_short_date)
                        s3_file_title = s3_file_title.replace('nyedes_', 'e_designations_v')
                        print(s3_file_title)
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(s3_file_title)))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Inclusionary Housing Designated Areas data set
                    if 'nycidha' in file:
                        if file.endswith('.shp'):
                            print('Copying - {} to S3 directory'.format(file))
                            file_no_ext = file.split('.')[0]
                            file_short_date = file_no_ext
                            s3_file_title = file.replace(file_no_ext, file_short_date)
                            s3_file_title = s3_file_title.replace('nycidha_', 'inclusionary_housing_v')
                            print(s3_file_title)
                            arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                                  os.path.join(temp_path, '{}'.format(s3_file_title)))
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Waterfront Access Plan data set
                    if 'nywap' in file:
                        if file.endswith('.shp'):
                            print('Copying - {} to S3 directory'.format(file))
                            file_no_ext = file.split('.')[0]
                            file_short_date = file_no_ext
                            s3_file_title = file.replace(file_no_ext, file_short_date)
                            s3_file_title = s3_file_title.replace('nywap_', 'waterfront_access_plan_v')
                            print(s3_file_title)
                            arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                                  os.path.join(temp_path, '{}'.format(s3_file_title)))
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting FRESH data set
                    if 'nycfreshzoning' in file:
                        if file.endswith('.shp'):
                            print('Copying - {} to S3 directory'.format(file))
                            file_no_ext = file.split('.')[0]
                            file_short_date = file_no_ext
                            s3_file_title = file.replace(file_no_ext, file_short_date)
                            s3_file_title = s3_file_title.replace('nycfreshzoning_', 'fresh_zones_v')
                            print(s3_file_title)
                            arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                                  os.path.join(temp_path, '{}'.format(s3_file_title)))
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Designated Areas in Manufacturing Districts (Appendix J)
                    if 'Designated_Areas_M_districts_AppendixJ' in file:
                        if file.endswith('.shp'):
                            print('Copying - {} to S3 directory'.format(file))
                            file_no_ext = file.split('.')[0]
                            print(file_no_ext)
                            file_short_date = file_no_ext
                            s3_file_title = file.replace(file_no_ext, file_short_date)
                            print(s3_file_title)
                            s3_file_title = s3_file_title.replace('Designated_Areas_M_districts_AppendixJ_', 'appendixj_designated_mdistricts_v')
                            print(s3_file_title)
                            arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                                  os.path.join(temp_path, '{}'.format(s3_file_title)))
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Transit Zones data set
                    if 'nyctransitzones' in file:
                        if file.endswith('.shp'):
                            print('Copying - {} to S3 directory'.format(file))
                            file_no_ext = file.split('.')[0]
                            file_ext = file.split('.')[1]
                            file_short_date = file_no_ext[-6:] + '01'
                            s3_file_title = 'transitzones_v{}.{}'.format(file_short_date, file_ext)
                            arcpy.FeatureClassToFeatureClass_conversion(os.path.join(current_sub_dir_shp_path, file),
                                                                        temp_path, s3_file_title)
                            print("Copied - {}".format(s3_file_title))
                            if file.endswith('.shp'):
                                print('Repairing - {}'.format(file))
                                arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
        else:
            # Targeting data sets with short form directory date naming convention
            current_sub_dir_str = datetime.datetime.strftime(current_sub_dir_date, '%m%d%y')
            current_sub_dir_shp_path = os.path.join(dcp_dataset_path, current_year, current_sub_dir_str, 'shp')
            for file in os.listdir(current_sub_dir_shp_path):
                if not file.endswith('.pdf'):
                    # Targeting Commercial Overlays data set
                    if 'nyco' in file:
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nyco', 'commercial_overlays_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Limited Height districts data set
                    if 'nylh' in file:
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nylh', 'limited_height_districts_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Special Purpose districts data set
                    if 'nysp' in file and '_sd' not in file:
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nysp', 'special_purpose_districts_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Special Purpose Sub-districts data set
                    if 'nysp_sd' in file:
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nysp_sd', 'special_purpose_subdistricts_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Zoning Districts data set
                    if 'nyzd' in file:
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nyzd', 'zoning_districts_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Zoning Map Amendment data set
                    if 'nyzma' in file and file.endswith('.shp'):
                        current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                        s3_file_title = file.replace('nyzma', 'zoning_map_amendments_v{}'.format(current_sub_dir_dt))
                        arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                              os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                        if file.endswith('.shp'):
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
                    # Targeting Sidewalk Cafe data set
                    if 'nysidewalkcafe' in file:
                        if file.endswith('.shp'):
                            current_sub_dir_dt = '20' + current_sub_dir_str[-2:] + current_sub_dir_str[:4]
                            s3_file_title = file.replace('nysidewalkcafe', 'sidewalk_cafes_v{}'.format(current_sub_dir_dt))
                            arcpy.Copy_management(os.path.join(current_sub_dir_shp_path, file),
                                                  os.path.join(temp_path, '{}'.format(file.replace(file, s3_file_title))))
                            print('Repairing - {}'.format(file))
                            arcpy.RepairGeometry_management(os.path.join(temp_path, '{}'.format(s3_file_title)))
        sub_dir_dates = []
        directory_years = []

    # Remove all directories from export location
    if os.path.exists(os.path.join(temp_path, 'metadata')):
        shutil.rmtree(os.path.join(temp_path, 'metadata'))
    if os.path.exists(os.path.join(temp_path, 'info')):
        shutil.rmtree(os.path.join(temp_path, 'info'))

    # Replace previous cycle's zip files
    for f in os.listdir(temp_path):
        if f.endswith('.zip'):
            print('Removing previous zip: {}'.format(f))
            os.remove(os.path.join(temp_path, f))

    dataset_dictionary = {}

    # Parse filenames for data title, file extension, and data set version to zip and export re-named shapefiles
    for f in os.listdir(temp_path):
        if f.endswith('.xml') and 'condo' in f or f.endswith('.ini') or f.endswith('.txt'):
            print('Removing previous zip: {}'.format(f))
            os.remove(os.path.join(temp_path, f))
            continue
        if 'mappluto' not in f:
            data_title = f.replace('_{}'.format(f.split('_')[-1]), '')
            f_ext = f.split('.', 1)[-1]
            f_version = f.split('_')[-1].split('.', 1)[0]
            print("Zipping: {}, {}".format(data_title, f_ext))
            f_version = f_version.replace('_', '')
            f_version = f_version.replace('v', '')
            dataset_dictionary[data_title] = f_version

        else:
            data_title = f.split('_', 1)[0]
            f_ext = f.split('.', 1)[-1]
            print("Zipping: {}, {}".format(data_title, f_ext))
            f_version = f.split('_', 1)[-1].split('.', 1)[0]
            dataset_dictionary[data_title] = '{}'.format(f_version)

        if os.path.exists(os.path.join(temp_path, '{}.zip'.format(data_title))):
            zip.write(os.path.join(temp_path, f), '{}.{}'.format(data_title, f_ext),
                      compress_type= zipfile.ZIP_DEFLATED)
        if not os.path.exists(os.path.join(temp_path, '{}.zip'.format(data_title))):
            zip = zipfile.ZipFile(os.path.join(temp_path, '{}.zip'.format(data_title)), 'w')
            zip.write(os.path.join(temp_path, f), '{}.{}'.format(data_title, f_ext),
                      compress_type= zipfile.ZIP_DEFLATED)
    zip.close()

    # Create list of available dataset zips in temp directory for comparison with datasets on s3
    zip_datasets = []

    for f in os.listdir(temp_path):
        if f.endswith('.zip'):
            zip_datasets.append(f)

    # Create list of grouped data sets for structuring of keys in bucket
    print('Data sets to be uploaded - {}'.format(zip_datasets))

    zoning_files = ['commercial_overlays', 'special_purpose_districts', 'special_purpose_subdistricts',
                    'limited_height_districts', 'sidewalk_cafes', 'zoning_districts', 'zoning_map_amendments']

    lpc_files = ['historic_districts_lpc', 'individual_landmarks_lpc', 'scenic_landmarks_lpc']

    dtm_files = ['condo', 'dtm_block_centroids', 'shoreline_polygon', 'tax_lot_polygon']

    mappluto_files = ['mappluto']

    # Assigning connection credentials based on initialization file inputs

    s3_client = boto3.client('s3',
                        region_name= config.get('CREDENTIALS', 'region_name'),
                        endpoint_url= config.get('CREDENTIALS', 'endpoint_url'),
                        aws_access_key_id= config.get('CREDENTIALS', 'aws_access_key_id'),
                        aws_secret_access_key= config.get('CREDENTIALS', 'aws_secret_access_key'))

    s3_resource = boto3.resource('s3',
                        region_name= config.get('CREDENTIALS', 'region_name'),
                        endpoint_url= config.get('CREDENTIALS', 'endpoint_url'),
                        aws_access_key_id= config.get('CREDENTIALS', 'aws_access_key_id'),
                        aws_secret_access_key= config.get('CREDENTIALS', 'aws_secret_access_key'))


    # Define s3 bucket we wish to upload zip files to (edm-storage)

    print('Defining selected Bucket')
    selected_bucket = 'edm-storage'

    # Upload zipped data sets to bucket keys with specified naming conventions based on previously assigned grouping lists

    for z in zip_datasets:
        data_title = z.split('.')[0]
        f_version = dataset_dictionary[data_title]
        # Uploading Zoning data sets
        if data_title in zoning_files:
            zoning_path = r'ZONING_FEATURES/{0}/{1}/{2}'.format(data_title, f_version, z)
            with open(os.path.join(temp_path, '{}'.format(z)), 'rb') as f:
                print("Adding: {} to s3 bucket".format(zoning_path))
                s3_client.upload_fileobj(f, selected_bucket, zoning_path, ExtraArgs={'ACL':'public-read'})
        # Uploading Landmark Preservation Commission data sets
        if data_title in lpc_files:
            lpc_path = r'LPC/{0}/{1}/{2}'.format(data_title, f_version, z)
            with open(os.path.join(temp_path, '{}'.format(z)), 'rb') as f:
                print("Adding: {} to s3 bucket".format(lpc_path))
                s3_client.upload_fileobj(f, selected_bucket, lpc_path, ExtraArgs={'ACL':'public-read'})
        # Uploading DTM data sets
        if data_title in dtm_files:
            dtm_path = r'DTM/{0}/{1}/{2}'.format(data_title, f_version, z)
            with open(os.path.join(temp_path, '{}'.format(z)), 'rb') as f:
                print("Adding: {} to s3 bucket".format(dtm_path))
                s3_client.upload_fileobj(f, selected_bucket, dtm_path, ExtraArgs={'ACL':'public-read'})
        # Uploading remaining data sets that are not associated with DTM, LPC, or Zoning
        if data_title not in zoning_files and data_title not in lpc_files and data_title not in dtm_files:
            data_path = r'{0}/{1}/{2}'.format(data_title.upper(), f_version, z)
            with open(os.path.join(temp_path, '{}'.format(z)), 'rb') as f:
                print("Adding: {}".format(data_path))
                s3_client.upload_fileobj(f, selected_bucket, data_path, ExtraArgs={'ACL':'public-read'})

    # Print bucket keys after uploads to allow for review
    edm_storage_bucket = s3_resource.Bucket(selected_bucket)

    for f in edm_storage_bucket.objects.all():
        print(f.key)

    # Write start, end, and total times to log file
    EndTime = datetime.datetime.now().replace(microsecond=0)
    print('Script runtime: {}'.format(EndTime - StartTime))
    log.write(str(StartTime) + '\t' + str(EndTime) + '\t' + str(EndTime - StartTime) + '\n')
    log.close()

except:
    # If error encountered, print Python and ArcPy error messages
    print('error')
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    pymsg = 'PYTHON ERRORS:\nTraceback Info:\n' + tbinfo + '\nError Info:\n' + str(sys.exc_info()[1])
    msgs = 'ArcPy ERRORS:\n' + arcpy.GetMessages() + '\n'

    print(pymsg)
    print(msgs)

    log.write('' + pymsg + '\n')
    log.write('' + msgs + '')
    log.write('\n')
    log.close()
