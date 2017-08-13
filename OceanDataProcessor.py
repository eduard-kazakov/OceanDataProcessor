# -*- coding: utf-8 -*-

from __future__ import print_function
import time

class OceanDataProcessor():
    
    NODATA = '-999'
    DEPTH_LIMIT = 50
    supported_data_types = ['WOD']

    def __init__(self, data_type, data_source, delimeter, year_min, year_max, lat_min, lat_max, lon_min, lon_max):
        if data_type not in self.supported_data_types:
            print ('Data type is not supported')
            return -1
            
        self.data_source = data_source
        self.data_type = data_type
        self.delimeter = delimeter
        self.year_min = year_min
        self.year_max = year_max
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
        
    ######### Parsers
    
    def list_to_line (self, input_list, delimeter):
        line = ''
        for element in input_list:
            line = line + str(element) + delimeter
        return line[:-len(delimeter)]
    
    def WOD_station_to_lines (self, WOD_station, uniqie_variables, delimeter):
        #print ('asd'    )
        
        depth_index = WOD_station['variables'].index('Depth')
        lines_array = []
        if depth_index >= 0:
            for measurement in WOD_station['measurements']:
                if float(measurement[depth_index]) > self.DEPTH_LIMIT:
                    continue
                current_line_array = [WOD_station['name'],WOD_station['lat'],WOD_station['lon'],WOD_station['year'],WOD_station['month'],WOD_station['day']]
                for unique_variable in uniqie_variables:
                    current_line_array.append(self.NODATA)
                i = 0
                while i < len(measurement):
                    current_parameter_name = WOD_station['variables'][i]
                    current_parameter_index = uniqie_variables.index(current_parameter_name)
                    if measurement[i].isdigit():
                        current_line_array[current_parameter_index + 6] = measurement[i]
                    else:
                        current_line_array[current_parameter_index + 6] = measurement[i]
                    i += 1
                lines_array.append(self.list_to_line(current_line_array,delimeter))

            return lines_array
                
    def parse_WOD_point_data(self, data_source):
        input_data = open(data_source,'r').readlines()
        station_parsing = False
        stations = []
        unique_variables = []
        station_number = 0        
        current_station = {'name':'','lat':'','lon':'','year':'','month':'','day':'','variables':[],'measurements':[]}
        for line in input_data:
            if station_parsing:
                current_line = line.replace(' ','').split(self.delimeter)

                if current_line[0] == 'NODCCruiseID':
                    current_station['name'] = current_line[2]
                
                if current_line[0] == 'Latitude':
                    current_station['lat'] = current_line[2]

                if current_line[0] == 'Longitude':
                    current_station['lon'] = current_line[2]

                if current_line[0] == 'Year':
                    current_station['year'] = current_line[2]

                if current_line[0] == 'Month':
                    current_station['month'] = current_line[2]

                if current_line[0] == 'Day':
                    current_station['day'] = current_line[2]

                if current_line[0] == 'VARIABLES':
                    i = 0
                    while i < len(current_line)-1:
                        if ((i+2) % 3) == 0:
                            if current_line[i]:
                                current_station['variables'].append(current_line[i])
                                if current_line[i] not in unique_variables:
                                    unique_variables.append(current_line[i])
                        i += 1
                
                if current_line[0].isdigit():
                    i = 0
                    current_measurement = []
                    while i < len(current_line)-1:
                        if ((i+2) % 3) == 0:
                            current_measurement.append(current_line[i])
                        i += 1
                    current_station['measurements'].append(current_measurement)
                    

            if line.startswith('#'):
                if station_parsing:
                    if (float(current_station['year']) >= self.year_min) and (float(current_station['year']) <= self.year_max):
                        if (float(current_station['lat']) >= self.lat_min) and (float(current_station['lat']) <= self.lat_max):
                            if (float(current_station['lon']) >= self.lon_min) and (float(current_station['lon']) <= self.lon_max):
                                station_number += 1
                                print('Processing raw record #%s' % str(station_number), end='\r')
                                stations.append(current_station)
                    
                current_station = {'name':'','lat':'','lon':'','year':'','month':'','day':'','variables':[],'measurements':[]}
                    
                station_parsing = True
                
        print ('')
        return {'stations':stations,'unique_variables':unique_variables}
    #################
        
        
        
    def parse(self):
        if self.data_type == 'WOD':
            dataset = self.parse_WOD_point_data(self.data_source)
            return dataset
        
    
    def parse_and_write_to_file(self):
        pass
    
    def write_to_file(self, dataset, output_file):
        if self.data_type == 'WOD':
            #dataset = self.parse_WOD_point_data(self.data_source)
            #return dataset
            output = open(output_file,'w')
            unique_variables = dataset['unique_variables']
            title_line = 'NAME' + self.delimeter + 'LAT' + self.delimeter + 'LON' + self.delimeter + 'YEAR' + self.delimeter + 'MONTH' + self.delimeter + 'DAY' + self.delimeter
            for unique_variable in unique_variables:
                title_line += unique_variable + self.delimeter
            
            output.write(title_line + '\n')
            station_number = 0
            for station in dataset['stations']:
                station_number += 1
                print('Writing station #%s' % str(station_number), end='\r')
                lines_array = self.WOD_station_to_lines(station,unique_variables,self.delimeter)
                for line in lines_array:
                    output.write(line+'\n')
    
            output.close()
    
start_time = time.time()
WOD_sample = OceanDataProcessor('WOD','E:/OCOLOR/data_examples/point_data_samples/ocldb1490697552.28643.APB.csv',',',1997, 2018, 40, 90, -180, 180)
var = WOD_sample.parse()
WOD_sample.write_to_file(var,'E:/OCOLOR/data_examples/point_data_samples/ocldb1490697552.28643.APB_processed.csv')
print("--- Execution time: %s seconds ---" % (time.time() - start_time))
