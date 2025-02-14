from netCDF4 import Dataset
import pprint

def get_netcdf_header(file_path: str) -> dict:
    """
    Reads a NetCDF file and returns its header as a dictionary.

    :param file_path: Path to the NetCDF file.
    :return: Dictionary containing global attributes, dimensions, and variables.
    """
    header_info = {}
    
    with Dataset(file_path, 'r') as nc_file:
        # Extract global attributes
        header_info['global_attributes'] = {attr: getattr(nc_file, attr) for attr in nc_file.ncattrs()}
        
        # Extract dimensions
        header_info['dimensions'] = {dim: len(nc_file.dimensions[dim]) for dim in nc_file.dimensions}
        
        # Extract variable metadata
        header_info['variables'] = {
            var: {
                'dimensions': nc_file.variables[var].dimensions,
                'shape': nc_file.variables[var].shape,
                'dtype': str(nc_file.variables[var].dtype),
                'attributes': {attr: getattr(nc_file.variables[var], attr) for attr in nc_file.variables[var].ncattrs()}
            }
            for var in nc_file.variables
        }
    
    return header_info



# Example usage :
if __name__ == "__main__":
    pprint.pprint(get_netcdf_header('/home/mryan/Downloads/haddock_detections_NEFSC_SBNMS_200601_CH3_newpath.nc'))