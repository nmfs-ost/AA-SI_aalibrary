from loguru import logger
import echopype as ep
from itertools import chain, combinations
import itertools

class FrequencyData():
    """Given some dataset 'Sv', list all frequencies available. This class offers methods which help map out frequencies and channels plus additional utilities. 
    """   
    
    def __init__(self, Sv):
        """Initializes class object and parses the frequencies available within the echdata object (xarray.Dataset) 'Sv'.

        Args:
            Sv (xarray.Dataset): The 'Sv' echodata object.
        """
        
        self.Sv = Sv # Crreate a self object.
        self.frequency_list = [] # Declares a frequency list to be modified.
        
        self.construct_frequency_list() # Construct the frequency list.
        #TODO : This string needs cleaning up ; remove unneeded commas and empty tuples.
        self.frequency_set_combination_list = self.construct_frequency_set_combination_list() # Constructs a list of available frequency set permutations. Example : [('18 kHz',), ('38 kHz',), ('120 kHz',), ('200 kHz',), ('18 kHz', '38 kHz'), ('18 kHz', '120 kHz'), ('18 kHz', '200 kHz'), ('38 kHz', '120 kHz'), ('38 kHz', '200 kHz'), ('120 kHz', '200 kHz'), ('18 kHz', '38 kHz', '120 kHz'), ('18 kHz', '38 kHz', '200 kHz'), ('18 kHz', '120 kHz', '200 kHz'), ('38 kHz', '120 kHz', '200 kHz'), ('18 kHz', '38 kHz', '120 kHz', '200 kHz')]
        # print(self.frequency_set_combination_list)
        self.frequency_pair_combination_list = self.construct_frequency_pair_combination_list() # Constructs a list of all possible unequal permutation pairs of frequencies. Example : [('18 kHz', '38 kHz'), ('18 kHz', '120 kHz'), ('18 kHz', '200 kHz'), ('38 kHz', '120 kHz'), ('38 kHz', '200 kHz'), ('120 kHz', '200 kHz')] 
        # print(self.frequency_pair_combination_list)
        self.construct_frequency_map()
    
        
    def construct_frequency_list(self):
        """Parses the frequencies available in the xarray 'Sv'
        """
        for i in range(len(self.Sv.Sv)): # Iterate through the natural index associated with Sv.Sv .
            
            self.frequency_list.append(str(self.Sv.Sv[i].coords.get("channel")).split(" kHz")[0].split("GPT")[1].strip()+" kHz") # Extract frequency.
        logger.debug(f"Constructed frequency list: {self.frequency_list}") # Log the constructed frequency list.    
        return self.frequency_list # Return string array frequency list of the form [18kHz, 70kHz, 200 kHz]
        
        
    def powerset(self, iterable):
        """Generates combinations of elements of iterables ; powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)

        Args:
            iterable (_type_): A list.

        Returns:
            _type_: Returns combinations of elements of iterables.
        """        
        
        s = list(iterable) # Make a list from the iterable.
        return chain.from_iterable(combinations(s, r) for r in range(len(s)+1)) # Returns a list of tuple elements containing combinations of elements which derived from the iterable object.


    def construct_frequency_set_combination_list(self):
        """Constructs a list of available frequency set permutations. 
        Example : [
            ('18 kHz',), ('38 kHz',), ('120 kHz',), ('200 kHz',), ('18 kHz', '38 kHz'),
            ('18 kHz', '120 kHz'), ('18 kHz', '200 kHz'), ('38 kHz', '120 kHz'), ('38 kHz', '200 kHz'),
            ('120 kHz', '200 kHz'), ('18 kHz', '38 kHz', '120 kHz'), ('18 kHz', '38 kHz', '200 kHz'),
            ('18 kHz', '120 kHz', '200 kHz'), ('38 kHz', '120 kHz', '200 kHz'),
            ('18 kHz', '38 kHz', '120 kHz', '200 kHz')
            ]


        Returns:
            list<tuple>: A list of tuple elements containing frequency combinations which is useful for the KMeansOperator class.
        """  
              
        return list(self.powerset(self.frequency_list)) # Returns a list of tuple elements containing frequency combinations which is useful for the KMeansOperator class.
    
    
    def print_frequency_set_combination_list(self):
        """Prints frequency combination list one element at a time.
        """
                
        for i in self.frequency_set_combination_list:  # For each frequency combination associated with Sv.
            print(i) # Print out frequency combination tuple.
            
            
    def construct_frequency_pair_combination_list(self):
        """Returns a list of tuple elements containing frequency combinations which is useful for the KMeansOperator class.

        Returns:
            list<tuple>: A list of tuple elements containing frequency combinations which is useful for the KMeansOperator class.
        """  
              
        return list(itertools.combinations(self.frequency_list, 2)) # Returns a list of tuple elements containing frequency combinations which is useful for the KMeansOperator class.
    
    def print_frequency_pair_combination_list(self):
        """Prints frequency combination list one element at a time.
        """
                
        for i in self.frequency_pair_combination_list:  # For each frequency combination associated with Sv.
            print(i) # Print out frequency combination tuple.
            

    def print_frequency_list(self):
        """Prints each frequency element available in Sv.
        """        
        
        for i in self.frequency_list:# For each frequency in the frequency_list associated with Sv.
            print(i) # Print out the associated frequency.
            
            
    def construct_frequency_map(self, frequencies_provided = True):
        """Either using a channel_list or a frequency_list this function provides one which satisfies all requirements of this class structure. In particular the channels and frequencies involved have to be known and mapped to oneanother.

        Args:
            frequencies_provided (boolean): was a frequency_list provided at object creation? If so then 'True' if a channel_list instead was used then 'False'.
        """        
        if frequencies_provided == True:
            self.simple_frequency_list = self.frequency_list
            self.frequency_map = [] # Declare a frequency map to be populated with string frequencies of the form [[1,'38kHz'],[2,'120kHz'],[4,'200kHz']] where the first element is meant to be the channel representing the frequency. This is an internal object. Do not interfere.
            for j in self.simple_frequency_list: # For each frequency 'j'.
                for i in range(len(self.Sv.Sv)): # Check each channel 'i'.
                    
                    channel_desc = str(self.Sv.Sv[i].coords.get("channel"))
                    
                    if "ES" in channel_desc: # If the channel description contains "ES" then it is an ES channel.
                        numeric_frequency_desc = str(self.Sv.Sv[i].coords.get("channel")).split("ES")[1].split("-")[0].strip()
                        if numeric_frequency_desc == j.split("kHz")[0].strip():
                            self.frequency_map.append([i,numeric_frequency_desc+" kHz"])
                    
                    
                    if "GPT" in channel_desc: # If the channel description contains "GPT" then it is a GPT channel.
                        numeric_frequency_desc = str(self.Sv.Sv[i].coords.get("channel")).split(" kHz")[0].split("GPT")[1].strip()
                        if numeric_frequency_desc == j.split("kHz")[0].strip(): # To see if the channel associates with the frequency 'j' .
                            self.frequency_map.append([i,numeric_frequency_desc+" kHz"]) # If so append it and the channel to the 'frequency_list'.
        else:
            
            channel_desc = str(self.Sv.Sv[i].coords.get("channel"))
            
            if "ES" in channel_desc: # If the channel description contains "ES" then it is an ES channel.
                for i in self.channel_list:
                    self.frequency_map.append([i,str(self.Sv.Sv[i].coords.get("channel")).split(" kHz")[0].split("ES")[1].strip()+" kHz"])
                    
            if "GPT" in channel_desc: # If the channel description contains "ES" then it is an ES channel.        
                for i in self.channel_list:
                    self.frequency_map.append([i,str(self.Sv.Sv[i].coords.get("channel")).split(" kHz")[0].split("GPT")[1].strip()+" kHz"])
                    
        
        
        # Remove duplicates from frequency_list.
        
        
        self.frequency_map = [list(t) for t in set(tuple(item) for item in self.frequency_map)]
        
        
        
           
            
            

def main():

    input_path = "/home/mryan/Desktop/HB1603_L1-D20160707-T190150.nc"
    ed = ep.open_converted(input_path)
    Sv = ep.calibrate.compute_Sv(ed)
    

    freq_data = FrequencyData(Sv)
    logger.debug(freq_data.frequency_map)



if __name__ == "__main__":
    main()
