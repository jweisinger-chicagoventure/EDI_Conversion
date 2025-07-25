#step 1: read EDI file as a string.
import os
import sys
import json
import re
import logging
from datetime import date, time
# edi_file_name = '186b6f7b542b40859e3fcac497869c3e_20250718145022_10_608_1752850251187'
logger = logging.getLogger(__name__)
edi_file_name = 'TEST'

with open('output.log', 'w') as f:
    f.write("")

edi_file_text = ''
with open(f'{edi_file_name}.EDI', 'r') as f:
    edi_file_text = f.read()

dict_output = {
    'Interchange_Control_Header' : {},
    'Functional_Group_Header': {},
    'Heading' :  {'N1_Loop': {}}, #{'Details' : {}, 'N1Loop': {}, 'PO1Loop': {}, 'CTLoop': {}}
    'Detail' : {'PO1_Loop': {}, 'PID_Loop':{}},
    'Summary' : {'CTT_Loop': {}}
}

valid_set = {'ISA', 'GS', 'ST', 'BEG', 'REF', 'PER', 'N1', 'N2', 'N3', 'N4', 'PO1', 'PID', 'CTT', 'SE', 'GE', 'IEA'}

#log the first exception (no match) 
#log all non-matches
# found_list = {}
segment_order = []
exceed_dict = dict()
segment_limits = {'ISA': 16, 'GS': 8, 'ST': 2, 'BEG': 5, 'REF': 2, 'PER': 4, 'N1': 4, 'N2': 1, 'N3': 1, 'N4': 4, 'PO1': 9, 'PID': 5, 'CTT': 1, 'SE': 2, 'GE': 2, 'IEA': 2}

edi_file_list_groups = edi_file_text.split('~')

# for i in range(1):
current_group = edi_file_list_groups[0]
edi_file_list = current_group.split('^')


current_group = edi_file_list_groups[1]
edi_file_list = current_group.split('^')


for i in range(len(edi_file_list_groups)):
    current_group = edi_file_list_groups[i]
    edi_file_list = current_group.split('^')
    unique_val = edi_file_list[0]

    segment_order.append(unique_val)
    match unique_val:
        case 'ISA':
            for j in range(len(edi_file_list)):
                match j:
                    case 1:
                        dict_output['Interchange_Control_Header'][f'Authorization_Information_Qualifier'] = edi_file_list[j]
                    case 2:
                        dict_output['Interchange_Control_Header'][f'Authorization_Information'] = edi_file_list[j]
                    case 3:
                        dict_output['Interchange_Control_Header'][f'Security_Information_Qualifier'] = edi_file_list[j]
                    case 4:
                        dict_output['Interchange_Control_Header'][f'Security_Information'] = edi_file_list[j]
                    case 5:
                        dict_output['Interchange_Control_Header'][f'InterchangeID_Sender_Qualifier'] = edi_file_list[j]
                    case 6:
                        dict_output['Interchange_Control_Header'][f'Sender_Id'] = edi_file_list[j]
                    case 7:
                        dict_output['Interchange_Control_Header'][f'Interchange_ID_Receiver_Qualifier'] = edi_file_list[j]
                    case 8:
                        dict_output['Interchange_Control_Header'][f'Receiver_Id'] = edi_file_list[j]
                    case 9:
                        dict_output['Interchange_Control_Header'][f'Interchange_Date'] = edi_file_list[j]
                    case 10:
                        dict_output['Interchange_Control_Header'][f'Interchange_Time'] = edi_file_list[j]
                    case 11:
                        dict_output['Interchange_Control_Header'][f'Interchange_Control_Standards_Identifier'] = edi_file_list[j]
                    case 12:
                        dict_output['Interchange_Control_Header'][f'Interchange_Control_Version_Number'] = edi_file_list[j]
                    case 13:
                        dict_output['Interchange_Control_Header'][f'Interchange_Control_Number'] = edi_file_list[j]
                    case 14:
                        dict_output['Interchange_Control_Header'][f'Acknowledgment_Requested'] = edi_file_list[j]
                    case 15:
                        dict_output['Interchange_Control_Header'][f'Usage_Indicator'] = edi_file_list[j]
                    case 16:
                        dict_output['Interchange_Control_Header'][f'Component_Element_Separator'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
        case 'GS':
            for j in range(len(edi_file_list)):
                match j:
                    case 1:
                        dict_output['Functional_Group_Header'][f'Functional_Identifier_Code'] = edi_file_list[j]
                    case 2:
                        dict_output['Functional_Group_Header'][f'Application_Senders_Code'] = edi_file_list[j]
                    case 3:
                        dict_output['Functional_Group_Header'][f'Application_Receivers_Code'] = edi_file_list[j]
                    case 4:
                        dict_output['Functional_Group_Header'][f'Date'] = edi_file_list[j]
                    case 5:
                        dict_output['Functional_Group_Header'][f'Time'] = edi_file_list[j]
                    case 6:
                        dict_output['Functional_Group_Header'][f'Group_Control_Number'] = edi_file_list[j]
                    case 7:
                        dict_output['Functional_Group_Header'][f'Responsible_Agency_Code'] = edi_file_list[j]
                    case 8:
                        dict_output['Functional_Group_Header'][f'Version/Release/Industry_Identifier_Code'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
        case 'ST':
            unique_val_convert = 'Transaction_Set_Header'
            dict_output['Heading'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:
                        dict_output['Heading'][unique_val_convert][f'Transaction_Set_Identifier_Code'] = edi_file_list[j]
                    case 2:
                        dict_output['Heading'][unique_val_convert][f'Transaction_Set_Control_Number'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
        case 'BEG':
            unique_val_convert = 'Beginning_Segment_for_Purchase_Order'
            dict_output['Heading'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:
                        dict_output['Heading'][unique_val_convert][f'Transaction_Set_Purpose_Code'] = edi_file_list[j]
                    case 2:
                        dict_output['Heading'][unique_val_convert][f'Purchase_Order_Type_Code'] = edi_file_list[j]
                    case 3:
                        dict_output['Heading'][unique_val_convert][f'Purchase_Order_Number'] = edi_file_list[j]
                    case 4:
                        dict_output['Heading'][unique_val_convert][f'Filler_01'] = edi_file_list[j]
                    case 5:
                        dict_output['Heading'][unique_val_convert][f'Date'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'REF':
            unique_val_convert = 'Reference_Identification'
            dict_output['Heading'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:
                        dict_output['Heading'][unique_val_convert][f'Reference_Identification_Qualifier'] = edi_file_list[j]
                    case 2:
                        dict_output['Heading'][unique_val_convert][f'Reference_Identification'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
                
        case 'PER':
            unique_val_convert = 'Administrative_Communications_Contract'
            dict_output['Heading'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Heading'][unique_val_convert][f'Contact_Function_Code'] = edi_file_list[j]
                    case 2:
                        dict_output['Heading'][unique_val_convert][f'Name'] = edi_file_list[j]
                    case 3:
                        dict_output['Heading'][unique_val_convert][f'Communication_Number_Qualifier'] = edi_file_list[j]
                    case 4:
                        dict_output['Heading'][unique_val_convert][f'Communication_Number'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        # case s if re.match(r"^N", unique_val):
            # dict_output['Groups']['Details'][unique_val] = {}
        case 'N1':
            unique_val_convert = 'Name'
            dict_output['Heading']['N1_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Entity_Identifier_Code'] = edi_file_list[j]
                    case 2:
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Name'] = edi_file_list[j]
                    case 3:
                        dict_output['Heading']['N1_Loop'][unique_val_convert]["Identification_Code_Qualifier"] = edi_file_list[j]
                    case 4:
                        dict_output['Heading']['N1_Loop'][unique_val_convert]["Identification_Code"] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
        case 'N2':
            unique_val_convert = 'Additional_Name_Information'
            dict_output['Heading']['N1_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Name'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'N3':
            unique_val_convert = 'Address_Information'
            dict_output['Heading']['N1_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Address_Information'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   
        case 'N4':
            unique_val_convert = 'Geographic_Location'
            dict_output['Heading']['N1_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'City_Name'] = edi_file_list[j]
                    case 2:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'State_or_Province_Code'] = edi_file_list[j]
                    case 3:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Postal_Code'] = edi_file_list[j]
                    case 4:  
                        dict_output['Heading']['N1_Loop'][unique_val_convert][f'Country_Code'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'PO1':
            unique_val_convert = 'Baseline_Item_Data'
            dict_output['Detail']['PO1_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Assigned_Identification'] = edi_file_list[j]
                    case 2:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Quantity_Ordered'] = edi_file_list[j]
                    case 3:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Unit_or_Basis_for_Measurement_Code'] = edi_file_list[j]
                    case 4:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Unit_Price'] = edi_file_list[j]
                    case 5:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Filler_01'] = edi_file_list[j]
                    case 6:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Basis_Product_Service_ID_Qualifier'] = edi_file_list[j]
                    case 7:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Basis_Product_Service_ID'] = edi_file_list[j]
                    case 8:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Product/ServiceID_Qualifier'] = edi_file_list[j]
                    case 9:  
                        dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Product/ServiceID'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'PID':
            unique_val_convert = 'Product/Item_Description'
            dict_output['Detail']['PID_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Detail']['PID_Loop'][unique_val_convert][f'Item_Description_Type'] = edi_file_list[j]
                    case 2:
                        dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_01'] = edi_file_list[j]
                    case 3:
                        dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_02'] = edi_file_list[j]
                    case 4:
                        dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_03'] = edi_file_list[j]
                    case 5:  
                        dict_output['Detail']['PID_Loop'][unique_val_convert][f'Description'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'CTT':
            unique_val_convert = 'Transaction_Totals'
            dict_output['Summary']['CTT_Loop'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Summary']['CTT_Loop'][unique_val_convert][f'Number_Of_Line_Items'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'SE':
            unique_val_convert = 'Transaction_Set_Trailer'
            dict_output['Summary'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Summary'][unique_val_convert][f'Number_Of_Included_Segments'] = edi_file_list[j]
                    case 2:  
                        dict_output['Summary'][unique_val_convert][f'Transaction_Set_Control_Number'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'GE':
            unique_val_convert = 'Functional_Group_Trailer'
            dict_output['Summary'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Summary'][unique_val_convert][f'Number_of_Transaction_Sets_Included'] = edi_file_list[j]
                    case 2:  
                        dict_output['Summary'][unique_val_convert][f'Group_Control_Number'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

        case 'IEA':
            unique_val_convert = 'Interchange_Control_Trailer'
            dict_output['Summary'][unique_val_convert] = {}
            for j in range(len(edi_file_list)):
                match j:
                    case 1:  
                        dict_output['Summary'][unique_val_convert][f'Number_of_Included_Functional_Groups'] = edi_file_list[j]
                    case 2:  
                        dict_output['Summary'][unique_val_convert][f'Interchange_Control_Number'] = edi_file_list[j]
                    case _:
                        if j != 0:
                            if unique_val not in exceed_dict.keys():
                                exceed_dict[unique_val] = 0
                            exceed_dict[unique_val] += 1   

###################################################
#Error handling -- Start by writing all to log file:
# console_output = sys.stdout
# with open('log.txt', 'w') as f:
#     sys.stdout = f
    ########
    #Ensuring correctness of segment markers and order.

    #most important - make sure all segments have valid names (otherwise, we could have unexpected errors when checking plcaement)

logging.basicConfig(filename = 'output.log', level=logging.INFO)
logger = logging.getLogger('output_log')
segments_valid = True
for segment in segment_order:
    if segment not in valid_set and segment != '':
            logger.info(f'{segment} does not exist in the current set of known segments and cannot be validated at this time.')
            segments_valid = False
            error_found = True

# if segments_valid:
    #check errors related to ordering, segment existence, and file end marker 
length_segment_order = len(segment_order)
for i, segment in enumerate(segment_order):
    if segment in valid_set:
        match i:
            case 0:
                if segment != 'ISA':
                    logger.error(f'The {i+1}st segment must be ISA.')
            case 1:
                if segment != 'GS':
                    logger.error(f'The {i+1}nd segment must be GS.')
            case 2:
                if segment != 'ST':
                    logger.error(f'The {i+1}rd segment must be ST.')
            case 3:
                if segment != 'BEG':
                    logger.error(f'The {i+1}th segment must be BEG.')
            case 4:
                if segment != 'REF':
                    logger.error(f'The {i+1}th segment must be REF.')
            case 5:
                if segment != 'PER':
                    logger.error(f'The {i+1}th segment must be PER.')
            case ctt if ctt == length_segment_order - 5 + int(segment_order[-1] != ''):
                if segment != 'CTT':
                    logger.error(f'The 4th-to-last segment must be CTT.')
            case se if se == length_segment_order - 4 + int(segment_order[-1] != ''):
                if segment != 'SE':
                    logger.error(f'The 3rd-to-last segment must be SE.')
            case ge if ge == length_segment_order - 3 + int(segment_order[-1] != ''):
                if segment != 'GE':
                    logger.error(f'The 2nd-to-last segment must be GE.')
            case iea if iea == length_segment_order - 2 + int(segment_order[-1] != ''):
                if segment != 'IEA':
                    logger.error(f'The last segment must be IEA.')
                if segment_order[-1] != '':
                    logger.error(f'The EDI file must end with a tilde.')

            case tilde if tilde == length_segment_order - 1:
                if segment != '':
                    logger.error(f'The EDI file must end with a tilde.')

#finding duplicates
segment_counts = dict()
for segment in segment_order:
    if segment in valid_set:
        if segment != '':
            if segment not in segment_counts.keys():
                segment_counts[segment] = 0
            segment_counts[segment] += 1

# print('test...')
# print(segment_counts)
for segment_key in segment_counts.keys():
    if segment_key[0] != 'N' and segment_counts[segment_key] > 1:
        logger.warning(f'Segment {segment_key} is duplicated.')

#ensure that the PER loop exists:
if 'PER' and 'PID' in segment_order:
    per_position = segment_order.index('PER')
    pid_position = segment_order.index('PID')

    #ensure there is something inside the PER loop
    if per_position + 1 == pid_position:
        logger.warning('There are no product orders inside the PER loop.')
    else:
    #cycling through to ensure
    # the Nx values are in order -- that is, they are always increasing unless they return to N1, and they never go beyond N4.
    # after all Nx values are shown, we make sure that a PO1 value exists.
        po1_found = False
        previous = segment_order[per_position + 1]
        if previous == 'PO1':
            if not per_position + 2 == pid_position: # this would be a valid loop without N1\
                logger.warning('PO1 value too early in PER loop.')
            po1_found = True
        elif previous[0] != 'N':
            logger.error('Invalid value in PER loop')
        else:
            current = None
            for i in range(per_position+2, pid_position):
                current = segment_order[i]
                if current == 'PO1':
                    if not i + 1 == pid_position: # this would be a valid loop without N1\
                        logger.warning('PO1 value too early in PER loop.')
                        po1_found = True
                    break
                elif current[0] != 'N':
                    logger.error('Invalid value in PER loop')
                    break
                else:
                    if int(current[1]) < int(previous[1]) and not int(current[1]) == 1:
                        logger.error('Incorrect Nx order within PER loop')
                        break
                previous = current
        if segment_order[pid_position-1] != 'PO1' and not po1_found:
            logger.warning('PO1 does not exist in PER loop.')
else:
    logger.warning('The PER loop either does not exist, or does not terminate before PID')

########
#Ensuring correctness of individual fields.
#If such a segment does not exist, there will have already been an error above documenting this.
#For now, this is mainly just type checking

# valid_set = {'ISA', 'GS', 'ST', 'BEG', 'REF', 'PER', 'N1', 'N2', 'N3', 'N4', 'PO1', 'PID', 'CTT', 'SE', 'GE', 'IEA'}
if 'ISA' in segment_order:
    for j in range(segment_limits['ISA']+1):
        match j:
            case 1:
                value = dict_output['Interchange_Control_Header'][f'Authorization_Information_Qualifier']
                if not value.isdigit():
                    logger.warning("ISA's Authorization Information Qualifier field should only contain digits")
            case 2:
                value = dict_output['Interchange_Control_Header'][f'Authorization_Information']
            case 3:
                value = dict_output['Interchange_Control_Header'][f'Security_Information_Qualifier'] 
                if not value.isdigit():
                    logger.warning("ISA's Security Information Qualifier field should only contain digits")
            case 4:
                value = dict_output['Interchange_Control_Header'][f'Security_Information']
            case 5:
                value = dict_output['Interchange_Control_Header'][f'InterchangeID_Sender_Qualifier']
                if not value.isalpha():
                    logger.warning("ISA's Interchange ID Sender Qualifier field should only contain letters")
            case 6:
                value = dict_output['Interchange_Control_Header'][f'Sender_Id']
                if not value.strip().isalpha():
                    logger.warning("ISA's Sender Id field should only contain letters")
            case 7:
                value = dict_output['Interchange_Control_Header'][f'Interchange_ID_Receiver_Qualifier']
                if not value.strip().isdigit():
                    logger.warning("ISA's Interchange ID Receiver Qualifier field should only contain digits")
            case 8:
                value = dict_output['Interchange_Control_Header'][f'Receiver_Id'] 
                if not value.strip().isdigit():
                    logger.warning("ISA's Receiver Id field should only contain digits")
            case 9:
                value = dict_output['Interchange_Control_Header'][f'Interchange_Date']
                # if not isinstance(value, date):
                if not value.isdigit():
                    logger.warning("ISA's Interchange Date field should be a date")
            case 10:
                value = dict_output['Interchange_Control_Header'][f'Interchange_Time']
                # if not isinstance(value, time):
                if not value.isdigit():
                    logger.warning("ISA's Interchange Time field should be a time")
            case 11:
                value = dict_output['Interchange_Control_Header'][f'Interchange_Control_Standards_Identifier']
                if not value.isalpha():
                    logger.warning("ISA's Interchange Control Standards Identifier field should only contain letters")
            case 12:
                value = dict_output['Interchange_Control_Header'][f'Interchange_Control_Version_Number'] 
                if not value.isdigit():
                    logger.warning("ISA's Interchange Control Version Number field should only contain digits")
            case 13:
                value = dict_output['Interchange_Control_Header'][f'Interchange_Control_Number']
                if not value.isdigit():
                    logger.warning("ISA's Interchange Control Number field should only contain digits")
            case 14:
                value = dict_output['Interchange_Control_Header'][f'Acknowledgment_Requested']
                if not value.isdigit():
                    logger.warning("ISA's Acknowledgment Requested field should only contain digits")
            case 15:
                value = dict_output['Interchange_Control_Header'][f'Usage_Indicator']
                if not value.isalpha():
                    logger.warning("ISA's Usage Indicator field should only contain letters")
            case 16:
                value = dict_output['Interchange_Control_Header'][f'Component_Element_Separator']
                if value.isdigit() or len(value) != 1:
                    logger.warning("ISA's Component Element Separator field should be a single non-digit character")
        if 'ISA' in exceed_dict.keys():
            logger.info(f'ISA instructions exceed limit of {segment_limits['ISA']}')
if 'GS' in segment_order:
    for j in range(segment_limits['GS']+1):
        match j:
            case 1:
                value = dict_output['Functional_Group_Header'][f'Functional_Identifier_Code'] 
                if not value.isalpha():
                    logger.warning("GS's Functional Identifier Code field should only contain letters")
            case 2:
                value = dict_output['Functional_Group_Header'][f'Application_Senders_Code'] 
                if not value.isalpha():
                    logger.warning("GS's Application Senders Code field should only contain letters")
            case 3:
                value = dict_output['Functional_Group_Header'][f'Application_Receivers_Code']
                if not value.isdigit():
                    logger.warning("GS's Application Receivers Code field should only contain digits")
            case 4:
                value = dict_output['Functional_Group_Header'][f'Date']
                if not value.isdigit():
                # if not isinstance(value, date):
                    logger.warning("GS's Date field should be a date")
            case 5:
                value = dict_output['Functional_Group_Header'][f'Time'] 
                if not value.isdigit():
                # if not isinstance(value, time):
                    logger.warning("GS's Time field should be a time")
            case 6:
                value = dict_output['Functional_Group_Header'][f'Group_Control_Number']
                if not value.isdigit():
                    logger.warning("GS's Group Control Number field should only contain digits")
            case 7:
                value = dict_output['Functional_Group_Header'][f'Responsible_Agency_Code']
                if not value.isalpha():
                    logger.warning("GS's Responsible Agency Code field should only contain letters")
            case 8:
                value = dict_output['Functional_Group_Header'][f'Version/Release/Industry_Identifier_Code']
                if not value.isdigit():
                    logger.warning("GS's Version/Release/Industry Identifier Code field should only contain digits")
    if 'GS' in exceed_dict.keys():
        logger.info(f'GS instructions exceed limit of {segment_limits['GS']}')
if 'ST' in segment_order:
    unique_val_convert = 'Transaction_Set_Header'
    for j in range(segment_limits['ST']+1):
        match j:
            case 1:
                value = dict_output['Heading'][unique_val_convert][f'Transaction_Set_Identifier_Code'] 
                if not value.isdigit():
                    logger.warning("ST's Transaction Set Identifier Code field should only contain digits")
            case 2:
                value = dict_output['Heading'][unique_val_convert][f'Transaction_Set_Control_Number']
                # print(value)
                if not value.isdigit():
                    logger.warning("ST's Transaction Set Control Number field should only contain digits")
                
    if 'ST' in exceed_dict.keys():
        logger.info(f'ST instructions exceed limit of {segment_limits['ST']}')
        
if 'BEG' in segment_order:
    unique_val_convert = 'Beginning_Segment_for_Purchase_Order'
    for j in range(segment_limits['BEG']+1):
        match j:
            case 1:
                value = dict_output['Heading'][unique_val_convert][f'Transaction_Set_Purpose_Code']
                if not value.isdigit():
                    logger.warning("BEG's Transaction Set Purpose Code field should only contain digits")
            case 2:
                value = dict_output['Heading'][unique_val_convert][f'Purchase_Order_Type_Code']
                if not value.isalpha():
                    logger.warning("BEG's Purchase Order Type Code field should only contain letters")
            case 3:
                value = dict_output['Heading'][unique_val_convert][f'Purchase_Order_Number']
                if not value.isdigit():
                    logger.warning("BEG's Purchase Order Number field should only contain digits")
            case 4:
                value = dict_output['Heading'][unique_val_convert][f'Filler_01']
                if value != '':
                    logger.warning("BEG's Filller 01 field should be an empty string")
            case 5:
                value = dict_output['Heading'][unique_val_convert][f'Date']
                if not value.isdigit():
                # if not isinstance(value, date):
                    logger.warning("BEG's Date field should be a date")
    if 'BEG' in exceed_dict.keys():
        logger.info(f'BEG instructions exceed limit of {segment_limits['BEG']}')
        

if 'REF' in segment_order:
    unique_val_convert = 'Reference_Identification'
    for j in range(segment_limits['REF']+1):
        match j:
            case 1:
                value = dict_output['Heading'][unique_val_convert][f'Reference_Identification_Qualifier']
                if not value.isalpha():
                    logger.warning("REF's Reference Identification Qualifier field should only contain letters")
            case 2:
                value = dict_output['Heading'][unique_val_convert][f'Reference_Identification']
                if not value.isdigit():
                    logger.warning("REF's Reference Identification field should only contain digits")
    if 'REF' in exceed_dict.keys():
        logger.info(f'REF instructions exceed limit of {segment_limits['REF']}')

if 'PER' in segment_order:
    unique_val_convert = 'Administrative_Communications_Contract'
    for j in range(segment_limits['PER']+1):
        match j:
            case 1:  
                value = dict_output['Heading'][unique_val_convert][f'Contact_Function_Code']
                if not value.isalpha():
                    logger.warning("PER's Contact Function Code field should only contain letters")
            case 2:
                value = dict_output['Heading'][unique_val_convert][f'Name']
                # if not value.isalpha():
                #     print("PER's Name field should only contain letters")
            case 3:
                value = dict_output['Heading'][unique_val_convert][f'Communication_Number_Qualifier']
                if not value.isalpha():
                    logger.warning("PER's Communication Number Qualifier field should only contain letters")
            case 4:
                value = dict_output['Heading'][unique_val_convert][f'Communication_Number'] 
                # if not value.isalpha():
                #     print("PER's Communication Number field should only contain letters")
    if 'PER' in exceed_dict.keys():
        logger.info(f'PER instructions exceed limit of {segment_limits['PER']}')

if 'N1' in segment_order:
    unique_val_convert = 'Name'
    for j in range(segment_limits['N1']+1):
        match j:
            case 1:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Entity_Identifier_Code']
                if not value.isalpha():
                    logger.warning("N1's Entity Identifier Code field should only contain letters")
            case 2:
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Name'] 
            case 3:
                value = dict_output['Heading']['N1_Loop'][unique_val_convert]["Identification_Code_Qualifier"] 
                if not value.isdigit():
                    logger.warning("N1's Identification Code Qualifier field should only contain digits")
            case 4:
                value = dict_output['Heading']['N1_Loop'][unique_val_convert]["Identification_Code"] 
                if not value.isdigit():
                    logger.warning("N1's Identification Code field should only contain digits")
    if 'N1' in exceed_dict.keys():
        logger.info(f'N1 instructions exceed limit of {segment_limits['N1']}')

if 'N2' in segment_order:
    unique_val_convert = 'Additional_Name_Information'
    for j in range(segment_limits['N2']+1):
        match j:
            case 1:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Name']
                if not value.isalpha():
                    logger.warning("N2's Name field should only contain letters")
    if 'N2' in exceed_dict.keys():
        logger.info(f'N2 instructions exceed limit of {segment_limits['N2']}')

if 'N3' in segment_order:
    unique_val_convert = 'Address_Information'
    for j in range(segment_limits['N3']+1):
        match j:
            case 1:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Address_Information']
    if 'N3' in exceed_dict.keys():
        logger.info(f'N3 instructions exceed limit of {segment_limits['N3']}')

if 'N4' in segment_order:
    unique_val_convert = 'Geographic_Location'
    for j in range(segment_limits['N4']+1):
        match j:
            case 1:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'City_Name']
                if not value.isalpha():
                    logger.warning("N4's City Name field should only contain letters")
            case 2:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'State_or_Province_Code']
                if not value.isalpha():
                    logger.warning("N4's State or Province Code field should only contain letters")
            case 3:  
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Postal_Code']
                if not value.isdigit():
                    logger.warning("N4's Postal Code field should only contain digits")
            case 4: 
                value = dict_output['Heading']['N1_Loop'][unique_val_convert][f'Country_Code']
                if not value.isalpha():
                    logger.warning("N4's Country Code field should only contain letters")
    if 'N4' in exceed_dict.keys():
        logger.info(f'N4 instructions exceed limit of {segment_limits['N4']}')

if 'PO1' in segment_order:
    unique_val_convert = 'Baseline_Item_Data'
    for j in range(segment_limits['PO1']+1):
        match j:
            case 1:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Assigned_Identification']
                if not value.isdigit():
                    logger.warning("PO1's Assigned Identification field should only contain digits")
            case 2:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Quantity_Ordered']
                if not value.isdigit():
                    logger.warning("PO1's Quantity Ordered field should only contain digits")
            case 3:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Unit_or_Basis_for_Measurement_Code']
                if not value.isalpha():
                    logger.warning("PO1's Unit or Basis for Measurement Code field should only contain letters")
            case 4:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Unit_Price']
                # if not value.strip().isdigit():
                # if not isinstance(value, float):
                #     print("PO1's Unit Price field should be a float")
                #     print(isinstance('42.21', float))
                #     print(value)
                value = value.replace('.', '')
                if not value.isdigit():
                    logger.warning("PO1's Unit Price field should be a float")

            case 6:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Basis_Product_Service_ID_Qualifier']
                if not value.isalpha():
                    logger.warning("PO1's Basis Product Service ID Qualifier field should only contain letters")
            case 7:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Basis_Product_Service_ID']
                if not value.isalnum():
                    logger.warning("PO1's Basis Product Service ID field should only contain letters or digits")
            case 8:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Product/ServiceID_Qualifier']
                if not value.isalpha():
                    logger.warning("PO1's Product/ServiceID Qualifier field should only contain letters")
            case 9:  
                value = dict_output['Detail']['PO1_Loop'][unique_val_convert][f'Product/ServiceID'] 
                if not value.isdigit():
                    logger.warning("PO1's Product/ServiceID field should only contain digits")
    if 'PO1' in exceed_dict.keys():
        logger.info(f'PO1 instructions exceed limit of {segment_limits['PO1']}')

if 'PID' in segment_order:
    unique_val_convert = 'Product/Item_Description'
    for j in range(segment_limits['PID']+1):
        match j:
            case 1:  
                value = dict_output['Detail']['PID_Loop'][unique_val_convert][f'Item_Description_Type']
                if not value.isalpha():
                    logger.warning("PID's Item Description Type field should only contain letters")
            case 2:
                value = dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_01']
                if value != '':
                    logger.warning("PID's Filler 01 field should be an empty string.")
            case 3:
                value = dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_02']
                if value != '':
                    logger.warning("PID's Filler 01 field should be an empty string.")
            case 4:
                value = dict_output['Detail']['PID_Loop'][unique_val_convert][f'Filler_03']
                if value != '':
                    logger.warning("PID's Filler 01 field should be an empty string.")
            case 5:  
                value = dict_output['Detail']['PID_Loop'][unique_val_convert][f'Description']
                if value.isalpha():
                    logger.warning("PID's Description field should only contain letters")
    if 'PID' in exceed_dict.keys():
        logger.info(f'PID instructions exceed limit of {segment_limits['PID']}')

if 'CTT' in segment_order:
    unique_val_convert = 'Transaction_Totals'
    for j in range(segment_limits['CTT']+1):
        match j:
            case 1:  
                value = dict_output['Summary']['CTT_Loop'][unique_val_convert][f'Number_Of_Line_Items']
                if not value.isdigit():
                    logger.warning("CTT's Number Of Line Items field should only contain digits.")
    if 'CTT' in exceed_dict.keys():
        logger.info(f'CTT instructions exceed limit of {segment_limits['CTT']}')

if 'SE' in segment_order:
    unique_val_convert = 'Transaction_Set_Trailer'
    for j in range(segment_limits['SE']+1):
        match j:
            case 1:  
                value = dict_output['Summary'][unique_val_convert][f'Number_Of_Included_Segments']
                if not value.isdigit():
                    logger.warning("CTT's Number Of Included Segments field should only contain digits.")
            case 2:  
                value = dict_output['Summary'][unique_val_convert][f'Transaction_Set_Control_Number']
                if not value.isdigit():
                    logger.warning("CTT's Transaction Set Control Number field should only contain digits.")

    if 'SE' in exceed_dict.keys():
        logger.info(f'SE instructions exceed limit of {segment_limits['SE']}')

if 'GE' in segment_order:
    unique_val_convert = 'Functional_Group_Trailer'
    for j in range(segment_limits['GE']+1):
        match j:
            case 1:  
                value = dict_output['Summary'][unique_val_convert][f'Number_of_Transaction_Sets_Included']
                if not value.isdigit():
                    logger.warning("GE's Number of Transaction Sets Included field should only contain digits.")
            case 2:  
                value = dict_output['Summary'][unique_val_convert][f'Group_Control_Number']
                if not value.isdigit():
                    logger.warning("GE's Group Control Number field should only contain digits.")
    if 'GE' in exceed_dict.keys():
        logger.info(f'GE instructions exceed limit of {segment_limits['GE']}')

if 'IEA' in segment_order:
    unique_val_convert = 'Interchange_Control_Trailer'
    for j in range(segment_limits['IEA']+1):
        match j:
            case 1:  
                value = dict_output['Summary'][unique_val_convert][f'Number_of_Included_Functional_Groups']
                if not value.isdigit():
                    logger.warning("IEA's Number of Included Functional Groups field should only contain digits.")
            case 2:  
                value = dict_output['Summary'][unique_val_convert][f'Interchange_Control_Number']
                if not value.isdigit():
                    logger.warning("IEA's Interchange Control Number field should only contain digits.")

    if 'IEA' in exceed_dict.keys():
        logger.info(f'IEA instructions exceed limit of {segment_limits['IEA']}')
############
#Complete error handling and log file:
# sys.stdout = console_output
########
log_results = ''
with open('output.log', 'r') as f:
    log_results = f.read()
#Converting to JSON and Saving final result IF no error (log file should be empty)

if log_results == '':
    json_output = json.dumps(dict_output, indent=4)
    with open("output.json", "w") as f:
        json.dump(json_output, f, indent=4)
    print('EDI file successfully converted to JSON.')
else:
    print('Error in converting EDI file to JSON. View more information in log.txt')
# print(json_output)