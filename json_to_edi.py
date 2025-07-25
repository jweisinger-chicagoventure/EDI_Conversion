#step 1: read JSON file into variable (as string)
import json
import re

json_input_text = {}
ghi_output = ""

with open ("output.json", "r") as f:
    json_input_text = json.load(f)

#step 2: convert JSON to dictionary
# print(json_input_text)


dict_input = json.loads(json_input_text)

value_count = 0
def check_dictionary(dict_input):
    global value_count
    global ghi_output
    for key, value in dict_input.items():
        if type(value) != dict:
            ghi_output += value + "^"
            value_count += 1
        # else:
        #     match key:
        #         case "Beginning Segment for Purchase Order":
        #             ghi_output += 'BEG' + "^"
        #         case "Transaction Set Header":
        #             ghi_output += 'ST' + "^"
        #         case "Person Identification":
        #             ghi_output += 'PER' + "^"
        #         case "Reference Number":
        #             ghi_output += 'REF' + "^"
        #         case "Product ID":
        #             ghi_output += 'PID' + "^"
        #         case "SE":
        #             ghi_output += 'SE' + "^"
        #         case "GE":
        #             ghi_output += 'GE' + "^"
        #         case "IEA":
        #             ghi_output += 'IEA' + "^"
        #         case "N1":
        #             ghi_output += 'N1' + "^"
        #         case "N2":
        #             ghi_output += 'N2' + "^"
        #         case "N3":
        #             ghi_output += 'N3' + "^"
        #         case "N4":
        #             ghi_output += 'N4' + "^"
        #         case "N5":
        #             ghi_output += 'N5' + "^"
        #         case "PO1":
        #             ghi_output += 'PO1' + "^"
        #         case "CTT":
        #             ghi_output += 'CTT' + "^"
        #     check_dictionary(value)
        #     if value_count >= 1:
        #         ghi_output = ghi_output[:-1]
        #         ghi_output += "~"
        #         value_count = 0

# def check_dictionary(dict_input):
#     for key, value in dict_input.items():
#         if type(value) != dict:
#             ghi_output += value + "^"
# for key, value in dict_input['Interchange'].items():
#     if type(value) != dict:
#         ghi_output += value + "^"
# ghi_output = ghi_output[:-1]
# ghi_output += "~"
# for key, value in dict_input['Groups'].items():
#     if type(value) != dict:
#         ghi_output += value + "^"
# ghi_output = ghi_output[:-1]
# ghi_output += "~"
# ghi_output = ghi_output[:-1]

# for i in range(len(dict_input['Interchange'])):
#     print(dict_input['Interchange'].keys()[i])
#     del dict_input['Interchange'][list(dict_input['Interchange'].keys())[i]]
# for i in range(len(dict_input['Groups'])):
#     if 
#     del dict_input['Groups'][list(dict_input['Groups'].keys())[i]]

ghi_output += "ISA" + "^"
check_dictionary(dict_input['Interchange_Control_Header'])
ghi_output = ghi_output[:-1]
ghi_output += "~"


ghi_output += "GS" + "^"
check_dictionary(dict_input['Functional_Group_Header'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "ST" + "^"
check_dictionary(dict_input['Heading']['Transaction_Set_Header'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "BEG" + "^"
check_dictionary(dict_input['Heading']['Beginning_Segment_for_Purchase_Order'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "REF" + "^"
check_dictionary(dict_input['Heading']['Reference_Identification'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "PER" + "^"
check_dictionary(dict_input['Heading']['Administrative_Communications_Contract'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "N1" + "^"
check_dictionary(dict_input['Heading']['N1_Loop']['Name'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "N1" + "^"
check_dictionary(dict_input['Heading']['N1_Loop']['Name'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "N2" + "^"
check_dictionary(dict_input['Heading']['N1_Loop']['Additional_Name_Information'])
ghi_output = ghi_output[:-1]
ghi_output += "~"


ghi_output += "N3" + "^"
check_dictionary(dict_input['Heading']['N1_Loop']['Address_Information'])
ghi_output = ghi_output[:-1]
ghi_output += "~"


ghi_output += "N4" + "^"
check_dictionary(dict_input['Heading']['N1_Loop']['Geographic_Location'])
ghi_output = ghi_output[:-1]
ghi_output += "~"


# ghi_output += "N4" + "^"
# check_dictionary(dict_input['Summary']['Transaction_Set_Trailer'])
# ghi_output = ghi_output[:-1]
# ghi_output += "~"

ghi_output += "PO1" + "^"
check_dictionary(dict_input['Detail']['PO1_Loop']['Baseline_Item_Data'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "PID" + "^"
check_dictionary(dict_input['Detail']['PID_Loop']['Product/Item_Description'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "CTT" + "^"
check_dictionary(dict_input['Summary']['CTT_Loop']['Transaction_Totals'])
ghi_output = ghi_output[:-1]
ghi_output += "~"


ghi_output += "SE" + "^"
check_dictionary(dict_input['Summary']['Transaction_Set_Trailer'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "GE" + "^"
check_dictionary(dict_input['Summary']['Functional_Group_Trailer'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

ghi_output += "IEA" + "^"
check_dictionary(dict_input['Summary']['Interchange_Control_Trailer'])
ghi_output = ghi_output[:-1]
ghi_output += "~"

with open("output.edi", "w") as file:
    file.write(ghi_output)
print(ghi_output)
# print(ghi_output)