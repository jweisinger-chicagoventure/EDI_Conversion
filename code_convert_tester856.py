#Conversion from Oracle Data -> EDI File (850)
import oracledb
import pandas as pd
from datetime import datetime, timedelta
import os

os.makedirs("./856_Test_Files", exist_ok=True)
#Retrieving password from secure file
password_txt = ''
with open('password.txt', 'r') as f:
    password_txt = f.read()

#Connection to Oracle DB
oracledb.init_oracle_client("C:/instantclient_23_8")
conn = oracledb.connect(user="JWEISINGER[TYPENEX]", password=password_txt, dsn="10.0.190.27:1521/APEX_SANDBOX")
#Function to retrieve result of SQL Query
def retrieve(sql_statement):
    with conn.cursor() as cursor:
        cursor.execute(sql_statement)
        return  [description[0] for description in cursor.description], cursor.fetchall() #columns, table
    
#Variable definitions based on original File
# user_specified_order_id = "2010120011"
company_columns, company_table = retrieve("SELECT * FROM COMPANIES WHERE HEADQUARTER = 'Y'")
pd_company = pd.DataFrame(company_table, columns=company_columns)
company_name_var = pd_company.loc[0, 'COMPANY_NAME'] #COMPANY_NAME_VAR
address_1_var = pd_company.loc[0, 'ADDRESS_1'] #ADDRESS_1_VAR
address_2_var = pd_company.loc[0, 'ADDRESS_2'] #ADDRESS_2_VAR

city_var = pd_company.loc[0, 'CITY'] #CITY_VAR
state_var = pd_company.loc[0, 'STATE'] #STATE_VAR
zip_var = pd_company.loc[0, 'ZIP'] #ZIP_VAR

inter_con_num = retrieve("SELECT GHX_INTER_CON_NUM.NEXTVAL FROM DUAL")[1][0][0] #INTER_CON_NUM
group_con_num = retrieve("SELECT GHX_GROUP_CON_NUM.NEXTVAL FROM DUAL")[1][0][0] #GROUP_CON_NUM
order_details_columns, order_details_table = retrieve(f"""SELECT *FROM ORDER_HEADERS, FULL_CONTRACT WHERE (CREATED_USER ='GHXTEST' OR CREATED_USER ='GHX')
  AND ORDER_DATE IS NOT NULL AND FULFILLED_DATE IS NOT NULL AND SHIPPED_DATE IS NOT NULL AND (FULL_CONTRACT.GHX_INVOICE ='Y' OR ORDER_ID ='24024699') AND (INVOICE_DATE IS NULL)
  AND (FULL_CONTRACT.BILL_COUNTRY='USA' OR FULL_CONTRACT.BILL_COUNTRY='CANADA') AND FULL_CONTRACT.BILL_TO_# = ORDER_HEADERS.BILL_TO_# AND FULL_CONTRACT.SHIP_TO_# = ORDER_HEADERS.SHIP_TO_# AND (ORDER_HEADERS.GHX_810_DATE IS NULL)
  AND ORDER_HEADERS.ORDER_STATUS <> 'CANCEL'""")
pd_order = pd.DataFrame(order_details_table, columns=order_details_columns)
for i, row in enumerate(pd_order.iterrows()):
    dict_table = pd_order.to_dict(orient="index")
    created_user_var = pd_order.loc[i, "CREATED_USER"] #CREATED_USER_VAR
    ghx_orderid_var = pd_order.loc[i, "GHX_ORDERID"] #GHX_ORDERID_VAR
    order_status_var = pd_order.loc[i, "ORDER_STATUS"] #ORDER_STATUS_VAR
    shipping_status_var = pd_order.loc[i, "SHIPPING_STATUS"] #SHIPPING_STATUS_VAR
    orderid_var = pd_order.loc[i, "ORDER_ID"] #ORDERID_VAR
    print(f"order id var... : {orderid_var}")
    order_total_var = pd_order.loc[i, "ORDER_TOTAL"] #ORDER_TOTAL_VAR
    ship_to_address_1_var = pd_order.loc[i, "SHIP_TO_ADDRESS_1"].iloc[0] #SHIP_TO_ADDRESS_1_VAR
    ship_to_address_2_var = pd_order.loc[i, "SHIP_TO_ADDRESS_2"].iloc[0] #SHIP_TO_ADDRESS_2_VAR
    shipped_date_var = pd_order.loc[i, "SHIPPED_DATE"] #SHIPPED_DATE_VAR
    ship_to_num_var = pd_order.loc[i, "SHIP_TO_#"].iloc[0] #SHIP_TO_#_VAR
    cus_po_var = pd_order.loc[i, "CUS_PO"] #CUS_PO_VAR
    ship_to_name = pd_order.loc[i, 'SHIP_TO_NAME'].iloc[0]
    order_value = pd_order.loc[i, 'ORDER_ID']
    bill_overdue = pd_order.loc[i, 'BILL_OVER_DUE'] if type(pd_order.loc[0, 'BILL_OVER_DUE']) == str else pd_order.loc[0, 'BILL_OVER_DUE'].iloc[0] #BILL_OVER_DUE_VAR
    shipping_type_var = pd_order.loc[i, 'SHIPPING_TYPE'] #SHIPPING_TYPE_VAR
    shipping_carrier_var = pd_order.loc[i, 'SHIPPING_CARRIER'] #SHIPPING_CARRIER_VAR
    split_order_id_var = pd_order.loc[i, 'SPLIT_ORDER_ID'] #SPLIT_ORDER_ID_VAR
    bill_to_city_var = pd_order.loc[i, 'BILL_TO_CITY'].iloc[0] #BILL_TO_CITY_VAR
    bill_to_st_var = pd_order.loc[i, 'BILL_TO_ST'].iloc[0] #BILL_TO_STATE_VAR
    bill_to_zip_var = pd_order.loc[i, 'BILL_TO_ZIP'].iloc[0] #BILL_TO_ZIP_VAR
    ship_to_city_var = pd_order.loc[i, 'SHIP_TO_CITY'].iloc[0] #SHIP_TO_CITY_VAR
    ship_to_st_var = pd_order.loc[i, 'SHIP_TO_ST'].iloc[0] #SHIP_TO_STATE_VAR
    ship_to_zip_var = pd_order.loc[i, 'SHIP_TO_ZIP'].iloc[0] #SHIP_TO_ZIP_VAR
    bill_to_name_var = pd_order.loc[i, 'BILL_TO_NAME'].iloc[0] #BILL_TO_NAME_VAR
    bill_to_num_var = pd_order.loc[i, 'BILL_TO_#'].iloc[0] #BILL_TO_NUM_VAR
    ship_total_var = pd_order.loc[i, 'SHIP_TOTAL'] #SHIP_TOTAL_VAR
    mini_order_surcharge_var = pd_order.loc[i, 'MINI_ORDER_SURCHAGE'] #MINI_ORDER_SURCHARGE_VAR
    saturday_surcharge_var = pd_order.loc[i, 'SATURDAY_SURCHAGE'] #SATURDAY_SURCHARGE_VAR
    drop_ship_surcharge_var = pd_order.loc[i, 'DROP_SHIP_SURCHARGE'] #DROPSHIP_SURCHARGE_VAR
    sales_tax_var = pd_order.loc[i, 'SALES_TAX'] #SALES_TAX_VAR
    terms_var = pd_order.loc[i, 'TERMS'] #TERMS_VAR

    split_order_id_var = pd_order.loc[i, 'SPLIT_ORDER_ID'] #SPLIT_ORDER_ID_VAR

    ghx_header_columns, ghx_header_table = retrieve(f"SELECT * FROM GHX_HEADERS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND SHIP_TO_# = '{ship_to_num_var}'")
    pd_ghx = pd.DataFrame(ghx_header_table, columns=ghx_header_columns)
    ghx_ship_to_num_var = pd_ghx.loc[0, "GHX_SHIP_TO_#"] if not pd_ghx.empty else '' #GHX_SHIP_TO_#_VAR
    ghx_ship_to_name_var = pd_ghx.loc[0, "SHIP_TO_NAME"] if not pd_ghx.empty else '' #GHX_SHIP_TO_NAME_VAR
    cust_po = pd_ghx.loc[0, "CUST_PO"] if not pd_ghx.empty else '' #PO_DATE_VAR
    po_date_var = pd_ghx.loc[0, "PO_DATE"] if not pd_ghx.empty else '' #PO_DATE_VAR
    vn_value_var = pd_ghx.loc[0, "VN_VALUE"] if not pd_ghx.empty else '' #VN_VALUE_VAR
    sn_value_var = pd_ghx.loc[0, "SN_VALUE"] if not pd_ghx.empty else '' #SN_VALUE_VAR
    rush_order_var = pd_ghx.loc[0, 'RUSH_ORDER'] if not pd_ghx.empty else '' #SPLIT_ORDER_ID_VAR

    ref_po_var = pd_ghx.loc[0, "REF_PO"] if not pd_ghx.empty else ''
    ref_co_var = pd_ghx.loc[0, "REF_CO"] if not pd_ghx.empty else ''
    ref_qc_var = pd_ghx.loc[0, "REF_QC"] if not pd_ghx.empty else ''

    line_counter = 0
    #
    ghx_detail_columns, ghx_detail_table = retrieve(f"SELECT * FROM GHX_DETAILS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND LINE_ID IN (SELECT GHX_LINEID FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}') ORDER BY LINE_ID ASC")
    pd_detail = pd.DataFrame(ghx_detail_table, columns=ghx_detail_columns)
    line_counter += 1 if not pd_detail.empty else 0 
    ghx_line_id_var = pd_detail.loc[0, "LINE_ID"] if not pd_detail.empty else '' #GHX_LINE_ID_VAR
    ghx_uom_var = pd_detail.loc[0, "UOM"] if not pd_detail.empty else '' #GHX_UOM_VAR
    ghx_qty_var = pd_detail.loc[0, "QTY"] if not pd_detail.empty else '' #QTY_VAR
    buyer_id_identifier_var = pd_detail.loc[0, "BUYER_ID_IDENTIFIER"] if not pd_detail.empty else '' #BUYER_ID_IDENTIFIER_VAR
    buyer_id_var = pd_detail.loc[0, "BUYER_ID"] if not pd_detail.empty else '' #BUYER_ID_VAR
    ghx_case_price_var = pd_detail.loc[0, "CASE_PRICE"] if not pd_detail.empty else '' #CASE_PRICE_VAR
    mapped_vendor_var = pd_detail.loc[0, "MAPPED_VENDOR_ID"] if not pd_detail.empty else '' #MAPPED_VENDOR_VAR
    vendor_id_var = pd_detail.loc[0, "VENDOR_ID"] if not pd_detail.empty else '' #VENDOR_ID


    scac_columns, scac_table = retrieve(f"SELECT MIN(CODE) as SCAC, MIN(FISHER_CODE) as FISHER_SCAC FROM GHX_SCAC WHERE SHIP_TYPE = '{shipping_type_var}' AND FULL_DESC LIKE '{shipping_carrier_var}%' AND VALID = 'Y'")
    pd_ghx_scac = pd.DataFrame(scac_table, columns=scac_columns)
    scac_var = pd_ghx_scac.loc[0, "SCAC"] if not pd_ghx_scac.empty else ''
    fisher_scac_var = pd_ghx_scac.loc[0, "FISHER_SCAC"] if not pd_ghx_scac.empty else ''
    scac_var =  retrieve(f"SELECT SCAC_CODE FROM ORDER_HEADERS WHERE ORDER_ID = '{orderid_var}'")[1][0][0] if scac_var == None else scac_var 
    scac_var = 'UNKN' if scac_var == None else scac_var
    fisher_scac_var = 'UNKN' if fisher_scac_var == None else fisher_scac_var
    track_columns, track_table = retrieve(f"SELECT DISTINCT TRACK_NO FROM ORDER_TRACKS WHERE ORDER_ID = '{orderid_var}' ORDER BY TRACK_NO")
    pd_track = pd.DataFrame(track_table, columns=track_columns)
    track_no_var = pd_track.loc[0, "TRACK_NO"] #TRACK_NO_VAR

    order_detail_columns, order_detail_table = retrieve(f"SELECT MAX(PRODUCT_ID) as PRODUCT_ID, MAX(CASE_PRICE) as CASE_PRICE, SUM(QTY) as QTY, MAX(REPLACE(PRODUCT_DESC,'|','-')) as PRODUCT_DESC, SUM(QTY) as QTY,  MAX(BACKORDER_LINE_FLAG) as BACKORDER_LINE  FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}'")
    pd_order_detail= pd.DataFrame(order_detail_table, columns=order_detail_columns)
    order_details_case_price_var = pd_order_detail.loc[0, "CASE_PRICE"] if not pd_order_detail.empty else '' #ORDER_DETAILS_CASE_PRICE_VAR
    order_details_line_sum_qty_var = pd_order_detail.loc[0, "QTY"].iloc[0] if not pd_order_detail.empty else ''  #ORDER_DETAILS_LINE_SUM_QTY_VAR
    order_details_product_id_var = pd_order_detail.loc[0, "PRODUCT_ID"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_PRODUCT_ID_VAR
    order_details_product_desc_var = pd_order_detail.loc[0, "PRODUCT_DESC"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_PRODUCT_DESC_VAR
    order_details_backorder_line = pd_order_detail.loc[0, "BACKORDER_LINE"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_PRODUCT_DESC_VAR
    order_detail_count = retrieve(f"SELECT COUNT(*) FROM ORDER_DETAILS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND GHX_LINEID = '{ghx_line_id_var}'")[1][0][0]
    order_detail_count2 = retrieve(f"SELECT COUNT(*) FROM PRODUCTS WHERE PRODUCT_ID = '{mapped_vendor_var}' AND VALID_FOR_SALE = '1'")[1][0][0]
    count_var  = retrieve(f"SELECT COUNT(*) FROM GHX_HEADERS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND SHIP_TO_# = '{ship_to_num_var}'") #COUNT_VAR
    count_line_var  = retrieve(f"SELECT COUNT(*) FROM ORDER_DETAILS, GHX_DETAILS WHERE ORDER_ID = '{orderid_var}' AND ORDER_DETAILS.GHX_ORDERID = '{ghx_orderid_var}' AND GHX_DETAILS.GHX_ORDERID = ORDER_DETAILS.GHX_ORDERID AND ORDER_DETAILS.GHX_LINEID = GHX_DETAILS.LINE_ID") # WHERE GHX_ORDERID = '{ghx_orderid_var}' AND SHIP_TO_# = '{ship_to_num_var}'") #COUNT_VAR
    count_package_var  = retrieve(f"SELECT COUNT(DISTINCT TRACK_NO) FROM ORDER_TRACKS WHERE ORDER_ID = '{orderid_var}'") #COUNT_PACKAGE_VAR
    total_weight_package_var  = retrieve(f"SELECT SUM(LINE_WEIGHT) FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}'") # TOTAL_WEIGHT_PACKAGE_VAR
    terms_columns, terms_table = retrieve(f"SELECT * FROM TERMS WHERE TERMS_STR = '{terms_var}'")
    lot_var = retrieve(f"SELECT MIN(LOT) FROM ORDER_LOTS WHERE ORDER_ID = '{orderid_var}' AND PRODUCT_ID = '{order_details_product_id_var}'")[1][0][0]

    pd_terms = pd.DataFrame(terms_table, columns=terms_columns)
    discount_pct_var = pd_terms.loc[0, "DISCOUNT_PCT"] if not pd_terms.empty else '' #DISCOUNT_PCT_VAR
    discount_days_var = pd_terms.loc[0, "DISCOUNT_DAYS"] if not pd_terms.empty else '' #DISCOUNT_DAYS_VAR
    days_var = pd_terms.loc[0, "DAYS"] if not pd_terms.empty else 0 #DAYS_VAR
    typenex_uom_var = None
    if order_details_product_id_var != None:
        typenex_uom_var = retrieve(f"SELECT COUNT(*) FROM PRODUCTS WHERE PRODUCT_ID = '{order_details_product_id_var}'")
    segment_counter = 2
    segments = []


    ghx_status_code =  "IR" #GHX_STATUS_CODE
    veyer_ack08_msg = "IB Backorder" #VEYER_ACK08_MSG
    curr_date = datetime.now()
    segments.append(["ISA", "00", "          ", "00", "          ", "01", "600850213      ", 
    "ZZ", str(created_user_var)[:15] if len(created_user_var) >= 15 else str(created_user_var) + ("").join([' ' for i in range(15-len(created_user_var))]), 
    str(curr_date.strftime('%y%m%d')), str(curr_date.strftime('%H%M')), "U", "00401", inter_con_num[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))]) + str(inter_con_num), "0", "P", "|"])
    segments.append(["GS", "SH", "600850213", created_user_var, str(curr_date.strftime('%Y%m%d')), str(curr_date.strftime('%H%M')), str(group_con_num)[:9] if len(str(group_con_num))>= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num), "X", "004010"])
    segments.append(["ST", "856", str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num)])    
    # segments.append(["BSN", ("00" if (vn_value_var == "VN00106821" or vn_value_var == "71341601") else "06"), ("AD" if (vn_value_var == "71341601") else "AC"), cus_po_var,  (po_date_var.strftime("%Y%m%d") if po_date_var else po_date_var), "", "", "", orderid_var, str(curr_date.strftime('%Y%m%d'))])
    segments.append(["BSN", "00", orderid_var, str(curr_date.strftime('%Y%m%d'))] + ([str(curr_date.strftime('%H%M')), "0004"] if (vn_value_var == "71341601") else [str(curr_date.strftime('%H%M'))]) )
    segments.append(["HL", "1", "", "S"])
    segments.append(["TD1", "", (count_package_var if count_package_var != None else 1), "", "", "", "", (total_weight_package_var if total_weight_package_var != None else 10), "LB"]) if vn_value_var == "71341601" else None
    if shipping_carrier_var not in ["OPTI", "ECHO", "PUP"]:
        if vn_value_var == "71341601":
            segments.append(["TD5", "", 2, fisher_scac_var])
        else:
            curr_segment = ["TD5", "B", "2"] + ([fisher_scac_var] if vn_value_var == "VN00106821" else [scac_var])
            if shipping_carrier_var == "OTH" and shipping_type_var == "FREIGHT":
                curr_segment += ["M"]
            elif shipping_carrier_var == "UPS" or shipping_carrier_var == "FEDEX":
                curr_segment += ["U"]
            segments.append(curr_segment + [shipping_carrier_var])
    if vn_value_var == "71341601":
        segments.append(["REF", "CN", ref_qc_var])
        for row in pd_detail.iterrows():
            segments.append(["REF", "BM", track_no_var[:30]])
    else:
        for row in pd_track.iterrows():
            if track_no_var != None:
                segments.append(["REF", ("CN" if vn_value_var == "VN00106821" else "2I"), track_no_var[:30]])
    segments.append(["DTM", "011", str(curr_date.strftime('%Y%m%d'))])
    if vn_value_var == None or vn_value_var != "71341601":
        segments.append(["N1", "ST", ship_to_name, "91", ghx_ship_to_num_var if ghx_ship_to_num_var else ship_to_num_var])
        segments.append(["N3", ship_to_address_1_var]) 
        segments.append(["N3", ship_to_address_2_var]) if ship_to_address_2_var != None else None 
        segments.append(["N4", ship_to_city_var, ship_to_st_var, ship_to_zip_var])
    elif vn_value_var == "71341601":
        segments.append(["N1", "ST", ship_to_name, "91", ghx_ship_to_num_var])
    if vn_value_var != None:
        segments.append(["N1", "SF", company_name_var, "92", vn_value_var])
        segments.append(["N3", address_1_var])
        segments.append(["N4", city_var, state_var, zip_var])
    if vn_value_var != None and vn_value_var != "71341601":
        segments.append(["N1", "VN", company_name_var, "92", vn_value_var])
        segments.append(["N3", address_1_var])
        segments.append(["N4", city_var, state_var, zip_var])
    segments.append(["HL", "2", "1", "O"])
    segments.append(["PRF", cus_po_var])
    if ghx_orderid_var != None:
        segments.append(["REF", "OQ", ghx_orderid_var])
        if ref_po_var != None:
            segments.append(["REF", "PO", ref_po_var])

    for row in pd_detail.iterrows():
        curr_segment = []
        if order_details_line_sum_qty_var > 0:
            line_counter += 1
            segments.append(["HL", 2+line_counter, "2", "I"])
            curr_segment += ["LIN", ghx_line_id_var]
        if vn_value_var == "71341601":
            curr_segment += ["VA", vendor_id_var]
        if buyer_id_var != None:
            if vn_value_var == "71341601":
                segments.append(curr_segment + [(buyer_id_identifier_var if buyer_id_identifier_var else "IN"), buyer_id_var]) 
                curr_segment = []
            else:
                curr_segment += [(buyer_id_identifier_var if buyer_id_identifier_var else "IN"), buyer_id_var]
        if vn_value_var == None or vn_value_var != "71341601":
            segments.append(curr_segment + ["VC", order_details_product_id_var])
            curr_segment = []
        curr_segment += ["SN1", "", order_details_line_sum_qty_var]
        segments.append(curr_segment + [(ghx_uom_var if ghx_uom_var != None else (typenex_uom_var if typenex_uom_var else "CA"))] )
        if vn_value_var == "VN00106821":
            segments.append(["SLN", ghx_line_id_var, "", "I", "", "", "", "", "", "LT", (lot_var if lot_var != None else 'UNKNOWN')])  
            segments.append(["DTM", "036", "20501230"])
    segments.append(["CTT", 2+line_counter])
    segments.append(["SE", len(segments)-1, str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num)])
    segments.append(["GE", "1", str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num)])
    segments.append(["IEA", "1", str(inter_con_num)[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))])  + str(inter_con_num)])

    edi_text = ""
    for segment in segments:
        for field in segment:
            edi_text += str(field) + "^"
        edi_text = edi_text[:-1]
        edi_text += "~"

    with open(f"./856_Test_Files/{orderid_var}.txt", "w") as f:
        f.write(edi_text)
    print(edi_text)
    print("next")
    print("CARRIER")
    print(shipping_carrier_var)
print("done")

