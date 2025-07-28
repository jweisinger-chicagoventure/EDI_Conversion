#Conversion from Oracle Data -> EDI File (850)
import oracledb
import pandas as pd
from datetime import datetime, timedelta
import os

os.makedirs("./810_Test_Files", exist_ok=True)
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
order_details_columns, order_details_table = retrieve(f"""SELECT * FROM ORDER_HEADERS, FULL_CONTRACT WHERE (CREATED_USER ='GHXTEST' OR CREATED_USER ='GHX')
  AND ORDER_DATE IS NOT NULL AND FULFILLED_DATE IS NOT NULL AND SHIPPED_DATE IS NOT NULL AND (FULL_CONTRACT.GHX_INVOICE ='Y' OR ORDER_ID ='24024699') AND (INVOICE_DATE IS NULL)
  AND (FULL_CONTRACT.BILL_COUNTRY='USA' OR FULL_CONTRACT.BILL_COUNTRY='CANADA') AND FULL_CONTRACT.BILL_TO_# = ORDER_HEADERS.BILL_TO_# AND FULL_CONTRACT.SHIP_TO_# = ORDER_HEADERS.SHIP_TO_# AND (ORDER_HEADERS.GHX_810_DATE IS NULL)
  AND ORDER_HEADERS.ORDER_STATUS <> 'CANCEL'""")
pd_order = pd.DataFrame(order_details_table, columns=order_details_columns)
for i, row in enumerate(pd_order.iterrows()):
    created_user_var = pd_order.loc[i, "CREATED_USER"] #CREATED_USER_VAR
    ghx_orderid_var = pd_order.loc[i, "GHX_ORDERID"] #GHX_ORDERID_VAR
    order_status_var = pd_order.loc[i, "ORDER_STATUS"] #ORDER_STATUS_VAR
    shipping_status_var = pd_order.loc[i, "SHIPPING_STATUS"] #SHIPPING_STATUS_VAR
    orderid_var = pd_order.loc[i, "ORDER_ID"] #ORDERID_VAR
    order_total_var = pd_order.loc[i, "ORDER_TOTAL"] #ORDER_TOTAL_VAR
    ship_to_address_1_var = pd_order.loc[i, "SHIP_TO_ADDRESS_1"] #SHIP_TO_ADDRESS_1_VAR
    ship_to_address_2_var = pd_order.loc[i, "SHIP_TO_ADDRESS_2"] #SHIP_TO_ADDRESS_2_VAR
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
    ship_to_city_var = pd_order.loc[i, 'SHIP_TO_CITY'] #SHIP_TO_CITY_VAR
    ship_to_st_var = pd_order.loc[i, 'SHIP_TO_ST'] #SHIP_TO_STATE_VAR
    ship_to_zip_var = pd_order.loc[i, 'SHIP_TO_ZIP'] #SHIP_TO_ZIP_VAR
    bill_to_name_var = pd_order.loc[i, 'BILL_TO_NAME'].iloc[0] #BILL_TO_NAME_VAR
    bill_to_num_var = pd_order.loc[i, 'BILL_TO_#'].iloc[0] #BILL_TO_NUM_VAR
    bill_to_address_1_var = pd_order.loc[i, 'BILL_TO_ADDRESS_1'].iloc[0] #BILL_TO_NUM_VAR
    bill_to_address_2_var = pd_order.loc[i, 'BILL_TO_ADDRESS_2'].iloc[0] #BILL_TO_NUM_VAR
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
    "ZZ", (str(created_user_var)[:15] if len(created_user_var) >= 15 else str(created_user_var) + ("").join([' ' for i in range(15-len(created_user_var))]) ), 
    str(curr_date.strftime('%y%m%d')), str(curr_date.strftime('%H%M')), "U", "00400", (inter_con_num[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))]) + str(inter_con_num)), "0", "P", "|"])
    segments.append(["GS", "IN", "600850213", created_user_var, str(curr_date.strftime('%Y%m%d')), str(curr_date.strftime('%H%M')), (str(group_con_num)[:9] if len(str(group_con_num))>= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num)), "X", "004010"])
    segments.append(["ST", "810", (str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num))])
    curr_segment = ["BIG", str(curr_date.strftime('%Y%m%d')), orderid_var, po_date_var.strftime("%Y%m%d")]
    segments.append(curr_segment + [cus_po_var]) if vn_value_var == "71341601" else segments.append(curr_segment + [cus_po_var, "", "", "DI"])
    if ghx_orderid_var != None:
        segments.append(["REF", "OQ", ghx_orderid_var] + (["REF", "PO", ref_po_var] if ref_po_var else []))
    if vn_value_var == "VN00106821":
        segments.append(["N1", "ST", ship_to_name, "91", ghx_ship_to_num_var])
        segments.append(["N1", "RE", company_name_var, "91", "600850213"])
        segments.append(["N3", ((address_1_var if (address_1_var != None) else "") + " " + (address_2_var if (address_2_var != None) else "")).rstrip()])
        segments.append(["N4", city_var, state_var, zip_var])
    elif ghx_ship_to_num_var[:4] == "6125" and len(ghx_ship_to_num_var) == 6:
        segments.append(["N1", "BT", bill_to_name_var, "92", ghx_ship_to_num_var])
        segments.append(["N3", bill_to_address_1_var])
        segments.append(["N4", bill_to_city_var, bill_to_st_var, bill_to_zip_var])
        segments.append(["N1", "ST", ship_to_name, "92", ghx_ship_to_num_var])
        segments.append(["N1", "RI", company_name_var, "91", "600850213"])
        segments.append(["N3", ((address_1_var if (address_1_var != None) else "") + " " + (address_2_var if (address_2_var != None) else "")).rstrip()])
        segments.append(["N4", city_var, state_var, zip_var])
    elif vn_value_var == "71341601":
        segments.append(["N1", "ST", ship_to_name, "93"] + ([cus_po_var[-4:]] if (cus_po_var and len(cus_po_var) >=4) else [""]))
        segments.append(["N3", (ship_to_address_1_var + " " + (ship_to_address_2_var[:-4] if (ship_to_address_2_var != None and len(ship_to_address_2_var) >= 4 and ship_to_address_2_var[:-3] == "PO#") else ship_to_address_2_var).rstrip())])
        segments.append(["N4", ship_to_city_var, ship_to_st_var, ship_to_zip_var])
    else:
        segments.append(["N1", "BT", bill_to_name_var, "92", bill_to_num_var])
        segments.append(["N1", "ST", ship_to_name, "91", ghx_ship_to_num_var])
        segments.append(["N1", "R1", company_name_var, "91", "600850213"])
        segments.append(["N3", ((address_1_var if (address_1_var != None) else "") + " " + (address_2_var if (address_2_var != None) else "")).rstrip()])
        segments.append(["N4", city_var, state_var, zip_var])
    segments.append(["N1", "VN", company_name_var, "92", vn_value_var]) if (vn_value_var != None and vn_value_var != "71341601") else None
    segments.append(["N1", "SN", "", "92", sn_value_var]) if (sn_value_var != None) else None
    if ghx_ship_to_num_var[:4] == "6125" and len(ghx_ship_to_num_var) == 6:
        segments.append(["ITD", "ZZ", "3", "", "", ""]) if (discount_pct_var == None or discount_pct_var == '' or discount_pct_var == 0) else segments.append(["ITD", "ZZ", "3", discount_pct_var, "", discount_days_var])
    else:
        curr_segment = ["ITD"] + (["03"] if "71341601" else ["ZZ"]) + ["3"] + (["", "", ""] if (discount_pct_var == '' or discount_pct_var == None or discount_pct_var == 0) else [discount_pct_var, (curr_date + timedelta(days=(int(days_var) if days_var else 0))).strftime("%Y%m%d"), discount_days_var])
        curr_segment += [(curr_date + timedelta(days=(int(days_var) if days_var else 0))).strftime("%Y%m%d"), days_var]
        segments.append(curr_segment + [""]) if (discount_pct_var == '' or discount_pct_var == None or discount_pct_var == 0) else [int(discount_pct_var * order_total_var)]

    segments.append(["DTM", "011", shipped_date_var.strftime("%Y%m%d") if (shipped_date_var != None) else curr_date.strftime("%Y%m%d")])
    segments.append(["FOB", "PP", "DE"]) if vn_value_var == "71341601" else None
    #LOOP 
    line_items_counter = 0
    for row in pd_detail.iterrows():
        curr_segment = []
        line_items_counter += 1
        if order_details_line_sum_qty_var > 0:
            curr_segment += ["IT1", ghx_line_id_var, order_details_line_sum_qty_var] + ([ghx_uom_var] if (ghx_uom_var != None) else [typenex_uom_var if (typenex_uom_var != None) else "CA"]) 
            curr_segment += [order_details_case_price_var, ""] + (["VA", vendor_id_var] if vn_value_var == "71341601" else ["VC", order_details_product_id_var])
            if ghx_ship_to_num_var[:4] == "6125" and len(ghx_ship_to_num_var) == 6:
                segments.append(curr_segment + [""])
            else:
                if buyer_id_var != None:
                    segments.append(curr_segment + [(buyer_id_identifier_var if (buyer_id_identifier_var != None) else "IN")] + ([buyer_id_var, "PL", ghx_line_id_var] if "VN00106821" else [buyer_id_var]))
                else:
                    segments.append(curr_segment)
            segments.append(curr_segment + ["PID", "F", "", "", "", (order_details_product_desc_var[:80] if (order_details_product_desc_var and len(order_details_product_desc_var) >= 80) else order_details_product_desc_var)]) if vn_value_var == "71341601" else None
    curr_segment = ["TDS", int(100*(order_total_var + ship_total_var + (mini_order_surcharge_var if (mini_order_surcharge_var != None) else 0)) + saturday_surcharge_var + drop_ship_surcharge_var + (sales_tax_var if (sales_tax_var != None) else 0))]
    if ghx_ship_to_num_var[:4] == "6125" and len(ghx_ship_to_num_var) == 6:
        segments.append(curr_segment + [""])
    else:
        segments.append(curr_segment + [""]) if (discount_pct_var == '' or discount_pct_var == None or discount_pct_var == 0) else segments.append(curr_segment + ["", "", int(discount_pct_var * int(order_total_var))])
    if sales_tax_var != None and sales_tax_var > 0:
        segments.append(["TXI", "ST", sales_tax_var])
    if ship_total_var + saturday_surcharge_var + drop_ship_surcharge_var > 0:
        curr_segment = ["SAC", "C"] + (["D500"] if (vn_value_var == "VN00106821" or vn_value_var == "71341601") else ["G830"]) + ["", "", 100*(ship_total_var+saturday_surcharge_var+drop_ship_surcharge_var), "", "", "", "", "", "", "06", "", "" ]
        segments.append(curr_segment + (["Handling"] if (vn_value_var == "VN00106821" or vn_value_var == "71341601") else ["Shipping and Handling Charge"]) )
    if mini_order_surcharge_var > 0:
        segments.append(["SAC", "C", "G970", "", "", 100*mini_order_surcharge_var, "", "", "", "", "", "", "06", "", ""] + (["Small Order Charge"] if (vn_value_var == "VN00106821" or vn_value_var == "71341601") else ["Mini Order Charge"]))
    segments.append(["CTT", line_items_counter])
    segments.append(["SE", len(segments)-1, (str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num))])
    segments.append(["GE", "1", (str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num))])
    segments.append(["IEA", "1", (str(inter_con_num)[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))])  + str(inter_con_num))])
    edi_text = ""
    for segment in segments:
        for field in segment:
            edi_text += str(field) + "^"
        edi_text = edi_text[:-1]
        edi_text += "~"
    with open(f"./810_Test_Files/{orderid_var}.txt", "w") as f:
        f.write(edi_text)
