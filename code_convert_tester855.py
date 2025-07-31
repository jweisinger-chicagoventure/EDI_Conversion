#Conversion from Oracle Data -> EDI File (850)
import oracledb
import pandas as pd
from datetime import datetime, timedelta
import os

version = input("Please type the type of conversion (TOPS, GHX, FAX)")
if version != "GHX" and version != "FAX":
    version = "TOPS" #default version

os.makedirs(f"./855_Test_Files/{version}", exist_ok=True)
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
    # dict_table = pd_order.to_dict(orient="index")
    created_user_var = pd_order.loc[i, "CREATED_USER"] #CREATED_USER_VAR
    ghx_orderid_var = pd_order.loc[i, "GHX_ORDERID"] #GHX_ORDERID_VAR
    order_date_var = pd_order.loc[i, "ORDER_DATE"] #ORDER_DATE_VAR
    order_status_var = pd_order.loc[i, "ORDER_STATUS"] #ORDER_STATUS_VAR
    shipping_status_var = pd_order.loc[i, "SHIPPING_STATUS"] #SHIPPING_STATUS_VAR
    orderid_var = pd_order.loc[i, "ORDER_ID"] #ORDERID_VAR
    shipped_date_var = pd_order.loc[i, "SHIPPED_DATE"] #SHIPPED_DATE_VAR
    ship_to_num_var = pd_order.loc[i, "SHIP_TO_#"].iloc[0] #SHIP_TO_#_VAR
    cus_po_var = pd_order.loc[i, "CUS_PO"] #CUS_PO_VAR
    ship_to_name = pd_order.loc[i, 'SHIP_TO_NAME'].iloc[0]
    bill_overdue = pd_order.loc[i, 'BILL_OVER_DUE'] if type(pd_order.loc[0, 'BILL_OVER_DUE']) == str else pd_order.loc[0, 'BILL_OVER_DUE'].iloc[0] #BILL_OVER_DUE_VAR
    shipping_type_var = pd_order.loc[i, 'SHIPPING_TYPE'] #SHIPPING_TYPE_VAR
    split_order_id_var = pd_order.loc[i, 'SPLIT_ORDER_ID'] #SPLIT_ORDER_ID_VAR
    bill_to_num_var = pd_order.loc[i, 'BILL_TO_#'].iloc[0] #BILL_TO_NUM_VAR
    
    continue_var = False
    if version == "GHX":
        continue_columns, continue_table = retrieve(f"SELECT COUNT(*) FROM ORDER_HEADERS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND UPPER(CUS_PO) = UPPER('{cus_po_var}') and ORDER_STATUS != 'CANCEL' AND SHIPPING_STATUS != 'CANCEL'") #COUNT_VAR
        pd_continue = pd.DataFrame(continue_table, columns=continue_columns)
        if pd_continue.empty:
            continue_var = True
    if continue_var:
        continue

    split_order_id_var = pd_order.loc[0, 'SPLIT_ORDER_ID'] #SPLIT_ORDER_ID_VAR

    ghx_header_columns, ghx_header_table = retrieve(f"SELECT * FROM GHX_HEADERS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND SHIP_TO_# = '{ship_to_num_var}'")
    pd_ghx = pd.DataFrame(ghx_header_table, columns=ghx_header_columns)
    ghx_ship_to_num_var = pd_ghx.loc[0, "GHX_SHIP_TO_#"] if not pd_ghx.empty else '' #GHX_SHIP_TO_#_VAR
    ghx_ship_to_name_var = pd_ghx.loc[0, "SHIP_TO_NAME"] if not pd_ghx.empty else '' #GHX_SHIP_TO_NAME_VAR
    po_date_var = pd_ghx.loc[0, "PO_DATE"] if not pd_ghx.empty else '' #PO_DATE_VAR
    vn_value_var = pd_ghx.loc[0, "VN_VALUE"] if not pd_ghx.empty else '' #VN_VALUE_VAR
    sn_value_var = pd_ghx.loc[0, "SN_VALUE"] if not pd_ghx.empty else '' #SN_VALUE_VAR
    rush_order_var = pd_ghx.loc[0, 'RUSH_ORDER'] if not pd_ghx.empty else '' #RUSH_ORDER_VAR
    test_or_prod_var = "GHX_TEST" if ((pd_ghx.loc[0, "TEST_OR_PROD"] if not pd_ghx.empty else '') == "T") else "GHX" #TEST_OR_PROD_VAR
    ref_po_var = pd_ghx.loc[0, "REF_PO"] if not pd_ghx.empty else ''
    ref_co_var = pd_ghx.loc[0, "REF_CO"] if not pd_ghx.empty else ''
    ref_qc_var = pd_ghx.loc[0, "REF_QC"] if not pd_ghx.empty else ''

    line_counter = 0
    ghx_detail_columns, ghx_detail_table = retrieve(f"SELECT * FROM GHX_DETAILS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND LINE_ID IN (SELECT GHX_LINEID FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}') ORDER BY LINE_ID ASC")
    pd_detail = pd.DataFrame(ghx_detail_table, columns=ghx_detail_columns)
    if pd_detail.shape[0] > 0:
    
        line_counter += 1 if not pd_detail.empty else 0 
        ghx_line_id_var = pd_detail.loc[0, "LINE_ID"] if not pd_detail.empty else '' #GHX_LINE_ID_VAR
        ghx_uom_var = pd_detail.loc[0, "UOM"] if not pd_detail.empty else '' #GHX_UOM_VAR
        ghx_qty_var = pd_detail.loc[0, "QTY"] if not pd_detail.empty else '' #QTY_VAR
        buyer_id_identifier_var = pd_detail.loc[0, "BUYER_ID_IDENTIFIER"] if not pd_detail.empty else '' #BUYER_ID_IDENTIFIER_VAR
        buyer_id_var = pd_detail.loc[0, "BUYER_ID"] if not pd_detail.empty else '' #BUYER_ID_VAR
        ghx_case_price_var = pd_detail.loc[0, "CASE_PRICE"] if not pd_detail.empty else '' #CASE_PRICE_VAR
        mapped_vendor_var = pd_detail.loc[0, "MAPPED_VENDOR_ID"] if not pd_detail.empty else '' #MAPPED_VENDOR_VAR
        vendor_id_var = pd_detail.loc[0, "VENDOR_ID"] if not pd_detail.empty else '' #VENDOR_ID

        if version == "TOPS" or version == "GHX":
            order_detail_columns, order_detail_table = retrieve(f"SELECT MAX(PRODUCT_ID) as PRODUCT_ID, MAX(CASE_PRICE) as CASE_PRICE, SUM(QTY) as QTY, MAX(REPLACE(PRODUCT_DESC,'|','-')) as PRODUCT_DESC, SUM(QTY) as QTY,  MAX(BACKORDER_LINE_FLAG) as BACKORDER_LINE  FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}'")
            pd_order_detail = pd.DataFrame(order_detail_table, columns=order_detail_columns)
        elif version == "FAX":
            order_detail_columns, order_detail_table = retrieve(f"SELECT NVL(GHX_LINEID, LINE_ID) as LINE_ID, ORDER_DETAILS.PRODUCT_ID as PRODUCT_ID, REPLACE(ORDER_DETAILS.PRODUCT_DESC,'|','-') as PRODUCT_DESC, UOM, CASE_PRICE, QTY, BACKORDER_LINE_FLAG FROM ORDER_DETAILS, PRODUCTS WHERE ORDER_ID = '{orderid_var}' AND ORDER_DETAILS.PRODUCT_ID = PRODUCTS.PRODUCT_ID")
            pd_order_detail= pd.DataFrame(order_detail_table, columns=order_detail_columns)
            order_details_uom_var = pd_order_detail.loc[0, "UOM"] if not pd_order_detail.empty else '' #GHX_UOM_VAR
            order_details_qty_var = pd_order_detail.loc[0, "QTY"] if not pd_order_detail.empty else '' #QTY_VAR
            order_details_line_id_var = pd_order_detail.loc[0, 'LINE_ID'] #LINE_ID_VAR
            order_details_backorder_line = pd_order_detail.loc[0, "BACKORDER_LINE_FLAG"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_BACKORDER_LINE

        order_details_case_price_var = pd_order_detail.loc[0, "CASE_PRICE"] if not pd_order_detail.empty else '' #ORDER_DETAILS_CASE_PRICE_VAR
        order_details_line_sum_qty_var = pd_order_detail.loc[0, "QTY"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_LINE_SUM_QTY_VAR
        order_details_product_id_var = pd_order_detail.loc[0, "PRODUCT_ID"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_PRODUCT_ID_VAR
        order_details_product_desc_var = pd_order_detail.loc[0, "PRODUCT_DESC"] if not pd_order_detail.empty else ''  #ORDER_DETAILS_PRODUCT_DESC_VAR
        order_detail_count = retrieve(f"SELECT COUNT(*) FROM ORDER_DETAILS WHERE GHX_ORDERID = '{ghx_orderid_var}' AND GHX_LINEID = '{ghx_line_id_var}'")[1][0][0]
        order_detail_count2 = retrieve(f"SELECT COUNT(*) FROM PRODUCTS WHERE PRODUCT_ID = '{mapped_vendor_var}' AND VALID_FOR_SALE = '1'")[1][0][0]
        count_package_var  = retrieve(f"SELECT COUNT(DISTINCT TRACK_NO) FROM ORDER_TRACKS WHERE ORDER_ID = '{orderid_var}'") #COUNT_PACKAGE_VAR
        total_weight_package_var  = retrieve(f"SELECT SUM(LINE_WEIGHT) FROM ORDER_DETAILS WHERE ORDER_ID = '{orderid_var}'") # TOTAL_WEIGHT_PACKAGE_VAR
        typenex_uom_var = None
        if order_details_product_id_var != None:
            typenex_uom_var = retrieve(f"SELECT COUNT(*) FROM PRODUCTS WHERE PRODUCT_ID = '{order_details_product_id_var}'")
        segment_counter = 2
        segments = []
        switch_var = created_user_var if (version == "TOPS") else test_or_prod_var
        bak_var = ("06" if (version == "GHX" or version == "FAX") else ("00" if (vn_value_var == "VN00106821" or vn_value_var == "71341601") else "06"))
        bak_var2 = ("AC" if (version == "GHX" or version == "FAX") else ("AD" if (vn_value_var == "VN00106821") else "AC"))
        date_var = (order_date_var if (version == "FAX") else po_date_var)
        order_var = (ghx_orderid_var if (version == "GHX" or version == "FAX") else orderid_var)
        
        ghx_status_code =  "IR" #GHX_STATUS_CODE
        veyer_ack08_msg = "IB Backorder" #VEYER_ACK08_MSG
        curr_date = datetime.now()
        segments.append(["ISA", "00", "          ", "00", "          ", "01", "600850213      ", 
        "ZZ", (str(switch_var)[:15] if (len(switch_var) >= 15) else str(switch_var) + ("").join([' ' for i in range(15-len(switch_var))])), 
        str(curr_date.strftime('%y%m%d')), str(curr_date.strftime('%H%M')), "U", "00401", (inter_con_num[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))]) + str(inter_con_num)), "0", "P", "|"])
        segments.append(["GS", "PR", "600850213", switch_var, str(curr_date.strftime('%Y%m%d')), str(curr_date.strftime('%H%M')), str(group_con_num)[:9] if len(str(group_con_num))>= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num), "X", "004010"])
        segments.append(["ST", "855", (str(group_con_num)[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))])  + str(group_con_num))])    
        segments.append(["BAK", bak_var, bak_var2, cus_po_var,  (date_var.strftime("%Y%m%d") if date_var else "") , "", "", "",  order_var, str(curr_date.strftime('%Y%m%d'))])
        segments.append(["REF", "OQ", ghx_orderid_var]) if ghx_orderid_var != None else None
        if version == "TOPS":
            segments.append(["REF", "PO", ref_po_var]) if ref_po_var != None else None
            if vn_value_var == "71341601":
                segments.append(["REF", "CO", ref_co_var]) if ref_co_var != None else None
                segments.append(["REF", "QC", ref_qc_var]) if ref_qc_var != None else None
                segments.append(["N1", "ST", ship_to_name, "91", ghx_ship_to_num_var])
            else:
                segments.append(["N1", "ST"] + ([ghx_ship_to_name_var, "92"] if vn_value_var == "0000113971" else ["", "91"]) + [(ghx_ship_to_num_var if ghx_ship_to_num_var != None else ship_to_num_var)])
            if vn_value_var != None and vn_value_var != "71341601" and vn_value_var != "0000113971":
                segments.append(["N1", "SF", company_name_var, "92", vn_value_var])
                segments.append(["N3", address_1_var])
                segments.append(["N4", city_var, state_var, zip_var])
            if vn_value_var != None and vn_value_var != "71341601":
                segments.append(["N1", "VN", company_name_var, "92", vn_value_var])
                if vn_value_var != "0000113971":
                    segments.append(["N3", address_1_var])
                    segments.append(["N4", city_var, state_var, zip_var])
        elif version == "GHX" or version == "FAX":
            segments.append(["N1", "ST", "", "91", (ghx_ship_to_num_var if (ghx_ship_to_num_var != None) else ship_to_num_var)])
            segments.append(["N1", "BT", "", "91", bill_to_num_var]) if (version == "FAX") else None
            segments.append(["N1", "VN", company_name_var, "92", vn_value_var]) if vn_value_var != None else None
        segments.append(["N1", "SN", "", "92", sn_value_var]) if sn_value_var != None else None
        line_items_counter = 0
        if version == "TOPS":
            for row in pd_detail.iterrows():
                line_items_counter += 1
                segments.append(["PO1", ghx_line_id_var, ghx_qty_var, ghx_uom_var, order_details_case_price_var, "", ("VA" if vn_value_var == "71341601" else "VC"), vendor_id_var] + ([(buyer_id_identifier_var if buyer_id_identifier_var != None else "IN"), buyer_id_var] if (buyer_id_var != None) else []))
                if order_detail_count <= 0:
                    if order_detail_count == 0:
                        segments.append(["ACK", "IR", ghx_qty_var, ghx_uom_var, "", "", "", "ZZ", "I5 Product Has Been Discontinued"]) if vn_value_var == "71341601" else segments.append(["ACK", "IR", ghx_qty_var, ghx_uom_var])
                else:
                    segments.append((["PID", "F", "", "", ""] + ([order_details_product_desc_var + "-CREDIT HOLD"] if bill_overdue == "Y" else [order_details_product_desc_var])))
                    if order_status_var == "OPEN" and shipping_status_var == "BACKORDER" and order_details_backorder_line == 1:
                        if vn_value_var == "71341601":
                            ghx_status_code = "IR"
                            veyer_ack08_msg = "IB Backorder"
                        else:
                            ghx_status_code = "IB"
                    else:
                        if bill_overdue == "Y" or order_status_var == "OPEN" and shipping_status_var == "BACKORDER":
                            if vn_value_var == "VN00106821":
                                ghx_status_code = "IB"
                            elif vn_value_var == "71341601":
                                ghx_status_code = "IR"
                                veyer_ack08_msg = "IB Backorder"
                            else:
                                ghx_status_code = "IH"
                        else:
                            ghx_status_code = "IA"
                            if order_status_var == "OPEN" and shipping_status_var == "COMMITTED":
                                if vn_value_var == "VN00106821" or vn_value_var == "71341601":
                                    ghx_status_code = "IA"
                                    veyer_ack08_msg = ""
                                else:
                                    ghx_status_code = "AR"
                            if order_status_var == "COMPLETE" and shipping_status_var == "SHIPPED":
                                if vn_value_var == "VN00106821" or vn_value_var == "71341601":
                                    ghx_status_code = "IA"
                                    veyer_ack08_msg = ""
                                else:
                                    ghx_status_code = "AC"
                            if ghx_uom_var != typenex_uom_var:
                                if vn_value_var == "VN00106821" or vn_value_var == "71341601":
                                    ghx_status_code = "IA"
                                    veyer_ack08_msg = ""
                                else:
                                    ghx_status_code = "IC"
                            if ghx_case_price_var != order_details_case_price_var:
                                ghx_status_code = "IP"
                                if vn_value_var == "71341601":
                                    ghx_status_code = "IA"
                                    veyer_ack08_msg = ""
                                if vn_value_var == None or vn_value_var != "VN00106821" or vn_value_var != "71341601":
                                    if ghx_uom_var != typenex_uom_var:
                                        ghx_status_code = "IC"
                            if vendor_id_var != order_details_product_id_var:
                                if vn_value_var == "71341601":
                                    ghx_status_code = "IR"
                                    veyer_ack08_msg = "I4 Bad SKU"
                                else:
                                    ghx_status_code = "IS"
                            if ghx_qty_var != order_details_line_sum_qty_var:
                                if split_order_id_var != None:
                                    ghx_status_code = "BP"
                                else:
                                    ghx_status_code = "IQ"
                    if (ghx_status_code != "IB" and ghx_status_code != "IH") and (vn_value_var != "71341601" or vn_value_var == None) or (ghx_status_code == "IH" or ghx_status_code == "IB") and vn_value_var == "0000113971" or veyer_ack08_msg == None and vn_value_var == "71341601":
                        current_segment = ["ACK"] + [ghx_status_code, order_details_line_sum_qty_var, ghx_uom_var] + (["067"] if vn_value_var == "VN00106821" else []) +  (["068"] if vn_value_var == "0000113971" else []) +  (["083"] if vn_value_var == "71341601" else []) +  (["017"] if (vn_value_var != "VN00106821" and vn_value_var != "0000113971" and vn_value_var != "71341601") else [])
                        if (ghx_status_code == "IH" or ghx_status_code == "IB") and vn_value_var == "0000113971":
                            current_segment += [(shipped_date_var + pd.tseries.offsets.BusinessDay(20)).strftime('%Y%m%d') ]
                        else:
                            if rush_order_var == "RO":
                                current_segment += [(shipped_date_var + pd.tseries.offsets.BusinessDay(1)).strftime('%Y%m%d') ]  if shipping_type_var == "NEXT_DAY" else  [(shipped_date_var + pd.tseries.offsets.BusinessDay(2)).strftime('%Y%m%d') ] 
                            else:
                                current_segment += [(shipped_date_var + pd.tseries.offsets.BusinessDay(4)).strftime('%Y%m%d') ]
                        segments.append(current_segment + (["", "", ""] if vn_value_var == "71341601" else ["", "VC", order_details_product_id_var]))
                    else:
                        segments.append(["ACK", ghx_status_code, order_details_line_sum_qty_var, ghx_uom_var, "", "", "", "ZZ", veyer_ack08_msg]) if vn_value_var == "71341601" else segments.append([ghx_status_code, order_details_line_sum_qty_var, ghx_uom_var])
        elif version == "GHX":
            for row in pd_detail.iterrows():
                segments.append(["PO1", ghx_line_id_var, ghx_qty_var, ghx_uom_var, ghx_case_price_var, "", "VC", vendor_id_var] + (["IN", buyer_id_var] if (buyer_id_var != None) else []))
                segments.append(["ACK", "IR", ghx_qty_var, ghx_uom_var])
        elif version == "FAX":
            for row in pd_detail.iterrows():
                segments.append(["PO1", order_details_line_id_var, order_details_qty_var, order_details_uom_var, order_details_case_price_var, "", "VC", order_details_product_id_var] + (["IN", buyer_id_var] if (buyer_id_var != None) else []))
                segments.append((["PID", "F", "", "", ""] + ([order_details_product_desc_var + "-CREDIT HOLD"] if bill_overdue == "Y" else [order_details_product_desc_var])))
                if order_status_var == "OPEN" and shipping_status_var == "BACKORDER" and order_details_backorder_line == "1":
                    ghx_status_code = "IB"
                else:
                    if bill_overdue == "Y" or order_status_var == "OPEN" and shipping_status_var == "BACKORDER":
                        ghx_status_code = "IH"
                    else:
                        ghx_status_code = "IA"
                        if order_status_var == "OPEN" and shipping_status_var == "COMMITTED":
                            ghx_status_code = "AR"
                        if order_status_var == "COMPLETE" and shipping_status_var == "SHIPPED":
                            ghx_status_code = "AC"
                        if order_details_uom_var != order_details_uom_var:
                            ghx_status_code = "IC"
                if ghx_status_code != "IB" and ghx_status_code != "IH":
                    segments.append(["ACK", ghx_status_code, order_details_qty_var, order_details_uom_var, "017", (shipped_date_var + pd.tseries.offsets.BusinessDay(4)).strftime('%Y%m%d'), "", "VC", order_details_product_id_var])
                else:
                    segments.append(["ACK", ghx_status_code, order_details_qty_var, order_details_uom_var])
        segments.append(["CTT", line_items_counter])
        segments.append(["SE", len(segments)-1, (group_con_num[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))]) + str(group_con_num))])
        segments.append(["GE", 1, (group_con_num[:9] if len(str(group_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(group_con_num)))]) + str(group_con_num))])
        segments.append(["IEA", 1, (inter_con_num[:9] if len(str(inter_con_num)) >= 9 else ("").join(['0' for i in range(9-len(str(inter_con_num)))]) + str(inter_con_num))])
        edi_text = ""
        for segment in segments:
            for field in segment:
                edi_text += str(field) + "^"
            edi_text = edi_text[:-1]
            edi_text += "~"

        with open(f"./855_Test_Files/{version}/{orderid_var}.txt", "w") as f:
            f.write(edi_text)
