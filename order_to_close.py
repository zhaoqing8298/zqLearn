# -*- coding: utf-8 -*-

import sys
import hashlib
import json
import datetime

import openpyxl

import psycopg2

DB_Host = '192.168.1.117'
DB_Port = 5432
DB_Name = "Seeed_ERP"
DB_User = "erpreadonly"
DB_Pwd = "seeederp2015"

Start_Date = "2016-01-01"
End_Date = "2016-03-15"

"""
不管订单状态，直接比较哪些订单的扫描数量
"""

def _get_db_cr():
    conn = psycopg2.connect(host=DB_Host, port = DB_Port, database=DB_Name, user=DB_User, password=DB_Pwd)
    cr = conn.cursor()
    return cr

def _get_employee_from_oe():
    result = []
    cr = _get_db_cr()
    employee_sql = cr.mogrify(""" select id, name from hr_employee_view """)
    cr.execute(employee_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _convert_employee(raw):
    result = {}
    for one in raw:
        employee_id = one[0]
        if employee_id:
            result[employee_id] = one[1]
    return result
    

def _get_saleorder_from_oe():
    result = []
    cr = _get_db_cr()
    start_day = datetime.datetime.strptime(Start_Date, "%Y-%m-%d")
    end_day = datetime.datetime.strptime(End_Date, "%Y-%m-%d")
    order_sql = cr.mogrify(""" select id, name, state, remark, is_wholesale, create_date from esale_zencart_saleorder where state not in ('done') and create_date>%s and create_date<%s""", (start_day, end_day))
    cr.execute(order_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _convert_order(raw):
    result = {}
    for one in raw:
        order_id = one[0]
        if order_id:
            result[order_id] = {
                "name":one[1],
                "state":one[2],
                "remark":one[3],
                "is_wholesale":one[4],
                "date":one[5],
            }
    return result

def _get_order_ids(raw_order):
    all_ids = []
    for one in raw_order:
        all_ids.append(one[0])
    return tuple(all_ids)

def _get_saleorder_lines_from_oe(order_ids):
    result = []
    cr = _get_db_cr()
    orderline_sql = cr.mogrify(""" select order_id, product_id, quantity, name, model from esale_zencart_saleorder_line where order_id in %s""", (order_ids,))
    cr.execute(orderline_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _convert_orderline(raw):
    result = {}
    for one in raw:
        order_id = one[0]
        if order_id:
            if order_id not in result:
                result[order_id] = []
            result[order_id] += [{
                "product_id":one[1],
                "buy_qty": one[2],
                "name":one[3],
                "sku":one[4],
            }]
    return result

def _get_move_from_oe(order_ids):
    result = []
    cr = _get_db_cr()
    move_sql = cr.mogrify(""" select zen_sale_order_id, product_id, product_qty from stock_move where state in ('assigned', 'confirmed') and zen_sale_order_id in %s""", (order_ids,))
    cr.execute(move_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _convert_move(raw):
    result = {}
    for one in raw:
        order_id = one[0]
        if order_id:
            if order_id not in result:
                result[order_id] = []
            result[order_id] += [{
                "product_id":one[1],
                "hold_qty": one[2],
            }]
    return result

def _get_delivery_from_oe(order_ids):
    result = []
    cr = _get_db_cr()
    delivery_sql = cr.mogrify(""" select saleorder_id, id, name, state, applicant from stock_seeed_delivery where saleorder_id in %s order by create_date desc""", (order_ids,))
    cr.execute(delivery_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _get_delivery_ids(raw_delivery):
    all_ids = []
    for one in raw_delivery:
        all_ids.append(one[1])
    return tuple(all_ids)

def _convert_delivery(raw):
    result = {}
    for one in raw:
        order_id = one[0]
        if order_id:
            if order_id not in result:
                result[order_id] = []
            result[order_id] += [{
                "delivery_id":one[1],
                "name":one[2],
                "state":one[3],
                "employee":one[4],
            }]
    return result

def _get_scan_from_oe(delivery_ids):
    result = []
    cr = _get_db_cr()
    scan_sql = cr.mogrify(""" select delivery_id, product_id,  product_qty from stock_scan_record where delivery_id in %s""", (delivery_ids,))
    cr.execute(scan_sql)
    result = cr.fetchall()
    cr.close()
    return result

def _convert_scan(raw):
    result = {}
    for one in raw:
        delivery_id = one[0]
        if delivery_id:
            if delivery_id not in result:
                result[delivery_id] = []
            result[delivery_id] += [{
                "product_id":one[1],
                "scan_qty":one[2],
            }]
    return result

def _treat_line(order_id, orderlines, deliverys, scan_map, moves):
    """
    IMPORTANT:如果有合并发货请求的，可能会出错!
    """
    delivery_ids = [one_delivery['delivery_id'] for one_delivery in deliverys]
    delivery_name = ""
    delivery_state = ""
    delivery_employee = False
    if deliverys:
        delivery_name = deliverys[0]["name"]
        delivery_state = deliverys[0]["state"]
        delivery_employee = deliverys[0]["employee"]
    ## 1. 统计全部扫描数量 
    scan_product = {}
    for one_delivery_id in delivery_ids:
        if one_delivery_id not in scan_map:
            continue
        for one_scan in scan_map[one_delivery_id]: 
            scan_pid = one_scan["product_id"]
            scan_product_qty = one_scan["scan_qty"]
            if scan_pid not in scan_product:
                scan_product[scan_pid] = 0
            scan_product[scan_pid] += scan_product_qty
    ## 2. 统计全部占用数量
    move_product = {}
    for one_move in moves:
        move_pid = one_move["product_id"]
        hold_qty = one_move["hold_qty"]
        if move_pid not in move_product:
            move_product[move_pid] = 0
        move_product[move_pid] += hold_qty
    ## 3. 统计全部需求数量
    need_product = {}
    pname_map = {}
    for one_line in orderlines:
        need_pid = one_line["product_id"]
        need_product_qty = one_line["buy_qty"]
        pname_map[need_pid] = one_line["sku"] 
        if need_pid not in need_product:
            need_product[need_pid] = 0
        need_product[need_pid] += need_product_qty
    ## 4. 统计未发数量与占用数量
    rest_product = {}
    for one_pid in need_product:
        request_qty = need_product[one_pid]
        this_hold_qty = 0
        this_scan_qty = 0
        if one_pid in move_product:
            this_hold_qty = move_product[one_pid]
        if one_pid in scan_product:
            this_scan_qty = scan_product[one_pid]
        unscan_qty = request_qty - this_scan_qty
        if unscan_qty>0 or this_hold_qty>0:
            rest_product[one_pid] = [unscan_qty, this_hold_qty]
    ## 5. 转换成最终结果
    result = {}
    for one_pid in rest_product:
        pname = pname_map[one_pid]
        result[pname] = [rest_product[one_pid][0], rest_product[one_pid][1], delivery_name, delivery_state, delivery_employee]
    return result

def _get_final_result(one_order, lines):
    result = []
    is_first = True
    for one_pname in lines:
        if is_first:
            employee_name = ""
            if lines[one_pname] and lines[one_pname][4]:
                employee_name = employee_map[lines[one_pname][4]]
            result += [(
                one_order['name'],
                one_order['date'],
                one_order['state'],
                one_pname,
                ## 此处第一个and是否有必要？？值得商榷
                lines[one_pname] and lines[one_pname][0] or 0,
                lines[one_pname] and lines[one_pname][1] or 0,
                one_order['remark'],
                one_order['is_wholesale'],
                lines[one_pname] and lines[one_pname][2] or "",
                lines[one_pname] and lines[one_pname][3] or "",
                employee_name,
            )]
            is_first = False
        else:
            result += [(
                "",
                "",
                "",
                one_pname,
                lines[one_pname][0],
                lines[one_pname][1],
                "",
                "",
                "",
                ""
            )]
    return result



if __name__ == '__main__':
    raw_employee = _get_employee_from_oe()
    employee_map = _convert_employee(raw_employee)
    raw_order_data = _get_saleorder_from_oe()
    print "1. 获取线上订单数据完成."
    
    orders_map = _convert_order(raw_order_data)
    print "2. 转换订单结构成功"
    
    order_ids = _get_order_ids(raw_order_data)
    print "3. 解析订单ID成功"

    raw_orderline_data = _get_saleorder_lines_from_oe(order_ids)
    print "4. 获取订单明细数据完成."

    orderlines_map = _convert_orderline(raw_orderline_data)
    print "5. 转换订单明细结构成功"
    
    raw_move_data = _get_move_from_oe(order_ids)
    print "6. 获取库存占用成功"

    move_map = _convert_move(raw_move_data)
    print "7. 转换库存占用结构成功"

    raw_delivery_data = _get_delivery_from_oe(order_ids)
    print "8. 获取发货请求成功"

    delivery_map = _convert_delivery(raw_delivery_data)
    print "9. 转换发货请求结构成功"
    
    delivery_ids = _get_delivery_ids(raw_delivery_data)
    print "10. 解析发货请求ID成功"

    raw_scan_data = _get_scan_from_oe(delivery_ids)
    print "11. 获取扫描记录成功"
    
    scan_map = _convert_scan(raw_scan_data)
    print "12. 转换扫描记录数据完成."

    print "13. 开始对比..."
    final_filename = "to_close.xlsx"
    book = openpyxl.Workbook(optimized_write = True)
    new_sheet = book.create_sheet()
    count = 1
    new_sheet.append(("单号", "日期", "订单状态", "产品名称", "未扫描数量", "占用数量", "订单备注", "是否零售", "最后发货请求", "发货请求状态", "发货请求提起人" ))
    for one_id in order_ids:
        orderlines = []
        deliverys = []
        moves = []
        if one_id in orderlines_map:
            orderlines = orderlines_map[one_id]
        if one_id in delivery_map:
            deliverys = delivery_map[one_id]
        if one_id in move_map:
            moves = move_map[one_id]
        rest_product = _treat_line(one_id, orderlines, deliverys, scan_map, moves)
        if rest_product:
            one_row_data = _get_final_result(orders_map[one_id], rest_product)
            for i in one_row_data:
                new_sheet.append(i)
            print "已处理" + str(count) + "个订单"
            count += 1
    book.save(filename=final_filename)            
    print "aha, 处理完成!"
    
