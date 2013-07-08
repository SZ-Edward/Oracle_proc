create or replace procedure ETL_FIREWALL_POLY_COLLECT(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_province_code char(3);            --省代码
  v_month_tmp     varchar2(8) := '';  --汇总月份
  v_num1          number(10);         --防火墙变更数
  v_num2          number(10);         --审批工单数
  v_num3          number(10);         --未审批防火墙变更数
  v_sql_str varchar(2000) := 'select t1.province, t1.cnt, t2.cnt, t3.cnt
  from (select d1.province, d1.rpt_month, ''1'' as st, count(*) as cnt
          from int_firewall_acl_order_dy d1
         where 1 = 1
         group by d1.province, d1.rpt_month) t1,
       (select d2.province, d2.rpt_month, ''2'' as st, count(*) as cnt
          from int_firewall_acl_order_mo d2
         where 1 = 1
         group by d2.province, d2.rpt_month) t2,
       (select d3.province, d3.rpt_month, ''3'' as st, count(*) as cnt
          from int_firewall_acl_order_dy d3
         where not exists (select ''1''
                  from int_firewall_acl_order_mo t
                 where t.order_id = d3.order_id)
         group by d3.province, d3.rpt_month) t3
 where t1.province = t2.province
   and t2.province = t3.province
   and t1.rpt_month = t2.rpt_month
   and t2.rpt_month = t3.rpt_month';
   
  type cursor_firewall_poly is ref cursor;
  v_cursor_firewall cursor_firewall_poly;
  
BEGIN
  /*
********************************参数校验 begin********************************
*/
  o_return_code := '0';
  o_return_msg  := '汇总成功';

  --判断汇总月份是否为空，为空则结束存储过程 
  if i_month is null then
    o_return_code := '-1';
    o_return_msg  := '汇总月份不能为空。';
    return;
  end if;

  --校验汇总月份的格式，不符合格式则结束存储过程
  begin
    select to_char(to_date(i_month, 'YYYYMM'), 'YYYYMM')
      into v_month_tmp
      from dual;
  Exception
    when others then
      o_return_code := '-1';
      o_return_msg  := '汇总月份的格式出错，格式为:YYYYMM。 ' || SQLERRM;
      return;
  end;
  
  --校验汇总省份的格式，不符合格式则结束存储过程
  begin    
    if i_province_code is not null then
      select to_number(i_province_code) into v_province_code from dual;
      if length(i_province_code) != 2 then
        o_return_code := '-1';
        o_return_msg  := '汇总省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
      end if;
    end if;
    Exception
      when others then
        o_return_code := '-1';
        o_return_msg  := '汇总省份的格式出错，格式为三位数字.。 ' || SQLERRM;
        return;
  end;
/*
********************************参数校验 end********************************
*/

/*
********************************逻辑处理 begin********************************
*/  
if i_province_code is not null then
  delete from rpt_int_firewall_acl_order_mo r where r.rpt_cycle = v_month_tmp and r.province = i_province_code;  
  v_sql_str := v_sql_str || ' and t1.province = ' || i_province_code || ' and t1.rpt_month = ' || v_month_tmp || '';
  open v_cursor_firewall for v_sql_str;    
else
  delete from rpt_int_firewall_acl_order_mo r where r.rpt_cycle = v_month_tmp;  
  v_sql_str := v_sql_str || ' and t1.rpt_month = ' || v_month_tmp || '';
  open v_cursor_firewall for v_sql_str;        
end if;

loop
  fetch v_cursor_firewall into v_province_code, v_num1, v_num2, v_num3;
  
  exit when v_cursor_firewall%notfound; --没有数据则结束循环
  
  insert into rpt_int_firewall_acl_order_mo(
    province, rpt_cycle, num1, num2, num3, sum_time
  ) values(
    v_province_code, v_month_tmp, v_num1, v_num2, v_num3, sysdate
  );   
end loop;

close v_cursor_firewall;
/*
********************************逻辑处理 end********************************
*/  
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;

END ETL_FIREWALL_POLY_COLLECT;
