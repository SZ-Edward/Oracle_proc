create or replace procedure PORTAL_SAFETY_SCANNING_COLLECT(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_month_tmp      varchar2(8) := ''; --汇总月份
  v_province_code  char(3);           --汇总省份
  v_type           varchar2(30);      --漏洞类别
  v_level          varchar2(30);      --风险等级
  v_status         char(1);           --操作标识
  v_num            number(8);         --条数
                     
  type cursor_collect is ref cursor;
  v_cursor_collect cursor_collect;  
  
/*
********************************参数校验 begin********************************
*/
BEGIN
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
  delete from rpt_scan_result_sum r
  where r.audit_cycle = v_month_tmp
  and r.province = i_province_code; 
  open v_cursor_collect for 
    select a.appcycle, b.hole_risk_level, b.hole_type, a.opflag, count(1)
    from int_safe_issue a, int_norm_leak_bank b
    where a.cmhole_id = b.id
    and a.appcycle = b.rpt_month
    and a.appcycle = v_month_tmp
    --and a.province = b.province
    --and a.province = i_province_code
    group by a.opflag, b.hole_risk_level, b.hole_type, a.appcycle;
else
  delete from rpt_scan_result_sum r
  where r.audit_cycle = v_month_tmp
  and r.province = i_province_code; 
  open v_cursor_collect for 
    select a.appcycle, b.hole_risk_level, b.hole_type, a.opflag, count(1)
    from int_safe_issue a, int_norm_leak_bank b
    where a.cmhole_id = b.id
    and a.appcycle = b.rpt_month
    and a.appcycle = v_month_tmp
    --and a.province = i_province_code
    group by a.opflag, b.hole_risk_level, b.hole_type, a.appcycle;      
end if;

loop
    fetch v_cursor_collect into v_month_tmp, v_level, v_type, v_status, v_num;
    exit when v_cursor_collect%notfound;
    
    v_province_code := null;
    
    insert into rpt_scan_result_sum(
      province, audit_cycle, weakness_type, weakness_level, weakness_status, num, sum_time
    ) values(
      v_province_code, v_month_tmp, v_type, v_level, v_status, v_num, sysdate
    );
end loop;
/*
********************************逻辑处理 end********************************
*/  
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
      
END PORTAL_SAFETY_SCANNING_COLLECT;
