create or replace procedure ETL_AUDIT_POLY_COLLECT(
  o_return_code   out varchar2,
  o_return_msg    out varchar2,
  i_month             in varchar2,
  i_province_code in char
) is

  --临时变量
  v_province_code  char(3);            --省代码
  v_month_tmp      varchar2(8) := '';  --汇总月份
  v_type           varchar2(2);        --策略类型
  v_num            number(8);          --资源数量
  v_strategy_name  varchar2(256);      --策略名称（审计点）
  v_audit_content  varchar2(2) := '';  --审计内容
  v_lowest_rate    varchar2(2) := '';  --最低审计频率
  v_audit_tool     varchar2(2) := '';  --审计工具
  
  type cursor_audit_collect is ref cursor;
  v_cursor_audit_collect cursor_audit_collect;

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
if i_province_code is null then
  delete from rpt_safe_audit_strategy_sum r where to_char(r.audit_cycle, 'yyyymm') = v_month_tmp;
  open v_cursor_audit_collect for
    select k.province, to_char(k.audit_cycle, 'yyyymm'), k.audit_type, k.strategy_name, count(1), k.audit_content, k.less_audit_rate, k.audit_tool 
    from imp_safe_audit_strategy k
    where to_char(k.audit_cycle, 'yyyymm') = v_month_tmp
    group by k.province, to_char(k.audit_cycle, 'yyyymm'), k.audit_type, k.strategy_name, k.audit_content, k.less_audit_rate, k.audit_tool;
else
  delete from rpt_safe_audit_strategy_sum r where r.audit_cycle = v_month_tmp and r.province = i_province_code;
  open v_cursor_audit_collect for
    select k.province, to_char(k.audit_cycle, 'yyyymm'), k.audit_type, k.strategy_name, count(1), k.audit_content, k.less_audit_rate, k.audit_tool  
    from imp_safe_audit_strategy k
    where to_char(k.audit_cycle, 'yyyymm') = v_month_tmp
    and k.province = i_province_code
    group by k.province, to_char(k.audit_cycle, 'yyyymm'), k.audit_type, k.strategy_name, k.audit_content, k.less_audit_rate, k.audit_tool;
end if;

loop
  fetch v_cursor_audit_collect 
    into v_province_code, v_month_tmp, v_type, v_strategy_name, v_num, v_audit_content, v_lowest_rate, v_audit_tool;
    
  exit when v_cursor_audit_collect%notfound; --没有数据则结束循环
  
  insert into rpt_safe_audit_strategy_sum(
    province, audit_cycle, audit_type, strategy_name, num, sum_time, audit_content, less_audit_rate, audit_tool
  )
  values(
    v_province_code, v_month_tmp, v_type, v_strategy_name, v_num, sysdate, v_audit_content, v_lowest_rate, v_audit_tool
  );
end loop;

close v_cursor_audit_collect;
/*
********************************逻辑处理 begin********************************
*/  
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
    
END ETL_AUDIT_POLY_COLLECT;
