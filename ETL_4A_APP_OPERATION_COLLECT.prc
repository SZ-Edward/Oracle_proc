create or replace procedure ETL_4A_APP_OPERATION_COLLECT(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_province_code        char(3);            --省代码
  v_month_tmp            varchar2(8) := '';  --汇总月份
  v_begin_time           date;               --统计起始时间
  v_end_time             date;               --统计截止时间
  v_foura_login_value    number;             --4A侧登陆数量
  v_foura_op_value       number;             --4A侧操作数量
  v_app_login_value      number;             --CRM登陆数量
  v_app_op_value         number;             --CRM侧操作数量
  
  type cursor_op_collect is ref cursor;
  v_cursor_op_collect cursor_op_collect;

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
if i_province_code is not null then
  delete from rpt_4a_app_login_dy r where to_char(r.begin_time, 'yyyymm') = v_month_tmp;
  open v_cursor_op_collect for 
    select foura.province, to_date(foura.begin_time, 'yyyymm'), to_date(foura.end_time, 'yyyymm'), foura.dlvalue, foura.czvalue, app.dlvalue, app.czvalue
    from (select a.province, to_char(a.begin_time, 'yyyymm') begin_time, to_char(a.end_time, 'yyyymm') end_time,
              sum(a.dlvalue) dlvalue, sum(a.czvalue) czvalue
             from int_app_login_4a_dy a
             where a.province = i_province_code and a.rpt_month = v_month_tmp
             group by a.province, to_char(a.begin_time, 'yyyymm'), to_char(a.end_time, 'yyyymm')) foura,
            (select t.province, to_char(t.begin_time, 'yyyymm') begin_time, to_char(t.end_time, 'yyyymm') end_time,
              sum(t.dlvalue) dlvalue, sum(t.czvalue) czvalue
             from int_app_login_dy t
             where t.province = i_province_code and t.rpt_month = v_month_tmp
             group by t.province, to_char(t.begin_time, 'yyyymm'), to_char(t.end_time, 'yyyymm')) app
    where foura.province = app.province and foura.begin_time = app.begin_time and foura.end_time = app.end_time;
else
    delete from rpt_4a_app_login_dy r where to_char(r.begin_time, 'yyyymm') = v_month_tmp and r.province = i_province_code;
  open v_cursor_op_collect for 
    select foura.province, to_date(foura.begin_time, 'yyyymm'), to_date(foura.end_time, 'yyyymm'), foura.dlvalue, foura.czvalue, app.dlvalue, app.czvalue
    from (select a.province, to_char(a.begin_time, 'yyyymm') begin_time, to_char(a.end_time, 'yyyymm') end_time,
              sum(a.dlvalue) dlvalue, sum(a.czvalue) czvalue
             from int_app_login_4a_dy a
             where a.rpt_month = v_month_tmp
             group by a.province, to_char(a.begin_time, 'yyyymm'), to_char(a.end_time, 'yyyymm')) foura,
            (select t.province, to_char(t.begin_time, 'yyyymm') begin_time, to_char(t.end_time, 'yyyymm') end_time,
              sum(t.dlvalue) dlvalue, sum(t.czvalue) czvalue
             from int_app_login_dy t
             where t.rpt_month = v_month_tmp
             group by t.province, to_char(t.begin_time, 'yyyymm'), to_char(t.end_time, 'yyyymm')) app
    where foura.province = app.province and foura.begin_time = app.begin_time and foura.end_time = app.end_time;  
end if;
/*
********************************逻辑处理 begin********************************
*/ 
  --保证考核不重复（删除相同汇总条件的数据）
  if i_province_code is null then
    delete from rpt_4a_app_login_dy r
    where to_char(r.begin_time, 'yyyymm') = v_month_tmp;
  else
    delete from rpt_4a_app_login_dy r
    where to_char(r.begin_time, 'yyyymm') = v_month_tmp
    and r.province = i_province_code;
  end if;
  
loop
  fetch v_cursor_op_collect 
    into v_province_code, v_begin_time, v_end_time, v_foura_login_value, v_foura_op_value, v_app_login_value, v_app_op_value;
    
  exit when v_cursor_op_collect%notfound;
  
  insert into rpt_4a_app_login_dy(
    province, begin_time, end_time, num_4a_login, num_4a_operate, num_crm_login, num_crm_operate, sum_time
  ) values(
    v_province_code, v_begin_time, v_end_time, v_foura_login_value, v_foura_op_value, v_app_login_value, v_app_op_value, sysdate
  );   
end loop;

close v_cursor_op_collect;
/*
********************************逻辑处理 begin********************************
*/   
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;

END ETL_4A_APP_OPERATION_COLLECT;
