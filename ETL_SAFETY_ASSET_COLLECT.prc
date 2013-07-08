create or replace procedure ETL_SAFETY_ASSET_COLLECT(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char,
	i_platform_type in varchar2
) is

  --临时变量
  v_province_code char(3);            --省代码
  v_month_tmp     varchar2(8) := '';  --汇总月份
  v_type          char(2);            --资源类型或系统类型
  v_num           number(8);          --资源数量
  v_flag          char(1);            --一致性标识
  v_res_state     varchar2(2);        --资产状态
  v_iskey         number(8);          --是否关键
  
  type cursor_collect is ref cursor;
  v_cursor_collect cursor_collect;  
  
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

  --判断汇总的平台类型是否为空，为空则结束存储过程
  if i_platform_type is null or
     i_platform_type != '4A' and i_platform_type != 'SMP' then
    o_return_code := '-1';
    o_return_msg  := '请输入要汇总的平台类型（4A/SMP）。';
    return;
  end if;
  
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
--保证考核不重复（删除相同考核条件的数据）
if i_province_code is not null then
  if i_platform_type = '4A' then
    delete from rpt_res_info_sum fa
    where fa.province = i_province_code
    and fa.rpt_cycle = v_month_tmp;
    open v_cursor_collect for
      select k.province, k.rpt_month, k.res_type, count(1), k.coincidence_flag 
      from int_cover_res_info_mo k
      where k.rpt_month = v_month_tmp and k.province = i_province_code
      group by k.province, k.rpt_month, k.res_type, k.coincidence_flag;
  elsif i_platform_type = 'SMP' then
    delete from rpt_smp_cover_security_res_sum smp
    where smp.province = i_province_code
    and smp.rpt_cycle = v_month_tmp;
    open v_cursor_collect for
      select s.province, s.rpt_month, s.system_type, count(1), s.coincidence_flag, s.res_state, s.iskey 
      from int_smp_cover_security_res_mo s
      where s.rpt_month = v_month_tmp and s.province = i_province_code
      group by s.province, s.rpt_month, s.system_type, s.coincidence_flag, s.res_state, s.iskey;  
  end if;
else
  if i_platform_type = '4A' then
    delete from rpt_res_info_sum fa
    where fa.rpt_cycle = v_month_tmp;
    open v_cursor_collect for
      select k.province, k.rpt_month, k.res_type, count(1), k.coincidence_flag
      from int_cover_res_info_mo k
      where k.rpt_month = v_month_tmp 
      group by k.province, k.rpt_month, k.res_type, k.coincidence_flag;
  elsif i_platform_type = 'SMP' then
    delete from rpt_smp_cover_security_res_sum smp
    where smp.rpt_cycle = v_month_tmp;
    open v_cursor_collect for
      select s.province, s.rpt_month, s.system_type, count(1), s.coincidence_flag, s.res_state, s.iskey 
      from int_smp_cover_security_res_mo s
      where s.rpt_month = v_month_tmp
      group by s.province, s.rpt_month, s.system_type, s.coincidence_flag, s.res_state, s.iskey;  
  end if;  
end if;

loop
  if i_platform_type = 'SMP' then
    fetch v_cursor_collect into v_province_code, v_month_tmp, v_type, v_num, v_flag, v_res_state, v_iskey;
    exit when v_cursor_collect%notfound; --没有数据则结束循环
    
    insert into rpt_smp_cover_security_res_sum(province, rpt_cycle, system_type, num, sum_time, coincidence_flag, res_state, iskey)
    values(v_province_code, v_month_tmp, v_type, v_num, sysdate, v_flag, v_res_state, v_iskey);
  elsif i_platform_type = '4A' then
    fetch v_cursor_collect into v_province_code, v_month_tmp, v_type, v_num, v_flag;
    exit when v_cursor_collect%notfound; --没有数据则结束循环
  
    insert into rpt_res_info_sum(province, rpt_cycle, res_type, num, sum_time, coincidence_flag)
    values(v_province_code, v_month_tmp, v_type, v_num, sysdate, v_flag);
  end if;
end loop;

close v_cursor_collect;
/*
********************************逻辑处理 end********************************
*/
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
    
END ETL_SAFETY_ASSET_COLLECT;
