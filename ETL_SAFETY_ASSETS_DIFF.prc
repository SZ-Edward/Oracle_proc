create or replace procedure ETL_SAFETY_ASSETS_DIFF (
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

	--临时变量
  v_month_tmp      varchar2(8) := ''; --汇总月份
  v_province_code  char(3);  					--省代码

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
--比较差异数据，4A侧和SMP侧都有数据且数据一致
  if i_province_code is null then      
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '0'  --设置4A侧一致性标识为0
        where exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
            where foura.province = smp.province
               and foura.rpt_month = smp.rpt_month
               and foura.begin_time = smp.begin_time
               and foura.end_time = smp.end_time
               and foura.seq = smp.seq
               and foura.res_type = '04'
               and smp.res_type = '03'
               and smp.system_type = '00' 
               and foura.res_name = smp.res_name
               and foura.res_address = smp.ip_adress
               and foura.rpt_month = v_month_tmp );
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '0'  --设置SMP侧一致性标识为0
        where exists ( select 1 from INT_COVER_RES_INFO_MO foura 
            where foura.province = smp.province
               and foura.rpt_month = smp.rpt_month
               and foura.begin_time = smp.begin_time
               and foura.end_time = smp.end_time
               and foura.seq = smp.seq
               and foura.res_type = '04'
               and smp.res_type = '03'
               and smp.system_type = '00' 
               and foura.res_name = smp.res_name
               and foura.res_address = smp.ip_adress
               and foura.rpt_month = v_month_tmp );
  else 
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '0'  --设置4A侧一致性标识为0
        where exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and foura.res_name = smp.res_name
             and foura.res_address = smp.ip_adress
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );  
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '0'  --设置SMP侧一致性标识为0
        where exists ( select 1 from INT_COVER_RES_INFO_MO foura 
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and foura.res_name = smp.res_name
             and foura.res_address = smp.ip_adress
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );  
  end if;

--比较差异数据，4A侧和SMP侧都有数据但数据不一致
  if i_province_code is null then      
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '1'  --设置4A侧一致性标识为1
        where exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and (foura.res_name != smp.res_name or foura.res_address != smp.ip_adress)
             and foura.rpt_month = v_month_tmp );
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '1'  --设置SMP侧一致性标识为1
        where exists ( select 1 from INT_COVER_RES_INFO_MO foura 
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and (foura.res_name != smp.res_name or foura.res_address != smp.ip_adress)
             and foura.rpt_month = v_month_tmp );                 
  else 
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '1'  --设置4A侧一致性标识为1
        where exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00'
             and (foura.res_name != smp.res_name or foura.res_address != smp.ip_adress)
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );  
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '1'  --设置SMP侧一致性标识为1
        where exists ( select 1 from INT_COVER_RES_INFO_MO foura 
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and (foura.res_name != smp.res_name or foura.res_address != smp.ip_adress)
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );  
  end if;

--比较差异数据，只有SMP侧有数据，4A侧无数据
  if i_province_code is null then      
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '2'  --设置SMP侧一致性标识为2
        where not exists ( select 1 from INT_COVER_RES_INFO_MO foura 
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and foura.rpt_month = v_month_tmp );
  else
    update INT_SMP_COVER_SECURITY_RES_MO smp
      set smp.coincidence_flag = '2'  --设置SMP侧一致性标识为2
        where not exists ( select 1 from INT_COVER_RES_INFO_MO foura 
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );  
  end if;

--比较差异数据，只有4A侧有数据，SMP侧无数据
  if i_province_code is null then      
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '3'  --设置4A侧一致性标识为3
        where not exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00' 
             and foura.rpt_month = v_month_tmp );           
  else 
    update INT_COVER_RES_INFO_MO foura 
      set foura.coincidence_flag = '3'  --设置4A侧一致性标识为3
        where not exists ( select 1 from INT_SMP_COVER_SECURITY_RES_MO smp
          where foura.province = smp.province
             and foura.rpt_month = smp.rpt_month
             and foura.begin_time = smp.begin_time
             and foura.end_time = smp.end_time
             and foura.seq = smp.seq
             and foura.res_type = '04'
             and smp.res_type = '03'
             and smp.system_type = '00'
             and foura.rpt_month = v_month_tmp 
             and foura.province = i_province_code );                
  end if;
/*
********************************逻辑处理 end********************************
*/
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
    
END ETL_SAFETY_ASSETS_DIFF;
