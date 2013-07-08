create or replace procedure SMP_SAFETY_ASSETS_COVER_RATE(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_province_code       char(3);                 --省代码
  v_month_tmp           varchar2(8) := '';       --考核月份
  v_score_rule          number(5, 2) := 0.00;    --扣分规则
  v_score_actual        number(5, 2) := 0.00;    --实际扣分
  v_score_aft_crt       number(5, 2) := 0.00;    --修正后的分数
  v_rule_id             varchar2(20) := '20031'; --规则ID，kpi_rule
  v_rule_type           varchar2(2) := '03';     --归属大类，kpi_rule
  v_rule_class          varchar2(2) := '31';     --规则分类，kpi_rule
  v_fourA_diff_counter  number;                  --4A差异数计数器
  v_smp_diff_counter    number;                  --SMP差异数计数器  
  v_fourA_all_counter   number;                  --4A总数
    
  type t_table is table of varchar2(50) index by binary_integer;  --定义数组
  v_table_province t_table;  --省份数组
  
BEGIN
/*
********************************参数校验 begin********************************
*/
  o_return_code := '0';
  o_return_msg  := '考核成功';

  --判断考核月份是否为空，为空则结束存储过程 
  if i_month is null then
    o_return_code := '-1';
    o_return_msg  := '考核月份不能为空。';
    return;
  end if;

  --校验考核月份的格式，不符合格式则结束存储过程
  begin
    select to_char(to_date(i_month, 'YYYYMM'), 'YYYYMM')
      into v_month_tmp
      from dual;
  Exception
    when others then
      o_return_code := '-1';
      o_return_msg  := '考核月份的格式出错，格式为:YYYYMM。 ' || SQLERRM;
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
  
  --查询SMP上报数据完好性扣分规则 ，判断是否为空，为空则结束存储过程
  begin
    select r.score
      into v_score_rule
      from kpi_rule r
     where r.rule_id = v_rule_id
       and r.rule_type = v_rule_type
       and r.rule_class = v_rule_class;
  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := 'SMP上报数据完好性扣分规则为空 。 ';
      return;
  end;
 
  begin
    if i_province_code is null then  
      select m.org_code bulk collect into v_table_province from se_org m  --批量赋值到数组中
      where m.parent_org = 1 and m.org_id < 9000;
    else
      v_table_province(1) := i_province_code;
    end if;
    Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := '省份数据为空，请确保SE_ORG表中数据正常 。 ';
      return;
  end;
/*
********************************参数校验 end********************************
*/

/*
********************************逻辑处理 begin********************************
*/
--保证考核不重复（删除相同考核条件的数据）
if i_province_code is null then
  delete from kpi_check_rec rcd 
  where rcd.rule_id = v_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_rule_class
  and rcd.check_cycle = v_month_tmp;
else 
  delete from kpi_check_rec rcd 
  where rcd.rule_id = v_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_rule_class
  and rcd.check_cycle = v_month_tmp 
  and rcd.province = i_province_code;
end if;

for i in 1..v_table_province.count loop  
  v_province_code := v_table_province(i);
  
  --统计4A侧总的记录数
  select count(1) into v_fourA_all_counter
  from INT_COVER_RES_INFO_MO foura
  where foura.coincidence_flag is not null 
  and foura.province = v_province_code
  and foura.rpt_month = v_month_tmp;
  
  --统计4A侧的差异数   
  select count(1) into v_fourA_diff_counter
  from INT_COVER_RES_INFO_MO foura
  where (foura.coincidence_flag = '1' or foura.coincidence_flag = '3') 
  and foura.rpt_month = v_month_tmp
  and foura.province = v_province_code;
      
  --统计SMP侧的差异数    
  select count(1) into v_smp_diff_counter
  from INT_SMP_COVER_SECURITY_RES_MO smp
  where smp.coincidence_flag = '2' 
  and smp.province = v_province_code
  and smp.rpt_month = v_month_tmp;
  
--差异计数 
  if v_fourA_diff_counter + v_smp_diff_counter > 0.2 * v_fourA_all_counter then
    v_score_actual := 0.20;
    v_score_aft_crt := 0.20;
  else
    v_score_actual := 0.00;  
    v_score_aft_crt := 0.00;
  end if;
  
--将考核结果存入kpi_check_rec表
  if v_province_code is not null then 
    insert into kpi_check_rec
      (province,
       check_cycle,
       rule_id,
       rule_type,
       score,
       score_after_corrected,
       remark,
       check_time,
       rule_class)
    values
      (v_province_code,
       v_month_tmp,
       v_rule_id,
       v_rule_type,
       v_score_actual,
       v_score_aft_crt,
       v_month_tmp || '月份的安全资产管理覆盖率考核数据',
       sysdate,
       v_rule_class);
   end if;
end loop;
/*
********************************逻辑处理 end********************************
*/
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
    
END SMP_SAFETY_ASSETS_COVER_RATE;
