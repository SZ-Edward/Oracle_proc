create or replace procedure TREASURY_MODE_CHECKING(
	o_return_code   out varchar2,
	o_return_msg    out varchar2,
	i_month         in varchar2,
	i_province_code in char
) is

  --临时变量
  v_province_code           char(3);                      --省代码
  v_month_tmp               varchar2(8) := '';            --考核月份
  v_para_id                 varchar2(20) := '2013060708'; --参数ID，kpi_para
  v_rule_type               varchar2(2) := '08';          --归属大类，kpi_rule 
  v_num_of_permit_overtime  number;                       --授权允许的情况下，时间跨度大于6小时的数量
  v_num_of_apply            number;                       --申请量
  v_num_of_approve          number;                       --审批量
  
  v_apply_rule_id       varchar2(20) := '20081'; --规则ID，kpi_rule  
  v_apply_rule_class    varchar2(2) := '81'; --规则分类，kpi_rule
  v_score_apply_rule    number(5, 2) := 0.00; --金库模式申请量扣分规则

  v_time_rule_id       varchar2(20) := '20082'; --规则ID，kpi_rule  
  v_time_rule_class    varchar2(2) := '82'; --规则分类，kpi_rule
  v_score_time_rule    number(5, 2) := 0.00; --金库模式时间跨度扣分规则

  v_score_limited  number(5, 2) := 0.00; --金库模式扣分上限
  v_score_counter  number(5, 2) := 0.00; --缓存扣分
  v_score_actual  number(5, 2) := 0.00; --实际扣分
  v_score_aft_crt number(5, 2) := 0.00; --修正后的分数
  
  type cursor_record is ref cursor;  
  v_cursor_app   cursor_record;  --申请和审批数量
  v_cursor_overtime   cursor_record;  --时间跨度大于6小时的授权允许数量
  
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
  
  --查询金库模式申请量扣分规则 ，判断是否为空，为空则结束存储过程
  begin
    select r.score
      into v_score_apply_rule
      from kpi_rule r
     where r.rule_id = v_apply_rule_id
       and r.rule_type = v_rule_type
       and r.rule_class = v_apply_rule_class;
  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := 'SMP上报数据完好性扣分规则为空 。 ';
      return;
  end;
  
  --查询金库模式时间跨度扣分规则 ，判断是否为空，为空则结束存储过程
  begin
    select r.score
      into v_score_time_rule
      from kpi_rule r
     where r.rule_id = v_time_rule_id
       and r.rule_type = v_rule_type
       and r.rule_class = v_time_rule_class;

  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := 'SMP上报数据完好性扣分规则为空 。 ';
      return;
  end;

  --查询金库模式管理使用情况扣分上限，判断是否为空，为空则结束存储过程
  begin
    select to_number(p.para_value, '99.99')
      into v_score_limited
      from kpi_para p
     where p.para_id = v_para_id
       and p.rule_type = v_rule_type;
  Exception
    when no_data_found then
      o_return_code := '-1';
      o_return_msg  := 'SMP上报数据完好性扣分上限为空 。 ';
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
  where rcd.rule_id = v_apply_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_apply_rule_class
  and substr(rcd.check_cycle, 1, 6) = v_month_tmp;
  delete from kpi_check_rec rcd 
  where rcd.rule_id = v_time_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_time_rule_class
  and substr(rcd.check_cycle, 1, 6) = v_month_tmp;
  open v_cursor_app for
    select to_char(t.begin_time, 'yyyymmdd'), t.province, tcount, t1count from
    (select ay.province, ay.begin_time, count(1) tcount from int_4a_apply_operation_dy ay
    where to_char(ay.begin_time, 'yyyymm') = v_month_tmp
    group by ay.begin_time, ay.province) t,
    (select ae.province, ae.begin_time, count(1) t1count from int_4a_approve_operation_dy ae
    where to_char(ae.begin_time, 'yyyymm') = v_month_tmp
    group by ae.begin_time, ae.province) t1
    where t.begin_time = t1.begin_time
    and t.province = t1.province;
else
  delete from kpi_check_rec rcd 
  where rcd.rule_id = v_apply_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_apply_rule_class
  and rcd.province = i_province_code
  and substr(rcd.check_cycle, 1, 6) = v_month_tmp;
  delete from kpi_check_rec rcd 
  where rcd.rule_id = v_time_rule_id 
  and rcd.rule_type = v_rule_type
  and rcd.rule_class = v_time_rule_class
  and rcd.province = i_province_code
  and substr(rcd.check_cycle, 1, 6) = v_month_tmp;
  open v_cursor_app for
    select to_char(t.begin_time, 'yyyymmdd'), t.province, tcount, t1count from
    (select ay.province, ay.begin_time, count(1) tcount from int_4a_apply_operation_dy ay
    where to_char(ay.begin_time, 'yyyymm') = v_month_tmp
    group by ay.begin_time, ay.province) t,
    (select ae.province, ae.begin_time, count(1) t1count from int_4a_approve_operation_dy ae
    where to_char(ae.begin_time, 'yyyymm') = v_month_tmp
    group by ae.begin_time, ae.province) t1
    where t.begin_time = t1.begin_time
    and t.province = t1.province
    and t.province = i_province_code;
end if;  

loop
  fetch v_cursor_app into v_month_tmp, v_province_code, v_num_of_apply, v_num_of_approve;
  
  exit when v_cursor_app%notfound;
  
  begin
    if v_num_of_apply != v_num_of_approve then
      v_score_actual := v_score_apply_rule;
      v_score_counter := v_score_counter + v_score_actual;
    end if;
    
    if v_score_counter > v_score_limited then
      v_score_aft_crt := 0.00;
    else
      v_score_aft_crt := v_score_actual; 
    end if;
    --将金库模式申请量考核结果存入kpi_check_rec表
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
       v_apply_rule_id,
       v_rule_type,
       v_score_actual,
       v_score_aft_crt,
       v_month_tmp || '的金库模式申请量考核数据',
       sysdate,
       v_apply_rule_class);
    
    open v_cursor_overtime for
      select to_char(n.begintime, 'yyyymmdd'), n.province, count(1) 
      from int_4a_approve_operation_dy m, int_4a_apply_operation_dy n
      where m.result = '0'
      and m.request_id = n.request_id
      and m.province = n.province
      and (n.endtime - n.begintime) * 24 > 6
      and n.request_mode = '00'
      and n.request_cond = '01' 
      and to_char(n.begintime, 'yyyymmdd') = v_month_tmp
      and n.province = v_province_code
      group by to_char(n.begintime, 'yyyymmdd'), n.province;
    
    loop
      fetch v_cursor_overtime into v_month_tmp, v_province_code, v_num_of_permit_overtime;
      exit when v_cursor_overtime%notfound;
      
      if v_num_of_permit_overtime > 0 then
        v_score_actual := v_score_time_rule;
        v_score_counter := v_score_counter + v_score_actual;
      end if;
      
      if v_score_counter > v_score_limited then
        v_score_aft_crt := 0.00;
      else
        v_score_aft_crt := v_score_actual; 
      end if;
      --将金库模式时间跨度考核结果存入kpi_check_rec表
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
         v_time_rule_id,
         v_rule_type,
         v_score_actual,
         v_score_aft_crt,
         v_month_tmp || '的金库模式时间跨度考核数据',
         sysdate,
         v_time_rule_class);
      end loop;
    
    close v_cursor_overtime;
  end;
end loop;

close v_cursor_app;  
/*
********************************逻辑处理 end********************************
*/  
Exception
  when others then
    o_return_code := '-1';
    o_return_msg  := sqlerrm;
      
END TREASURY_MODE_CHECKING;
